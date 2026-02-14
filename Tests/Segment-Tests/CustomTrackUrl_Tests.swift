//
//  CustomTrackUrl_Tests.swift
//  Segment-Tests
//
//  Tests for customTrackUrl: per-event POSTs, X-Write-Key header, concurrency limit,
//  error handling, in-flight duplicate prevention, and no deadlocks.
//

#if !os(Linux) && !os(Windows)

import XCTest
@testable import Segment

private let customTrackWriteKey = "testCustomTrackUrl"
private let customTrackURLString = "https://custom.example.com/track"

final class CustomTrackUrl_Tests: XCTestCase {

    override func setUpWithError() throws {
        Telemetry.shared.enable = false
        RestrictedHTTPSession.reset()
        RecordingHTTPSession.reset()
        DelayingHTTPSession.reset()
    }

    override func tearDownWithError() throws {
        RecordingHTTPSession.reset()
        DelayingHTTPSession.reset()
    }

    // MARK: - Individual POSTs per event

    func testCustomTrackUrl_sendsIndividualPOSTsPerEvent() throws {
        let analytics = Analytics(
            configuration: Configuration(writeKey: customTrackWriteKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .operatingMode(.synchronous)
                .flushAt(3)
                .flushInterval(9999)
                .httpSession(RecordingHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        analytics.track(name: "Event One", properties: ["a": 1])
        analytics.track(name: "Event Two", properties: ["b": 2])
        analytics.track(name: "Event Three", properties: ["c": 3])

        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)

        let recorded = RecordingHTTPSession.recordedDataUploads
        XCTAssertEqual(recorded.count, 3, "Should send one POST per event")

        for (index, upload) in recorded.enumerated() {
            XCTAssertEqual(upload.request.url?.absoluteString, customTrackURLString)
            XCTAssertEqual(upload.request.httpMethod, "POST")
            guard let body = upload.body,
                  let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any] else {
                XCTFail("Body should be valid JSON for upload \(index)")
                return
            }
            XCTAssertNil(json["batch"], "Each request body should be a single event, not a batch wrapper")
            XCTAssertTrue(json["type"] != nil || json["event"] != nil || json["userId"] != nil,
                         "Body should look like a single event")
        }
    }

    // MARK: - X-Write-Key header

    func testCustomTrackUrl_setsXWriteKeyHeader() throws {
        let writeKey = "myWriteKey123"
        let analytics = Analytics(
            configuration: Configuration(writeKey: writeKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .operatingMode(.synchronous)
                .flushAt(1)
                .flushInterval(9999)
                .httpSession(RecordingHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        analytics.track(name: "Test", properties: nil)
        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)

        let recorded = RecordingHTTPSession.recordedDataUploads
        XCTAssertGreaterThanOrEqual(recorded.count, 1)
        let headerValue = recorded[0].request.value(forHTTPHeaderField: "X-Write-Key")
        XCTAssertEqual(headerValue, writeKey, "X-Write-Key header should match configuration writeKey")
    }

    // MARK: - Concurrent uploads limited

    func testCustomTrackUrl_concurrentUploadsLimited() throws {
        DelayingBlockNetworkCalls.delaySeconds = 0.02
        let analytics = Analytics(
            configuration: Configuration(writeKey: customTrackWriteKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(1000))
                .operatingMode(.synchronous)
                .flushAt(30)
                .flushInterval(9999)
                .httpSession(DelayingHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        for i in 0..<25 {
            analytics.track(name: "Event \(i)", properties: ["index": i])
        }

        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 30)

        XCTAssertEqual(DelayingHTTPSession.dataUploadCount, 25, "All 25 events should be sent")
        XCTAssertLessThanOrEqual(
            DelayingBlockNetworkCalls.maxConcurrentRequestCount, 20,
            "Concurrent uploads should be limited to 20"
        )
    }

    // MARK: - Error handling: HTTP error

    func testCustomTrackUrl_httpErrorHandling() throws {
        let analytics = Analytics(
            configuration: Configuration(writeKey: customTrackWriteKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .operatingMode(.synchronous)
                .flushAt(1)
                .flushInterval(9999)
                .httpSession(RestrictedHTTPSession(blocking: true, failing: true))
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        analytics.track(name: "Will Fail", properties: nil)
        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)
        // Flush completion invoked; no deadlock. Server received 400 so batch may be retried or left in storage.
    }

    // MARK: - No deadlock (semaphore / group.leave)

    func testCustomTrackUrl_noDeadlock() throws {
        let analytics = Analytics(
            configuration: Configuration(writeKey: customTrackWriteKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .operatingMode(.synchronous)
                .flushAt(5)
                .flushInterval(9999)
                .httpSession(RecordingHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        for i in 0..<5 {
            analytics.track(name: "NoDeadlock \(i)", properties: nil)
        }

        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)

        XCTAssertEqual(RecordingHTTPSession.recordedDataUploads.count, 5)
    }

    // MARK: - In-flight tracking prevents duplicate sends

    func testCustomTrackUrl_inFlightPreventsDuplicateSends() throws {
        DelayingBlockNetworkCalls.delaySeconds = 0.15
        DelayingBlockNetworkCalls.reset()
        let analytics = Analytics(
            configuration: Configuration(writeKey: customTrackWriteKey)
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .operatingMode(.synchronous)
                .flushAt(2)
                .flushInterval(9999)
                .httpSession(DelayingHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        analytics.track(name: "First", properties: nil)
        analytics.track(name: "Second", properties: nil)

        let flush1Done = XCTestExpectation(description: "flush 1 done")
        analytics.flush { flush1Done.fulfill() }
        // Trigger second flush before first has completed (batch still in flight).
        let flush2Done = XCTestExpectation(description: "flush 2 done")
        analytics.flush { flush2Done.fulfill() }

        wait(for: [flush1Done, flush2Done], timeout: 20)

        // Should have sent 2 events total (one batch of 2), not 4 (duplicate batch).
        XCTAssertEqual(DelayingHTTPSession.dataUploadCount, 2,
                       "In-flight tracking should prevent the same batch from being sent twice")
    }
}

#endif

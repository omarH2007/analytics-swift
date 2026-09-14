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
        ScriptedHTTPSession.reset()
    }

    override func tearDownWithError() throws {
        RecordingHTTPSession.reset()
        DelayingHTTPSession.reset()
        ScriptedHTTPSession.reset()
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

    // MARK: - Plain JSON POST, never an upload

    func testCustomTrackUrl_sendsPlainJSONPostsAndNeverUploads() throws {
        let writeKey = "customTrackNeverUploads"
        UserDefaults.standard.set(true, forKey: "com.segment.customTrackUrl.legacyBacklogPurged.\(writeKey)")
        // production defaults: disk storage (batches are files on disk), asynchronous mode.
        let analytics = Analytics(
            configuration: Configuration(writeKey: writeKey)
                .customTrackUrl(string: customTrackURLString)
                .flushAt(9999)
                .flushInterval(9999)
                .httpSession(ScriptedHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        for i in 0..<3 {
            analytics.track(name: "Plain \(i)", properties: ["i": i])
        }
        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)

        XCTAssertEqual(ScriptedHTTPSession.uploadTaskCount, 0, "customTrackUrl must never use an upload task (file or data)")
        let requests = ScriptedHTTPSession.postDataTaskRequests
        XCTAssertEqual(requests.count, 3, "One plain POST per event")
        for request in requests {
            XCTAssertEqual(request.url?.absoluteString, customTrackURLString)
            XCTAssertEqual(request.httpMethod, "POST")
            XCTAssertEqual(request.value(forHTTPHeaderField: "Content-Type"), "application/json")
            let body = try XCTUnwrap(request.httpBody)
            let json = try XCTUnwrap(try JSONSerialization.jsonObject(with: body) as? [String: Any])
            XCTAssertNil(json["batch"], "Body is a single event, not a batch file")
            XCTAssertEqual(json["type"] as? String, "track")
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
        // Flush completion invoked; no deadlock. The batch is dropped, never retried.
        XCTAssertFalse(analytics.storage.dataStore.hasData)
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

    // MARK: - At most once: a failed event is deleted and never sent again
    // These run with the production defaults: disk storage, asynchronous operating mode.

    func testCustomTrackUrl_failedEventsAreNeverRetried() throws {
        ScriptedHTTPSession.outcome = .status(500)
        let analytics = makeDiskAnalytics(writeKey: "customTrackNeverRetried")

        for i in 0..<3 {
            analytics.track(name: "NeverRetried \(i)", properties: nil)
        }
        flushAndWait(analytics)
        XCTAssertEqual(uploadedEventNames(prefix: "NeverRetried").count, 3)

        // later flushes (interval timer, new events, app launches) must not send them again.
        for _ in 0..<3 {
            flushAndWait(analytics)
        }
        XCTAssertEqual(uploadedEventNames(prefix: "NeverRetried").count, 3, "Failed events must never be re-sent")
        XCTAssertFalse(analytics.storage.dataStore.hasData, "Failed batches must be deleted")
    }

    func testCustomTrackUrl_offlineEventsAreNeverRetried() throws {
        ScriptedHTTPSession.outcome = .offline
        let analytics = makeDiskAnalytics(writeKey: "customTrackOfflineNeverRetried")

        for i in 0..<3 {
            analytics.track(name: "Offline \(i)", properties: nil)
        }
        flushAndWait(analytics)
        for _ in 0..<3 {
            flushAndWait(analytics)
        }

        XCTAssertEqual(uploadedEventNames(prefix: "Offline").count, 3, "Events that failed offline must never be re-sent")
        XCTAssertFalse(analytics.storage.dataStore.hasData)
    }

    func testCustomTrackUrl_flushAtOne_sendsEachEventExactlyOnce() throws {
        // the app's config: a flush per event, uploads still running when the next flush starts, endpoint failing.
        ScriptedHTTPSession.outcome = .status(503)
        ScriptedHTTPSession.responseDelay = 0.05
        let analytics = makeDiskAnalytics(writeKey: "customTrackFlushAtOne", flushAt: 1)

        for i in 0..<20 {
            analytics.track(name: "FlushAtOne \(i)", properties: nil)
        }
        flushAndWait(analytics)
        waitForUploads(prefix: "FlushAtOne", count: 20)
        RunLoop.main.run(until: Date(timeIntervalSinceNow: 0.5))
        flushAndWait(analytics)

        let names = uploadedEventNames(prefix: "FlushAtOne")
        XCTAssertEqual(names.count, 20, "Each event must be sent exactly once")
        XCTAssertEqual(Set(names).count, 20, "No event may be sent twice")
    }

    func testCustomTrackUrl_legacyBacklogIsDeletedWithoutSending() throws {
        let writeKey = "customTrackLegacyBacklog"
        let purgedKey = "com.segment.customTrackUrl.legacyBacklogPurged.\(writeKey)"

        // a batch left on disk by an SDK version that kept retrying it.
        let previousLaunch = makeDiskAnalytics(writeKey: writeKey)
        let store = try XCTUnwrap(previousLaunch.storage.dataStore.store as? DirectoryStore)
        try FileManager.default.createDirectory(at: store.config.storageLocation, withIntermediateDirectories: true)
        let legacyBatch = store.config.storageLocation.appendingPathComponent("legacy-segment-events.temp")
        let body = #"{ "batch": [{"type":"track","event":"Legacy Old","messageId":"legacy-old"}],"sentAt":"2025-01-01T00:00:00.000Z","writeKey":"customTrackLegacyBacklog"}"#
        try Data(body.utf8).write(to: legacyBatch)

        // first launch with this SDK version.
        UserDefaults.standard.removeObject(forKey: purgedKey)
        let analytics = Analytics(
            configuration: Configuration(writeKey: writeKey)
                .customTrackUrl(string: customTrackURLString)
                .flushAt(9999)
                .flushInterval(9999)
                .httpSession(ScriptedHTTPSession())
        )
        waitUntilStarted(analytics: analytics)

        analytics.track(name: "Legacy Fresh", properties: nil)
        flushAndWait(analytics)

        XCTAssertEqual(uploadedEventNames(prefix: "Legacy "), ["Legacy Fresh"], "Only this launch's events are sent")
        XCTAssertFalse(FileManager.default.fileExists(atPath: legacyBatch.path), "The legacy batch is deleted")
        XCTAssertTrue(UserDefaults.standard.bool(forKey: purgedKey), "The purge is marked done once nothing legacy is left")
        withExtendedLifetime(previousLaunch) {}
    }

    func testCustomTrackUrl_concurrentFlushQueue_sendsEachEventExactlyOnce() throws {
        for (label, storageMode) in [("disk", StorageMode.disk), ("memory", .memory(1000))] {
            ScriptedHTTPSession.reset()
            ScriptedHTTPSession.outcome = .status(500)
            ScriptedHTTPSession.responseDelay = 0.02
            let writeKey = "customTrackConcurrentFlushes-\(label)"
            UserDefaults.standard.set(true, forKey: "com.segment.customTrackUrl.legacyBacklogPurged.\(writeKey)")
            let analytics = Analytics(
                configuration: Configuration(writeKey: writeKey)
                    .customTrackUrl(string: customTrackURLString)
                    .storageMode(storageMode)
                    .flushQueue(DispatchQueue(label: "customTrackConcurrentFlushes", attributes: .concurrent))
                    .flushAt(9999)
                    .flushInterval(9999)
                    .httpSession(ScriptedHTTPSession())
            )
            waitUntilStarted(analytics: analytics)
            analytics.storage.hardReset(doYouKnowHowToUseThis: true)

            for i in 0..<30 {
                analytics.track(name: "Concurrent \(i)", properties: nil)
                if i % 3 == 0 {
                    analytics.flush()
                    analytics.flush()
                }
            }
            flushAndWait(analytics)
            waitForUploads(prefix: "Concurrent", count: 30)
            RunLoop.main.run(until: Date(timeIntervalSinceNow: 0.5))

            let names = uploadedEventNames(prefix: "Concurrent")
            XCTAssertEqual(names.count, 30, "\(label): each event must be sent exactly once")
            XCTAssertEqual(Set(names).count, 30, "\(label): no event may be sent twice")
        }
    }

    func testCustomTrackUrl_unreadableBatchIsKeptUntilItCanBeSent() throws {
        let analytics = makeDiskAnalytics(writeKey: "customTrackUnreadable")
        analytics.track(name: "Unreadable", properties: nil)
        let batch = try XCTUnwrap(analytics.storage.dataStore.fetch()?.dataFiles?.first)

        try FileManager.default.setAttributes([.posixPermissions: 0o000], ofItemAtPath: batch.path)
        flushAndWait(analytics)
        XCTAssertTrue(uploadedEventNames(prefix: "Unreadable").isEmpty, "Nothing is sent while the batch can't be read")
        XCTAssertTrue(FileManager.default.fileExists(atPath: batch.path), "A batch that was never attempted is not deleted")

        try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: batch.path)
        flushAndWait(analytics)
        flushAndWait(analytics)
        XCTAssertEqual(uploadedEventNames(prefix: "Unreadable"), ["Unreadable"], "Sent once as soon as it can be read")
        XCTAssertFalse(FileManager.default.fileExists(atPath: batch.path))
    }

    func testCustomTrackUrl_memoryStorage_failedEventsAreNeverRetried() throws {
        ScriptedHTTPSession.outcome = .status(500)
        let analytics = Analytics(
            configuration: Configuration(writeKey: "customTrackMemoryNeverRetried")
                .customTrackUrl(string: customTrackURLString)
                .storageMode(.memory(100))
                .flushAt(9999)
                .flushInterval(9999)
                .httpSession(ScriptedHTTPSession())
        )
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)

        for i in 0..<3 {
            analytics.track(name: "MemoryNeverRetried \(i)", properties: nil)
        }
        flushAndWait(analytics)
        for _ in 0..<3 {
            flushAndWait(analytics)
        }

        XCTAssertEqual(uploadedEventNames(prefix: "MemoryNeverRetried").count, 3, "Failed events must never be re-sent")
        XCTAssertFalse(analytics.storage.dataStore.hasData)
    }

    func testWithoutCustomTrackUrl_failedBatchIsStillKept() throws {
        // the default Segment path is unchanged.
        ScriptedHTTPSession.outcome = .status(500)
        let analytics = makeDiskAnalytics(writeKey: "defaultPathKeepsFailedBatch", customTrackUrl: false)

        analytics.track(name: "DefaultPath", properties: nil)
        flushAndWait(analytics)

        XCTAssertEqual(ScriptedHTTPSession.uploadedBodies.count, 1)
        XCTAssertTrue(analytics.storage.dataStore.hasData)
    }

    // MARK: - Helpers

    private func makeDiskAnalytics(writeKey: String, flushAt: Int = 9999, customTrackUrl: Bool = true, markLegacyBacklogPurged: Bool = true) -> Analytics {
        if markLegacyBacklogPurged {
            UserDefaults.standard.set(true, forKey: "com.segment.customTrackUrl.legacyBacklogPurged.\(writeKey)")
        }
        let configuration = Configuration(writeKey: writeKey)
            .flushAt(flushAt)
            .flushInterval(9999)
            .httpSession(ScriptedHTTPSession())
        if customTrackUrl {
            configuration.customTrackUrl(string: customTrackURLString)
        }
        let analytics = Analytics(configuration: configuration)
        waitUntilStarted(analytics: analytics)
        analytics.storage.hardReset(doYouKnowHowToUseThis: true)
        return analytics
    }

    private func flushAndWait(_ analytics: Analytics) {
        let flushDone = XCTestExpectation(description: "flush done")
        analytics.flush { flushDone.fulfill() }
        wait(for: [flushDone], timeout: 10)
    }

    private func waitForUploads(prefix: String, count: Int, timeout: TimeInterval = 10) {
        let deadline = Date(timeIntervalSinceNow: timeout)
        while uploadedEventNames(prefix: prefix).count < count, Date() < deadline {
            RunLoop.main.run(until: Date(timeIntervalSinceNow: 0.05))
        }
    }

    private func uploadedEventNames(prefix: String) -> [String] {
        return ScriptedHTTPSession.uploadedBodies.compactMap { body in
            guard let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let name = json["event"] as? String, name.hasPrefix(prefix) else { return nil }
            return name
        }
    }
}

#endif

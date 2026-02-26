//
//  HTTPClient+CustomBatchUpload.swift
//  Segment
//
//  Extension: when Configuration.customTrackUrl is set, events are sent as one POST per event
//  to that URL (body = single event JSON). All logic lives here so the main HTTPClient stays minimal for upstream sync.
//

import Foundation
#if os(Linux) || os(Windows)
import FoundationNetworking
#endif

private enum CustomBatchUploadConstants {
    static let maxConcurrentEventUploads = 20
    static let queue = DispatchQueue(label: "com.segment.analytics.batchSend", qos: .utility)
}

extension HTTPClient {

    /// Sends each event in the batch as a separate POST to customTrackUrl (body = single event JSON).
    /// Called only when customTrackUrl is set; does not affect the default Segment path.
    internal func sendBatchAsOneRequestPerEvent(batchFileURL: URL? = nil, batchData: Data? = nil, completion: @escaping (Result<Bool, Error>) -> Void) {
        guard let uploadURL = effectiveCustomTrackUrl else {
            completion(.failure(HTTPClientErrors.failedToOpenBatch))
            return
        }

        let data: Data
        if let batchFileURL {
            guard let d = try? Data(contentsOf: batchFileURL) else {
                analytics?.reportInternalError(HTTPClientErrors.failedToOpenBatch)
                completion(.failure(HTTPClientErrors.failedToOpenBatch))
                return
            }
            data = d
        } else if let batchData {
            data = batchData
        } else {
            completion(.failure(HTTPClientErrors.failedToOpenBatch))
            return
        }

        CustomBatchUploadConstants.queue.async { [weak self] in
            guard let self else { return }

            guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let events = json["batch"] as? [[String: Any]],
                  !events.isEmpty else {
                DispatchQueue.main.async { completion(.success(true)) }
                return
            }

            let group = DispatchGroup()
            let concurrency = DispatchSemaphore(value: CustomBatchUploadConstants.maxConcurrentEventUploads)
            var firstError: Error?
            let lock = NSLock()

            for event in events {
                guard let eventBody = try? JSONSerialization.data(withJSONObject: event) else { continue }

                concurrency.wait()
                group.enter()

                let request = configuredRequestForBatchUpload(for: uploadURL, method: "POST")

                let task = session.uploadTask(with: request, from: eventBody) { [weak self] (_, response, error) in
                    defer {
                        concurrency.signal()
                        group.leave()
                    }
                    guard let self else { return }

                    if let error = error {
                        lock.lock()
                        if firstError == nil { firstError = error }
                        lock.unlock()
                        return
                    }
                    if let http = response as? HTTPURLResponse, http.statusCode >= 300 {
                        lock.lock()
                        if firstError == nil { firstError = HTTPClientErrors.statusCode(code: http.statusCode) }
                        lock.unlock()
                    }
                }
                task.resume()
            }

            group.notify(queue: .main) { [weak self] in
                if let err = firstError {
                    self?.analytics?.reportInternalError(err)
                    completion(.failure(err))
                } else {
                    completion(.success(true))
                }
            }
        }
    }
}

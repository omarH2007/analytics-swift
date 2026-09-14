//
//  SegmentDestination.swift
//  Segment
//
//  Created by Cody Garvin on 1/5/21.
//

import Foundation
import Sovran

#if os(Linux) || os(Windows)
// Whoever is doing swift/linux/Windows development over there
// decided that it'd be a good idea to split out a TON
// of stuff into another framework that NO OTHER PLATFORM
// has; I guess to be special.  :man-shrugging:
import FoundationNetworking
#endif

public class SegmentAnonymousId: AnonymousIdGenerator {
    public func newAnonymousId() -> String {
        return UUID().uuidString
    }
}

public class SegmentDestination: DestinationPlugin, Subscriber, FlushCompletion {
    internal enum Constants: String {
        case integrationName = "Segment.io"
        case apiHost = "apiHost"
        case apiKey = "apiKey"
    }

    public let type = PluginType.destination
    public let key: String = Constants.integrationName.rawValue
    public let timeline = Timeline()
    public weak var analytics: Analytics? {
        didSet {
            initialSetup()
        }
    }

    internal struct UploadTaskInfo {
        let url: URL?
        let data: Data?
        let task: DataTask
        // set/used via an extension in iOSLifecycleMonitor.swift
        typealias CleanupClosure = () -> Void
        var cleanup: CleanupClosure? = nil
    }

    internal var httpClient: HTTPClient?
    private var uploads = [UploadTaskInfo]()
    private let uploadsQueue = DispatchQueue(label: "uploadsQueue.segment.com")
    /// customTrackUrl only: batches being taken out of storage, shared by every instance. See `takeCustomTrackBatch(url:)`.
    private static var customTrackClaimedFiles = Set<URL>()
    private static let customTrackClaimQueue = DispatchQueue(label: "com.segment.customTrackClaim")
    /// customTrackUrl only: batch files found on disk at launch that the one-time legacy purge still has to delete.
    private var customTrackLegacyFiles = Set<URL>()
    private var storage: Storage?

    @Atomic internal var eventCount: Int = 0

    internal func initialSetup() {
        guard let analytics = self.analytics else { return }
        storage = analytics.storage
        httpClient = HTTPClient(analytics: analytics)
        snapshotLegacyCustomTrackBacklog()

        // Add DestinationMetadata enrichment plugin
        add(plugin: DestinationMetadataPlugin())
    }

    public func update(settings: Settings, type: UpdateType) {
        guard let analytics = analytics else { return }
        let segmentInfo = settings.integrationSettings(forKey: self.key)
        // if customer cycles out a writekey at app.segment.com, this is necessary.
        /*
         This actually works differently than anticipated.  It was thought that when a writeKey was
         revoked, it's old writekey would redirect to the new, but it doesn't work this way.  As a result
         it doesn't appear writekey can be changed remotely.  Leaving this here in case that changes in the
         near future (written on 10/29/2022).
         */
        /*
        if let key = segmentInfo?[Self.Constants.apiKey.rawValue] as? String, key.isEmpty == false {
            if key != analytics.configuration.values.writeKey {
                /*
                 - would need to flush.
                 - would need to change the writeKey across the system.
                 - would need to re-init storage.
                 - probably other things too ...
                 */
            }
        }
         */
        // if customer specifies a different apiHost (ie: eu1.segmentapis.com) at app.segment.com ...
        if let host = segmentInfo?[Self.Constants.apiHost.rawValue] as? String, host.isEmpty == false {
            if host != analytics.configuration.values.apiHost {
                analytics.configuration.values.apiHost = host
                httpClient = HTTPClient(analytics: analytics)
            }
        }
    }

    // MARK: - Event Handling Methods
    public func execute<T: RawEvent>(event: T?) -> T? {
        guard let event = event else { return nil }
        let result = process(incomingEvent: event)
        if let r = result {
            queueEvent(event: r)
        }
        return result
    }

    // MARK: - Abstracted Lifecycle Methods
    internal func enterForeground() { }

    internal func enterBackground() {
        analytics?.flush()
    }

    // MARK: - Event Parsing Methods
    private func queueEvent<T: RawEvent>(event: T) {
        guard let storage = self.storage else { return }
        // Send Event to File System
        storage.write(.events, value: event)
        self._eventCount.mutate { count in
            count += 1
        }
    }

    public func flush(group: DispatchGroup) {
        group.enter()
        defer { group.leave() }
        
        guard let storage = self.storage else { return }
        guard let analytics = self.analytics else { return }
        
        // don't flush if analytics is disabled.
        guard analytics.enabled == true else { return }
        
        _eventCount.set(0)
        cleanupUploads()
        
        let type = storage.dataStore.transactionType
        let hasData = storage.dataStore.hasData
        
        analytics.log(message: "Uploads in-progress: \(pendingUploads)")

        if pendingUploads == 0 {
            if type == .file, hasData {
                flushFiles(group: group)
            } else if type == .data, hasData {
                // we know it's a data-based transaction as opposed to file I/O
                flushData(group: group)
            }
        } else {
            analytics.log(message: "Skipping processing; Uploads in progress.")
        }
    }
}

extension SegmentDestination {
    private func flushFiles(group: DispatchGroup) {
        guard let storage = self.storage else { return }
        guard let analytics = self.analytics else { return }
        guard let httpClient = self.httpClient else { return }

        if httpClient.effectiveCustomTrackUrl != nil {
            flushFilesToCustomTrackUrl(group: group)
            return
        }

        // Cooperative release of allocated memory by URL instances (dataFiles).
        autoreleasepool {
            guard let files = storage.dataStore.fetch()?.dataFiles else { return }
            
            for url in files {
                // Use the autorelease pool to ensure that unnecessary memory allocations
                // are released after each iteration. If there is a large backlog of files
                // to iterate, the host applications may crash due to OOM issues.
                autoreleasepool {
                    // enter for this url we're going to kick off
                    group.enter()
                    analytics.log(message: "Processing Batch:\n\(url.lastPathComponent)")
                    
                    // set up the task
                    let uploadTask = httpClient.startBatchUpload(writeKey: analytics.configuration.values.writeKey, batch: url) { [weak self] result in
                        defer {
                            group.leave()
                        }
                        guard let self else { return }
                        switch result {
                        case .success(_):
                            storage.remove(data: [url])
                            cleanupUploads()
                            
                            // we don't want to retry events in a given batch when a 400
                            // response for malformed JSON is returned, nor when a 429
                            // rate limit is returned.
                        case .failure(Segment.HTTPClientErrors.statusCode(code: 400)),
                             .failure(Segment.HTTPClientErrors.statusCode(code: 429)):
                            storage.remove(data: [url])
                            cleanupUploads()
                        default:
                            break
                        }
                        
                        analytics.log(message: "Processed: \(url.lastPathComponent)")
                        // the upload we have here has just finished.
                        // make sure it gets removed and it's cleanup() called rather
                        // than waiting on the next flush to come around.
                        cleanupUploads()
                    }
                    
                    // we have a legit upload in progress now, so add it to our list.
                    if let upload = uploadTask {
                        add(uploadTask: UploadTaskInfo(url: url, data: nil, task: upload))
                    } else {
                        // we couldn't get a task, so we need to leave the group or things will hang.
                        group.leave()
                    }
                }
            }
        }
    }
    
    private func flushData(group: DispatchGroup) {
        // DO NOT CALL THIS FROM THE MAIN THREAD, IT BLOCKS!
        // Don't make me add a check here; i'll be sad you didn't follow directions.
        guard let storage = self.storage else { return }
        guard let analytics = self.analytics else { return }
        guard let httpClient = self.httpClient else { return }
        
        if httpClient.effectiveCustomTrackUrl != nil {
            flushDataToCustomTrackUrl(group: group)
            return
        }
        
        let totalCount = storage.dataStore.count
        var currentCount = 0
        
        guard totalCount > 0 else { return }
        
        while currentCount < totalCount {
            // can't imagine why we wouldn't get data at this point, but if we don't, then split.
            guard let eventData = storage.dataStore.fetch() else { return }
            guard let data = eventData.data else { return }
            guard let removable = eventData.removable else { return }
            guard let dataCount = eventData.removable?.count else { return }
            
            currentCount += dataCount
            
            // enter for this data we're going to kick off
            group.enter()
            analytics.log(message: "Processing In-Memory Batch (size: \(data.count))")
            
            // we're already on a separate thread.
            // lets let this task complete so we can get all the values out.
            let semaphore = DispatchSemaphore(value: 0)
            
            // set up the task
            let uploadTask = httpClient.startBatchUpload(writeKey: analytics.configuration.values.writeKey, data: data) { [weak self] result in
                defer {
                    // leave for the url we kicked off.
                    group.leave()
                    semaphore.signal()
                }
                
                guard let self else { return }
                switch result {
                case .success(_):
                    storage.remove(data: removable)
                    cleanupUploads()
                    
                    // we don't want to retry events in a given batch when a 400
                    // response for malformed JSON is returned, nor when a 429
                    // rate limit is returned.
                case .failure(Segment.HTTPClientErrors.statusCode(code: 400)),
                     .failure(Segment.HTTPClientErrors.statusCode(code: 429)):
                    storage.remove(data: removable)
                    cleanupUploads()
                default:
                    break
                }
                
                analytics.log(message: "Processed In-Memory Batch (size: \(data.count))")
                // the upload we have here has just finished.
                // make sure it gets removed and it's cleanup() called rather
                // than waiting on the next flush to come around.
                cleanupUploads()
            }
            
            // we have a legit upload in progress now, so add it to our list.
            if let upload = uploadTask {
                add(uploadTask: UploadTaskInfo(url: nil, data: data, task: upload))
            } else {
                // we couldn't get a task, so we need to leave the group or things will hang.
                group.leave()
                semaphore.signal()
            }
            
            _ = semaphore.wait(timeout: .distantFuture)
        }
    }
}

// MARK: - customTrackUrl delivery (at most once, never retried)

/*
 When customTrackUrl is set, every event is attempted at most once and is never retried.

 Batches are taken out of storage *before* any request is made. Whatever happens next (2xx, 4xx, 5xx,
 timeout, offline, app suspended or killed mid-upload) a batch can never be fetched again, so a failing
 endpoint can't make later flushes, interval timers or app launches re-send the same events.
 */
extension SegmentDestination {
    /// customTrackUrl, disk storage.
    private func flushFilesToCustomTrackUrl(group: DispatchGroup) {
        guard let analytics = self.analytics else { return }
        guard let httpClient = self.httpClient else { return }
        guard let files = storage?.dataStore.fetch()?.dataFiles else { return }

        for url in files {
            autoreleasepool {
                guard let batch = takeCustomTrackBatch(url: url) else { return }

                group.enter()
                analytics.log(message: "Processing Batch:\n\(url.lastPathComponent)")
                httpClient.startBatchUpload(writeKey: analytics.configuration.values.writeKey, data: batch) { result in
                    defer { group.leave() }
                    if case .failure(let error) = result {
                        analytics.log(message: "Dropped \(url.lastPathComponent), it will not be retried: \(error)")
                    } else {
                        analytics.log(message: "Processed: \(url.lastPathComponent)")
                    }
                }
            }
        }
        finishLegacyCustomTrackPurgeIfDone()
    }

    /// customTrackUrl, memory storage.
    private func flushDataToCustomTrackUrl(group: DispatchGroup) {
        // DO NOT CALL THIS FROM THE MAIN THREAD, IT BLOCKS!
        guard let storage = self.storage else { return }
        guard let analytics = self.analytics else { return }
        guard let httpClient = self.httpClient else { return }

        // bounded by the starting count, so a store that fails to remove can't loop forever.
        var remaining = storage.dataStore.count
        while remaining > 0 {
            // fetch and remove as one step, so an overlapping flush can't take the same events.
            let taken: (data: Data, count: Int)? = SegmentDestination.customTrackClaimQueue.sync {
                guard let eventData = storage.dataStore.fetch(), let data = eventData.data,
                      let removable = eventData.removable, removable.isEmpty == false else { return nil }
                storage.remove(data: removable)
                return (data, removable.count)
            }
            guard let taken else { return }
            remaining -= taken.count

            group.enter()
            analytics.log(message: "Processing In-Memory Batch (size: \(taken.data.count))")
            let semaphore = DispatchSemaphore(value: 0)
            httpClient.startBatchUpload(writeKey: analytics.configuration.values.writeKey, data: taken.data) { result in
                defer {
                    group.leave()
                    semaphore.signal()
                }
                if case .failure(let error) = result {
                    analytics.log(message: "Dropped In-Memory Batch (size: \(taken.data.count)), it will not be retried: \(error)")
                } else {
                    analytics.log(message: "Processed In-Memory Batch (size: \(taken.data.count))")
                }
            }
            _ = semaphore.wait(timeout: .distantFuture)
        }
    }

    /// Claims a batch file, reads it and deletes it, returning its contents to send. Returns nil, and nothing
    /// is sent, if another flush already took it, it can't be read yet, it can't be deleted, or it is legacy.
    private func takeCustomTrackBatch(url: URL) -> Data? {
        guard let storage = self.storage else { return nil }
        let (claimed, legacy) = SegmentDestination.customTrackClaimQueue.sync {
            (SegmentDestination.customTrackClaimedFiles.insert(url).inserted, customTrackLegacyFiles.contains(url))
        }
        guard claimed else { return nil }
        defer { SegmentDestination.customTrackClaimQueue.sync { _ = SegmentDestination.customTrackClaimedFiles.remove(url) } }

        if legacy {
            storage.remove(data: [url])
            if FileManager.default.fileExists(atPath: url.path) == false {
                SegmentDestination.customTrackClaimQueue.sync { _ = customTrackLegacyFiles.remove(url) }
                analytics?.log(message: "Deleted legacy batch \(url.lastPathComponent) without sending it.")
            }
            return nil
        }

        // unreadable (e.g. protected data not available yet): leave it for a later flush.
        guard let batch = try? Data(contentsOf: url) else { return nil }
        storage.remove(data: [url])
        // a batch still on disk would be fetched and sent again by the next flush.
        if FileManager.default.fileExists(atPath: url.path) {
            analytics?.log(message: "Unable to delete \(url.lastPathComponent); it will not be sent.")
            return nil
        }
        return batch
    }

    private var customTrackLegacyPurgedKey: String? {
        guard let analytics = self.analytics else { return nil }
        return "com.segment.customTrackUrl.legacyBacklogPurged.\(analytics.configuration.values.writeKey)"
    }

    /// Batch files already on disk before this instance stores anything were left by SDK versions that re-sent
    /// failed customTrackUrl batches on every flush, so they were already attempted and failed. Once per install,
    /// remember them so they are deleted without being sent instead of uploading that backlog one more time.
    private func snapshotLegacyCustomTrackBacklog() {
        guard httpClient?.effectiveCustomTrackUrl != nil, let key = customTrackLegacyPurgedKey else { return }
        guard UserDefaults.standard.bool(forKey: key) == false else { return }
        guard let storage = self.storage, storage.dataStore.transactionType == .file else { return }

        let files = storage.dataStore.fetch()?.dataFiles ?? []
        if files.isEmpty {
            UserDefaults.standard.set(true, forKey: key)
        } else {
            SegmentDestination.customTrackClaimQueue.sync { customTrackLegacyFiles = Set(files) }
        }
    }

    /// The purge is only marked done once every legacy file is really gone, so an interrupted purge resumes next launch.
    private func finishLegacyCustomTrackPurgeIfDone() {
        guard let key = customTrackLegacyPurgedKey, UserDefaults.standard.bool(forKey: key) == false else { return }
        let done = SegmentDestination.customTrackClaimQueue.sync { () -> Bool in
            customTrackLegacyFiles = customTrackLegacyFiles.filter { FileManager.default.fileExists(atPath: $0.path) }
            return customTrackLegacyFiles.isEmpty
        }
        if done {
            UserDefaults.standard.set(true, forKey: key)
        }
    }
}

// MARK: - Upload management

extension SegmentDestination {
    internal func cleanupUploads() {
        // lets go through and get rid of any tasks that aren't running.
        // either they were suspended because a background task took too
        // long, or the os orphaned it due to device constraints (like a watch).
        uploadsQueue.sync {
            let before = uploads.count
            var newPending = uploads
            newPending.removeAll { uploadInfo in
                let shouldRemove = uploadInfo.task.state != .running
                if shouldRemove, let cleanup = uploadInfo.cleanup {
                    cleanup()
                }
                return shouldRemove
            }
            uploads = newPending
            let after = uploads.count
            analytics?.log(message: "Cleaned up \(before - after) non-running uploads.")
        }
    }

    internal var pendingUploads: Int {
        var uploadsCount = 0
        uploadsQueue.sync {
            uploadsCount = uploads.count
        }
        return uploadsCount
    }

    internal func add(uploadTask: UploadTaskInfo) {
        uploadsQueue.sync {
            uploads.append(uploadTask)
        }
    }
}

// MARK: Versioning

extension SegmentDestination: VersionedPlugin {
    public static func version() -> String {
        return __segment_version
    }
}

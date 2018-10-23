	//
//  Reswifq.swift
//  Reswifq
//
//  Created by Valerio Mazzeo on 21/02/2017.
//  Copyright © 2017 VMLabs Limited. All rights reserved.
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//  See the GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program. If not, see <http://www.gnu.org/licenses/>.
//

import Foundation
import RedisClient
import Vapor

public enum ReswifqError: Error {
    case unknownJobType(String)
}

// MARK: - Reswifq

public final class Reswifq: Queue {
    
    

    // MARK: Initialization

    public required init(client: RedisClient) {
        self.client = client
    }

    // MARK: Setting and Getting Attributes

    public let client: RedisClient

    public var jobMap = [String: Job.Type]()

    // MARK: Queue

    /// Priority not supported at the moment
    /// See https://github.com/antirez/redis/issues/1785
    public func enqueue(_ job: Job, priority: QueuePriority = .medium, scheduleAt: Date? = nil) throws -> Future<Void> {

        print("BEFORE ENQUEING. PRIORITY: \(priority)")
        let encodedJob = try JobBox(job, priority: priority).data().string(using: .utf8)

        if let scheduledAt = scheduleAt {
            // Delayed Job
            return try self.client.zadd(RedisKey(.queueDelayed).value, values: (score: scheduledAt.timeIntervalSince1970, member: encodedJob)).map(to: Void.self) {
                result in
                return
            }
        } else {
            // Normal Job
          //  print("before normal job in reswifq")
            return try self.client.lpush(RedisKey(.queuePending(priority)).value, values: encodedJob).map(to: Void.self) {
                result in
                print("after normal job in reswifq")

                return
            }
        }
    }

    public func dequeue(priority: QueuePriority) throws -> Future<PersistedJob> {

        
        return try self.client.rpoplpush(
            source: RedisKey(.queuePending(priority)).value,
            destination: RedisKey(.queueProcessing).value
            ).flatMap(to: PersistedJob.self){
                encodedJob in
                print("encodedJob: \(encodedJob)")
                
                let persistedJob = try self.persistedJob(with: encodedJob)
                return try self.setLock(for: persistedJob).map(to: PersistedJob.self){
                    response in
                    return persistedJob
                }
        }

    }

    public func bdequeue(priority: QueuePriority) throws -> Future<PersistedJob> {
        print("QueuePriority is: \(priority.rawValue)")
        return try self.client.brpoplpush(source: RedisKey(.queuePending(priority)).value, destination: RedisKey(.queueProcessing).value).flatMap(to: PersistedJob.self){
                encodedJob in
            print("inside bdequeue(priority: in reswifq")

                print("encodedJob: \(encodedJob)")

            let persistedJob = try self.persistedJob(with: encodedJob)
                return try self.setLock(for: persistedJob).map(to: PersistedJob.self){
                    response in
                    return persistedJob
                }
        }
    }

    public func complete(_ identifier: JobID) throws -> Future<Void> {

        return try self.client.multi().flatMap(to: Void.self){
            (client, transaction, mainResponse) in
         //   print("Back from multi. client: \(client), transaction:\(transaction), response:\(mainResponse)")
            
            
            do {
             //   print("Trying: Remove the job from the processing queue")
                // Remove the job from the processing queue

                return try client.lrem(RedisKey(.queueProcessing).value, value: identifier, count: -1).flatMap(to: Void.self){
                    response in
               //     print("Trying: Remove the lock")
                    
                    // Remove the lock

                    return try client.del(RedisKey(.lock(identifier)).value).flatMap(to: Void.self) {
                        response in
                        
                   //     print("Trying: Remove any retry attempt")
                        
                        // Remove any retry attempt
                        return try client.del(RedisKey(.retry(identifier)).value).flatMap(to: Void.self){
                            reponse in
                         //   print("Before Complete EXEC response is: \(reponse)")

                            return try client.execute("EXEC", arguments: nil).map(to: Void.self){
                                execResponse in
                             //   print("Inside Complete EXEC response is: \(execResponse)")
                                guard let _ = execResponse.array else {
                                    throw RedisClientError.invalidResponse(mainResponse)
                                }
                                
                                return
                            }
                        }
                    }
                }
                 
            }  catch RedisClientError.invalidResponse(let response) {
                guard response.status == .queued else {
                    throw RedisClientError.invalidResponse(response)
                }
            } catch {
                _ = try client.execute("DISCARD", arguments: nil).map(to: Void.self){
                    response in
                    throw RedisClientError.transactionAborted
                    
                }
            }
            let app = try Application()
            return app.future()

        }
        
    }
}

// MARK: - Queue Status

extension Reswifq {

    /**
     Fetches any pending job.

     - returns: An array of persisted jobs that have been enqueued and are waiting to be processed.
     */
    public func pendingJobs() throws -> Future<[JobID]> {

        return try self.client.lrange(RedisKey(.queuePending(.medium)).value, start: 0, stop: -1).map(to: [JobID].self) {
            jobIds in
            //     print("Im after lrange")
            return jobIds
        }
    }

    /**
     Fetches any processing job.

     - returns: An array of persisted jobs that have been dequeued and are being processed.
     */
    public func processingJobs() throws -> Future<[JobID]> {
      //  print("before lrange")
        return try self.client.lrange(RedisKey(.queueProcessing).value, start: 0, stop: 500).map(to: [JobID].self) {
            jobIds in
          //  print("Im after lrange. jobids: \(jobIds)")
            return jobIds
        }
    }

    /**
     Fetches any delayed job.

     - returns: An array of persisted jobs that have been scheduled for delayed execution.
     */
    public func delayedJobs() throws -> Future<[JobID]> {

        return try self.client.zrange(RedisKey(.queueDelayed).value, start: 0, stop: -1)
    }

    /**
     Fetches any overdue job.

     - returns: An array of persisted jobs that have been scheduled for delayed execution and are now overdue.
     */
    public func overdueJobs() throws -> Future<[JobID]> {

        return try self.client.zrangebyscore(RedisKey(.queueDelayed).value, min: 0, max: Date().timeIntervalSince1970).map(to: [JobID].self){
            result in
            return result
        }
    }

    public func enqueueOverdueJobs(with req: Request) throws -> Future<Void> {

        let newPromise = req.eventLoop.newPromise(Void.self)
        
        _ = try self.overdueJobs().map(to: Void.self){
            overdueJobs in
        //    print("SERVER RESPONSE IN enqueueOverdueJobs: \(overdueJobs)")
            guard overdueJobs.count > 0 else {
                newPromise.succeed()
                return
            }
            var x = 0
            for job in overdueJobs {
                
                _ = try self.client.multi().map(to: Void.self){
                    result in
                    let (client, transaction, response) = result
                    try transaction.enqueue {
                        // Remove the job from the delayed queue
                        _ = try client.zrem(RedisKey(.queueDelayed).value, member: job).map(to: Void.self) {
                            bACL in
                        //    print("bACL is: \(bACL)")
                            try transaction.enqueue {
                                // Add the job to the pending queue
                                // This is not ideal because subsequent delayed jobs would be executed in reverse order,
                                // but this is the best solution, until we can support queues with different priorities
                                
                                _ = try client.rpush(RedisKey(.queuePending(.medium)).value, values: job).map(to: Void.self) {
                                    bACL in
                               //     print("bACL22 is: \(bACL)")
                                    x += 1
                                    if x == overdueJobs.count {
                                        newPromise.succeed()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return newPromise.futureResult
    }

    /**
     Determines whether a job has overcome its time to live in the processing queue.
     
     - returns: `true` if the job has expired, `false` otherwise.
     */
    public func isJobExpired(_ identifier: JobID) throws -> Future<Bool> {

        return try self.client.get(RedisKey(.lock(identifier)).value).map(to: Bool.self){
            response in
      //      print("inside isJobExpired. respponse:\(response)")
            return response == nil
            }
    }

    /**
     Fetches the retry attempts for a given job.

     - parameter identifier: The identifier of the job to retrieve the retry attempts for.
     
     - returns: The number of retry attempts for the given jobs.
     */
    public func retryAttempts(for identifier: JobID) throws -> Future<Int64> {

        return try self.client.get(RedisKey(.retry(identifier)).value).map(to: Int64.self){
            attempts in
         //   print("Im inside retryAttempts")
            if let attempts = attempts {
                return Int64(attempts) ?? 0
            } else {
                return 0
            }
        }
    }

    /**
     Moves a job from the processing queue to the pending queue.
     The operation is performed in a transaction to ensure the job is in either one of the two queues.
     
     If the job is not expired the move operation is skipped and no error is thrown.
     
     - parameter identifier: The identifier of the job to retry.
     - returns: `true` if an retry attempt has been made, `false` otherwise.
     */
    @discardableResult
    public func retryJobIfExpired(with req: Request, identifier: JobID) throws -> Future<Bool> {
        let newPromise = req.eventLoop.newPromise(Bool.self)

        _ = try self.isJobExpired(identifier).map(to: Void.self){
            response in
            
            guard response != false else {
                newPromise.succeed(result: false)
                return
             //   return req.future(false)
            }
       //     print("BEOFRE MULTI retryJobIfExpired")
            _ = try self.client.multi().map(to: Void.self){
                result in
           //     print("AFTER MULTI retryJobIfExpired. RESULT: \(result)")

                let (client, transaction, response) = result
                _ = try transaction.enqueue {
                    // Remove the job from the processing queue
                    _ = try client.lrem(RedisKey(.queueProcessing).value, value: identifier, count: -1).map(to: Void.self){
                        _ in
                    //    print("BEOFRE lpush retryJobIfExpired. RESULT: \(result)")
                        var priority: QueuePriority = .medium
                        if identifier.contains("categories") {
                            priority = .categories
                        } else if identifier.contains("asins") {
                            priority = .asins
                        } else if identifier.contains("upcs") {
                            priority = .upcs
                        }
                      //  let priority: QueuePriority = identifier.contains("productPageData") || identifier.contains("asins") || identifier.contains("upcs") ? .high : .medium
                      //  print("Priority: \(priority), jobID: \(identifier)")
                        
                        _ = try client.lpush(RedisKey(.queuePending(priority)).value, values: identifier).map(to: Void.self){
                            _ in
                        //    print("AFTER lpush retryJobIfExpired. RESULT: \(result)")

                            _ = try client.incr(RedisKey(.retry(identifier)).value).map(to: Void.self){
                                _ in
                                _ = try client.execute("EXEC", arguments: nil).map(to: Void.self){
                                    execResponse in
                                    newPromise.succeed(result: true)
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return newPromise.futureResult.map(to: Bool.self) {
            result in
            return result
        }
        
    }
    
    
}

// MARK: - Queue Helpers

extension Reswifq {

    fileprivate func persistedJob(with encodedJob: String) throws -> PersistedJob {

        let jobBox = try JobBox(data: encodedJob.data(using: .utf8))

        guard let jobType = self.jobMap[jobBox.type] else {
            throw ReswifqError.unknownJobType(jobBox.type)
        }

        let job = try jobType.init(data: jobBox.job)

        return (identifier: encodedJob, job: job)
    }

    fileprivate func setLock(for persistedJob: PersistedJob) throws -> Future<Void> {

        return try self.client.setex(
            RedisKey(.lock(persistedJob.identifier)).value,
            timeout: persistedJob.job.timeToLive,
            value: persistedJob.identifier
            ).map(to: Void.self){
                result in
                
                
                
        }
    }
}

// MARK: RedisKey

extension Reswifq {

    struct RedisKey {

        // MARK: Initialization

        public init(_ key: RedisKey.Key) {
            self.init(key.components)
        }

        public init(_ components: String...) {
            self.init(components)
        }

        public init(_ components: [String]) {
            self.value = components.joined(separator: ":")
        }

        // MARK: Attributes

        public let value: String
    }
}

extension Reswifq.RedisKey {

    enum Key {

        case queuePending(QueuePriority)
        case queueProcessing
        case queueDelayed

        case lock(String)

        case retry(String)
    }
}

extension Reswifq.RedisKey.Key {

    var components: [String] {

        switch self {

        case .queuePending(let priority):
            return ["queue", "pending", priority.rawValue]

        case .queueProcessing:
            return ["queue", "processing"]

        case .queueDelayed:
            return ["queue", "delayed"]

        case .lock(let value):
            return ["lock", value]

        case .retry(let value):
            return ["retry", value]
        }
    }
}

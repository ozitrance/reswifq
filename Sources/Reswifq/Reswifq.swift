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
    public func enqueue(_ job: Job, priority: QueuePriority = .medium, scheduleAt: Date? = nil) throws {

        let encodedJob = try JobBox(job, priority: priority).data().string(using: .utf8)

        if let scheduledAt = scheduleAt {
            // Delayed Job
            try self.client.zadd(RedisKey(.queueDelayed).value, values: (score: scheduledAt.timeIntervalSince1970, member: encodedJob))
        } else {
            // Normal Job
            //try self.client.lpush(RedisKey(.queuePending(priority)).value, values: encodedJob)
            try self.client.lpush(RedisKey(.queuePending(.medium)).value, values: encodedJob)
        }
    }

    public func dequeue() throws -> Future<PersistedJob> {

        
        return try self.client.rpoplpush(
            source: RedisKey(.queuePending(.medium)).value,
            destination: RedisKey(.queueProcessing).value
            ).flatMap(to: PersistedJob.self){
                encodedJob in
                
                let persistedJob = try self.persistedJob(with: encodedJob)
                return try self.setLock(for: persistedJob).map(to: PersistedJob.self){
                    response in
                    return persistedJob
                }
        }

    }

    public func bdequeue() throws -> Future<PersistedJob> {

        return try self.client.brpoplpush(
            source: RedisKey(.queuePending(.medium)).value,
            destination: RedisKey(.queueProcessing).value
            ).flatMap(to: PersistedJob.self){
                encodedJob in

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
            print("Back from multi. client: \(client), transaction:\(transaction), response:\(mainResponse)")
            
            
            do {
                print("Trying: Remove the job from the processing queue")
                // Remove the job from the processing queue

                return try client.lrem(RedisKey(.queueProcessing).value, value: identifier, count: -1).flatMap(to: Void.self){
                    response in
                    print("Trying: Remove the lock")
                    
                    // Remove the lock

                    return try client.del(RedisKey(.lock(identifier)).value).flatMap(to: Void.self) {
                        response in
                        
                        print("Trying: Remove any retry attempt")
                        
                        // Remove any retry attempt
                        return try client.del(RedisKey(.retry(identifier)).value).flatMap(to: Void.self){
                            reponse in
                            
                            return try client.execute("EXEC", arguments: nil).map(to: Void.self){
                                execResponse in
                                
                                guard let result = execResponse.array else {
                                    throw RedisClientError.invalidResponse(mainResponse)
                                }
                                
                                //  return result
                            }

                            
                        }
                    }
                    throw RedisClientError.enqueueCommandError

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

        return try self.client.lrange(RedisKey(.queuePending(.medium)).value, start: 0, stop: -1)
    }

    /**
     Fetches any processing job.

     - returns: An array of persisted jobs that have been dequeued and are being processed.
     */
    public func processingJobs() throws -> Future<[JobID]> {

        return try self.client.lrange(RedisKey(.queueProcessing).value, start: 0, stop: -1)
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

        return try self.client.zrangebyscore(RedisKey(.queueDelayed).value, min: 0, max: Date().timeIntervalSince1970)
    }

    public func enqueueOverdueJobs() throws {

        _ = try self.overdueJobs().map(to: Void.self){
            overdueJobs in
           
            for job in overdueJobs {
                
                _ = try self.client.multi().map(to: Void.self){
                    (client, transaction, response) in
                   
                    try transaction.enqueue {
                        // Remove the job from the delayed queue
                        try client.zrem(RedisKey(.queueDelayed).value, member: job)
                    }
                    
                    try transaction.enqueue {
                        // Add the job to the pending queue
                        // This is not ideal because subsequent delayed jobs would be executed in reverse order,
                        // but this is the best solution, until we can support queues with different priorities
                        try client.rpush(RedisKey(.queuePending(.medium)).value, values: job)
                    }
                    
                    
                }
            }

            
        }

    }

    /**
     Determines whether a job has overcome its time to live in the processing queue.
     
     - returns: `true` if the job has expired, `false` otherwise.
     */
    public func isJobExpired(_ identifier: JobID) throws -> Future<Bool> {

        return try self.client.get(RedisKey(.lock(identifier)).value).map(to: Bool.self){
            response in
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
    public func retryJobIfExpired(_ identifier: JobID) throws -> Future<Bool> {
        
        return try self.isJobExpired(identifier).flatMap(to: Bool.self){
            response in
            
            guard response != false else {
                let app = try Application()
                return app.future(false)
            }
            return try self.client.multi().flatMap(to: Bool.self){
                (client, transaction, response) in
                
                try transaction.enqueue {
                    // Remove the job from the processing queue
                    return try client.lrem(RedisKey(.queueProcessing).value, value: identifier, count: -1).map(to: Bool.self){
                        _ in
                        return true
                    }
                }
                
                try transaction.enqueue {
                    // Add the job to the pending queue
                    return try client.lpush(RedisKey(.queuePending(.medium)).value, values: identifier).map(to: Bool.self){
                        _ in
                        return true
                    }
                }
                
                try transaction.enqueue {
                    // Increment the job's retry attempts
                    return try client.incr(RedisKey(.retry(identifier)).value).map(to: Bool.self){
                        _ in
                        return true
                    }
                }
                
                
                let app = try Application()
                return app.future(false)

                
            }
          //  return true
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

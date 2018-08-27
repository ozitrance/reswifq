//
//  Reswifc.swift
//  Reswifq
//
//  Created by Valerio Mazzeo on 24/02/2017.
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
import Dispatch
import Vapor

public protocol ReswifcProcess {

    var interval: UInt32 { get }

    func process(with req: Request) throws -> Future<Void>
}

/**
 Clock process that manages the repeated execution of individual processes.
 Each process will run in its own thread, at the interval it specifies.
 */
public final class Reswifc {

    // MARK: Initialization

    public init(processes: [ReswifcProcess]) {
        self.processes = processes
    }

    // MARK: Setting and Getting Attributes

    public let processes: [ReswifcProcess]

    // MARK: Processing

    private let group = DispatchGroup()

    private let dispatchQueue = DispatchQueue(label: "com.reswifq.Reswifc", attributes: .concurrent)

    private var isCancelled: Bool = false

    /**
     Starts the clock processing and wait indefinitely.
     */
    public func run(with req: Request) throws {

        for process in processes {
         //   while !self.isCancelled {
            _ = try process.process(with: req).map(to: Void.self){
                print("FINISHED PROCESS CYCLE")
                sleep(process.interval)
                try self.run(with: req)
            }
         //   }

         //   self.group.enter()

        //    self.dispatchQueue.async {


        //        self.group.leave()
        //    }
        }

     //   self.group.wait()
    }

    /**
     Stops the clock processing. This is useful for testing purposes, but probably doesn't have any real case use other than that.
     */
    public func stop(waitUntilAllProcessesAreFinished: Bool = false) {

  //      self.isCancelled = true

        if waitUntilAllProcessesAreFinished {
        //    self.group.wait()
            print("WAS SUPPOSE TO STOP/waitUntilAllProcessesAreFinished")
        }
    }
}

// MARK: - Expired Jobs Monitor

extension Reswifc {

    public final class Monitor: ReswifcProcess {

        // MARK: Initialization

        public init(queue: Reswifq, interval: UInt32 = 300, maxRetryAttempts: Int64 = 5) {
            self.queue = queue
            self.interval = interval
            self.maxRetryAttempts = max(1, maxRetryAttempts)
        }

        // MARK: Setting and Getting Attributes

        /// The source queue of the clock process.
        public let queue: Reswifq

        /**
         Defines how often, in seconds, the clock process have to check for expired jobs.
         */
        public let interval: UInt32

        /**
         The maximum number of retry attempts for an expired job.
         The minimum value is capped to 1.
         */
        public let maxRetryAttempts: Int64
        
        var jobIDs: [JobID]?
        var expired = 0
        var failed = 0
        var processing = 0

        func batchReQueue(with queue: Reswifq, req: Request, promise: Promise<Void>) throws {
            

            
            guard let count = self.jobIDs?.count, count > 0, let jobID = self.jobIDs?.remove(at: 0) else {
                // throw ABotError.nothingToEnqueueForType(type: "categoriesPageData")
                print("batchReQueue ERROR (self.jobIDs: \"\(self.jobIDs?.count)\")")
                promise.succeed()
                return
            }

            _ = try self.queue.retryAttempts(for: jobID).map(to: Void.self){
                retryAttempts in
              //  print("After retryAttempts. retryAttempts = \(retryAttempts)")
                if retryAttempts > self.maxRetryAttempts {
                 //   print("[Reswifc.Monitor] Removing job from the processing queue: \(jobID)")
                    _ = try self.queue.complete(jobID).map(to: Void.self) {
                        if let ids = self.jobIDs, ids.count > 0 {
                            try self.batchReQueue(with: queue, req: req, promise: promise)
                        } else {
                            promise.succeed()
                        }

                    }
                    //  continue
                } else {
                    _ = try self.queue.retryJobIfExpired(with: req, identifier: jobID).map(to: Void.self){
                        response in
                   //     print("[Reswifc.Monitor] Retry job: \(response)")
                        
                        if response {
                            self.expired += 1
                        } else {
                            self.processing += 1
                        }
                        
                        if let ids = self.jobIDs, ids.count > 0 {
                            try self.batchReQueue(with: queue, req: req, promise: promise)
                        } else {
                            promise.succeed()
                        }
                    }
                    
                }
            }

            
            
        }
        
        // MARK: Processing
        
        public func process(with req: Request) throws -> Future<Void> {
            expired = 0
            failed = 0
            processing = 0
            print("starting to process clock")
            let newPromise = req.eventLoop.newPromise(Void.self)
            _ = try self.queue.processingJobs().map(to: Void.self){
                jobIDs in
            //   print(jobIDs)
                self.jobIDs = jobIDs
                _ = try self.batchReQueue(with: self.queue, req: req, promise: newPromise)
            }
            
            return newPromise.futureResult
        }
    }
}

// MARK: - Delayed Jobs Scheduler

extension Reswifc {

    public final class Scheduler: ReswifcProcess {

        // MARK: Initialization

        public init(queue: Reswifq, interval: UInt32 = 60) {
            self.queue = queue
            self.interval = interval
        }

        // MARK: Setting and Getting Attributes

        /// The source queue of the clock process.
        public let queue: Reswifq

        /**
         Defines how often, in seconds, the clock process have to check for expired jobs.
         */
        public let interval: UInt32

        // MARK: Processing

        public func process(with req: Request) throws -> Future<Void> {

            
          //  do {
                return try self.queue.enqueueOverdueJobs(with: req).map(to: Void.self){
                    _ in
                    return
                    }.catchMap() {
                        error in
                        print("[Reswifc.Scheduler] Error while enqueuing delayed jobs: \(error.localizedDescription)")
                        return
                }
             //   }
         //   } catch let error {
              //  print("[Reswifc.Scheduler] Error while enqueuing delayed jobs: \(error.localizedDescription)")
          //  }
        }
    }
}

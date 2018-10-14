//
//  Worker.swift
//  Reswifq
//
//  Created by Valerio Mazzeo on 22/02/2017.
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
//import RedisClient

public class Worker {

    // MARK: Initialization

    public init(queue: Queue, maxConcurrentJobs: Int = 10, averagePollingInterval: UInt32 = 0) {
        self.queue = queue
        self.maxConcurrentJobs = max(1, maxConcurrentJobs)
        self.averagePollingInterval = averagePollingInterval
        self.semaphore = DispatchSemaphore(value: maxConcurrentJobs)
    }

    // MARK: Setting and Getting Attributes

    /// The source queue of the worker process.
    public let queue: Queue

    /**
     Defines the average amount of time (in seconds) which a worker's thread
     has to sleep in between jobs processing.

     When a value of `0` is specified, the worker `dequeue`s jobs,
     setting the wait parameter to `true`, asking the queue to block the thread
     and return only when a job is available.
    */
    public let averagePollingInterval: UInt32

    /**
     The maximum number of concurrent jobs this worker process can handle at the same time.
     The minimum value is capped to 1.
     */
    public let maxConcurrentJobs: Int

    // MARK: Processing

    private let group = DispatchGroup()

    private let dispatchQueue = DispatchQueue(label: "com.reswifq.Worker", attributes: .concurrent)

    private let semaphore: DispatchSemaphore

    private var isCancelled: Bool = false

    /**
     Starts the worker processing and wait indefinitely.
     */
    public func run(with req: Request) throws {
        self.isCancelled = false
        print("Starting run again")
            _ = try self.makeWorkItem(with: req).map(to: Void.self) {
          //      print("ThreadCheck - after running workitem - \(Thread.current). Time is \(Date().rfc1123)")
             //   sleep(5)

                print("after running workitem")
                if self.averagePollingInterval > 0 {
                    sleep(seconds: random(self.averagePollingInterval))
                }
              //  self.group.leave()

             //   self.semaphore.signal()
                try self.run(with: req)

                }.catchMap(){
                    error in
                    print("There was an erro in Worker.run(): \(error)")
        }
        
    }

    /**
     Stops the worker processing. This is useful for testing purposes, but probably doesn't have any real case use other than that. 
     */
    public func stop(waitUntilAllJobsAreFinished: Bool = false) {

        self.isCancelled = true

        if waitUntilAllJobsAreFinished {
            print("Need to waitUntilAllJobsAreFinished")
        //    self.group.wait()
        }
    }
    /*
    public func complete(_ job: JobID) throws {
        self.dispatchQueue.async {
            self.jobs[job] = nil
        }
    }
    */
    
    private func makeBDequeueWorkItem(with req: Request, newPromise: Promise<Void>) throws -> Future<Void> {
        print("Inside makeBDequeueWorkItem BEFORE")
        let priority: QueuePriority = req.http.url.absoluteString.contains("/products") || req.http.url.absoluteString.contains("/asins") || req.http.url.absoluteString.contains("/upcs") ? .high : .medium
        
        return try self.queue.bdequeue(priority: priority).flatMap(to: Void.self){
            persistedJob in
            /*
            let job = try persistedJob.job.data()
            let encoded = try? JSONSerialization.jsonObject(with: job) as? [String: Any] ?? [:]
            print("Inside makeBDequeueWorkItem 2nd")

            if let _ = encoded?["categoryPageData"] {
                print("skipping category")
                return try self.makeBDequeueWorkItem(with: req, newPromise: newPromise)
            } else if let _ = encoded?["productPageData"] {
               // print("product: \(product)")
                
            } else {
                print("encoded: \(encoded). job:\(job)")
            }
             */
            _ = try persistedJob.job.perform(with: req).map(to: Void.self) {
                _ = try self.queue.complete(persistedJob.identifier).map(to: Void.self){
                    print("Inside makeBDequeueWorkItem After COMPLETE")
                    newPromise.succeed()
                  //  return
                }
            }
            
            return newPromise.futureResult.map(to: Void.self){
                print("Inside newPromise After COMPLETE")

            }
            
        }
    }
    
    private func makeDequeueWorkItem(with req: Request) throws -> Future<Void> {
        let priority = req.http.url.absoluteString.contains("/products") || req.http.url.absoluteString.contains("/asins") || req.http.url.absoluteString.contains("/upcs") ? QueuePriority.high : QueuePriority.medium

        return try self.queue.dequeue(priority: priority).flatMap(to: Void.self){
            persistedJob in
       //     let job = try persistedJob.job.data()
        //    let encoded = try? JSONSerialization.jsonObject(with: job) as? [String: Any] ?? [:]
            /*
            if let _ = encoded?["categoryPageData"] {
                print("skipping category)")
                return try self.makeDequeueWorkItem(with: req)
            } else if let _ = encoded?["productPageData"] {
              //  print("product: \(product)")
                
            } else {
                print("encoded: \(encoded). job:\(job)")
            }
             */
            return try persistedJob.job.perform(with: req).flatMap(to: Void.self) {
                return try self.queue.complete(persistedJob.identifier).map(to: Void.self){
                    print("Inside makeDequeueWorkItem After COMPLETE")
                    return
                }
            }
        }
    }
    
    private func makeWorkItem(with req: Request) throws -> Future<Void> {
        let isPollingIntervalEqual0 = self.averagePollingInterval == 0
        let newPromise = req.eventLoop.newPromise(Void.self)
        print("Inside makeWorkItem BEFORE")
        return (isPollingIntervalEqual0 ? try makeBDequeueWorkItem(with: req, newPromise: newPromise) :  try makeDequeueWorkItem(with: req)).map(to: Void.self){
            print("Inside makeWorkItem After COMPLETE")
            return
        }
    }
}

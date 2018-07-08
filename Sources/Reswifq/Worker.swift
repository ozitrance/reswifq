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
    public func run() throws {
        self.isCancelled = false

         //   guard self.semaphore.wait(timeout: .distantFuture) == .success else {
         //      throw Abort(.badRequest) // Not sure if this can ever happen when using distantFuture
        //    }
   //     while !self.isCancelled {

    //    group.enter()

            print("ThreadCheck - before workItem - \(Thread.current). Time is \(Date().rfc1123)")
          //  sleep(5)
            print("before workItem")

            _ = try self.makeWorkItem().map(to: Void.self) {
                print("ThreadCheck - after running workitem - \(Thread.current). Time is \(Date().rfc1123)")
             //   sleep(5)

               // print("after running workitem")
                if self.averagePollingInterval > 0 {
                    sleep(seconds: random(self.averagePollingInterval))
                }
              //  self.group.leave()

             //   self.semaphore.signal()
                try self.run()

            }
     //   }
        /*
        self.isCancelled = false

        while !self.isCancelled {


       //     self.group.enter()

            _ = try self.makeWorkItem().map(to: Void.self) {
                
                print("after running workitem")
                if self.averagePollingInterval > 0 {
                    sleep(seconds: random(self.averagePollingInterval))
                }
            //    self.isCancelled = false

             //   self.semaphore.signal()
                
                //        self.group.leave()

            }
            
        //    guard self.semaphore.wait(timeout: .distantFuture) == .success else {
         //       continue // Not sure if this can ever happen when using distantFuture
        //    }

            /*
            let workItem = try self.makeWorkItem {

                print("after running workitem")
                if self.averagePollingInterval > 0 {
                    sleep(seconds: random(self.averagePollingInterval))
                }

          //      self.semaphore.signal()

        //        self.group.leave()
            }
            */
         //   print("About to run \(workItem)")
      //      self.dispatchQueue.async(execute: workItem)
        }*/
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
    
    private func makeBDequeueWorkItem() throws -> Future<Void> {

     //   let app = try Application()
        return try self.queue.bdequeue().flatMap(to: Void.self){
            persistedJob in
            return try persistedJob.job.perform().flatMap(to: Void.self) {
                return try self.queue.complete(persistedJob.identifier).map(to: Void.self){}
            }
            
        }
    }
    
    private func makeDequeueWorkItem() throws -> Future<Void> {
        
        return try self.queue.dequeue().flatMap(to: Void.self){
            persistedJob in
            print("IM BACK HEREEEEEEEE ")
            
            return try persistedJob.job.perform().flatMap(to: Void.self) {
                print("Back From Performing!")
                return try self.queue.complete(persistedJob.identifier).map(to: Void.self){
                    
                    print("Back From complete")
                }
            }
        }
    }
    
    private func makeWorkItem() throws -> Future<Void> {
        let isPollingIntervalEqual = self.averagePollingInterval == 0
        return isPollingIntervalEqual ? try makeBDequeueWorkItem() : try makeDequeueWorkItem()
    }
}

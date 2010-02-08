/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.monitor.dump.DumpMonitorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.dynamic.DynamicThreadPool;

import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy
 */
public class DeadlockSimulator {

    public static void main(String[] args) {
        ThreadPool threadPool = new DynamicThreadPool();
        DumpMonitorService dumpMonitorService = new DumpMonitorService();
        JvmMonitorService jvmMonitorService = new JvmMonitorService(EMPTY_SETTINGS, threadPool, dumpMonitorService).start();

        //These are the two resource objects
        //we'll try to get locks for
        final Object resource1 = "resource1";
        final Object resource2 = "resource2";
        //Here's the first thread.
        //It tries to lock resource1 then resource2
        Thread t1 = new Thread() {
            public void run() {
                //Lock resource 1
                synchronized (resource1) {
                    System.out.println("Thread 1: locked resource 1");
                    //Pause for a bit, simulating some file I/O or
                    //something. Basically, we just want to give the
                    //other thread a chance to run. Threads and deadlock
                    //are asynchronous things, but we're trying to force
                    //deadlock to happen here...
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                    }

                    //Now wait 'till we can get a lock on resource 2
                    synchronized (resource2) {
                        System.out.println("Thread 1: locked resource 2");
                    }
                }
            }
        };

        //Here's the second thread.
        //It tries to lock resource2 then resource1
        Thread t2 = new Thread() {
            public void run() {
                //This thread locks resource 2 right away
                synchronized (resource2) {
                    System.out.println("Thread 2: locked resource 2");
                    //Then it pauses, for the same reason as the first
                    //thread does
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                    }

                    //Then it tries to lock resource1.
                    //But wait!  Thread 1 locked resource1, and
                    //won't release it till it gets a lock on resource2.
                    //This thread holds the lock on resource2, and won't
                    //release it till it gets resource1.
                    //We're at an impasse. Neither thread can run,
                    //and the program freezes up.
                    synchronized (resource1) {
                        System.out.println("Thread 2: locked resource 1");
                    }
                }
            }
        };

        //Start the two threads.
        //If all goes as planned, deadlock will occur,
        //and the program will never exit.
        t1.start();
        t2.start();
    }
}

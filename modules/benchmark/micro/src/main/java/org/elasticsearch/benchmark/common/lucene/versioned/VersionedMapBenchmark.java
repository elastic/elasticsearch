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

package org.elasticsearch.benchmark.common.lucene.versioned;

import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.lucene.versioned.ConcurrentVersionedMap;
import org.elasticsearch.common.lucene.versioned.ConcurrentVersionedMapLong;
import org.elasticsearch.common.lucene.versioned.NativeVersionedMap;
import org.elasticsearch.common.lucene.versioned.VersionedMap;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author kimchy (Shay Banon)
 */
public class VersionedMapBenchmark {

    private final VersionedMap versionedMap;

    private final int readerIterations;

    private final int writerIterations;

    private final CountDownLatch latch;

    private final Thread[] readerThreads;

    private final Thread[] writerThreads;

    private final CyclicBarrier barrier1;
    private final CyclicBarrier barrier2;

    public VersionedMapBenchmark(VersionedMap versionedMap,
                                 int numberOfReaders, int readerIterations,
                                 int numberOfWriters, int writerIterations) {
        this.versionedMap = versionedMap;
        this.readerIterations = readerIterations;
        this.writerIterations = writerIterations;

        readerThreads = new Thread[numberOfReaders];
        for (int i = 0; i < numberOfReaders; i++) {
            readerThreads[i] = new Thread(new ReaderThread(), "reader[" + i + "]");
        }

        writerThreads = new Thread[numberOfWriters];
        for (int i = 0; i < numberOfWriters; i++) {
            writerThreads[i] = new Thread(new WriterThread(), "writer[" + i + "]");
        }

        latch = new CountDownLatch(numberOfReaders + numberOfWriters);
        barrier1 = new CyclicBarrier(numberOfReaders + numberOfWriters + 1);
        barrier2 = new CyclicBarrier(numberOfReaders + numberOfWriters + 1);

        // now, warm up a bit
        StopWatch stopWatch = new StopWatch("warmup");
        stopWatch.start();
        int warmupSize = 1000000;
        for (int i = 0; i < warmupSize; i++) {
            versionedMap.putVersion(i, i);
            versionedMap.beforeVersion(i, i);
        }
        stopWatch.stop();
        System.out.println("Warmup up of [" + warmupSize + "]: " + stopWatch.totalTime());
        versionedMap.clear();
    }

    public void run() throws Exception {
        for (int i = 0; i < 3; i++) {
            System.gc();
            MILLISECONDS.sleep(100);
        }

        long emptyUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

        for (Thread t : readerThreads) {
            t.start();
        }
        for (Thread t : writerThreads) {
            t.start();
        }
        barrier1.await();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        barrier2.await();

        latch.await();
        stopWatch.stop();

        // verify that the writers wrote...
        for (int i = 0; i < writerIterations; i++) {
            if (versionedMap.beforeVersion(i, Integer.MAX_VALUE)) {
                System.out.println("Wrong value for [" + i + ']');
            }
        }

        System.out.println("Total:");
        System.out.println("   - [" + readerThreads.length + "] readers with [" + readerIterations + "] iterations");
        System.out.println("   - [" + writerThreads.length + "] writers with [" + writerIterations + "] iterations");
        System.out.println("   - Took: " + stopWatch.totalTime());

        for (int i = 0; i < 3; i++) {
            System.gc();
            MILLISECONDS.sleep(100);
        }

        long bytesTaken = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() - emptyUsed;
        System.out.println("Size of [" + writerIterations + "] entries is " + new ByteSizeValue(bytesTaken));
    }

    private class ReaderThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < readerIterations; i++) {
                versionedMap.beforeVersion(i, i);
            }
            latch.countDown();
        }
    }

    private class WriterThread implements Runnable {
        @Override public void run() {
            try {
                barrier1.await();
                barrier2.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (int i = 0; i < writerIterations; i++) {
                versionedMap.putVersionIfAbsent(i, i);
            }
            latch.countDown();
        }
    }

    // Some results: Two cores machine, general average across 5 runs

//    VersionedMapBenchmark benchmark = new VersionedMapBenchmark(
//            versionedMap, 30, 2000000, 10, 2000000
//    );

//        Running [native] type
//        Took StopWatch '': running time  = 11.9s
//        -----------------------------------------
//        ms     %     Task name
//        -----------------------------------------
//        11909  100%
//
//        Size of [2000000] entries is 17.9mb

//        Running [nb] type
//        Took StopWatch '': running time  = 6.1s
//        -----------------------------------------
//        ms     %     Task name
//        -----------------------------------------
//        06134  100%
//
//        Size of [2000000] entries is 77.6mb

    public static void main(String[] args) throws Exception {
        String type = args.length > 0 ? args[0] : "nb";
        VersionedMap versionedMap;
        if ("nb".equalsIgnoreCase(type)) {
            versionedMap = new ConcurrentVersionedMapLong();
        } else if ("native".equalsIgnoreCase(type)) {
            versionedMap = new NativeVersionedMap();
        } else if ("concurrent".equalsIgnoreCase(type)) {
            versionedMap = new ConcurrentVersionedMap();
        } else {
            throw new IllegalArgumentException("Type [" + type + "] unknown");
        }
        System.out.println("Running [" + type + "] type");
        VersionedMapBenchmark benchmark = new VersionedMapBenchmark(
                versionedMap, 30, 2000000, 10, 2000000
        );
        benchmark.run();
    }
}

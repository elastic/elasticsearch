/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.benchmark.time;

import org.elasticsearch.common.StopWatch;

import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class SimpleTimeBenchmark {

    private static boolean USE_NANO_TIME = false;
    private static long NUMBER_OF_ITERATIONS = 1000000;
    private static int NUMBER_OF_THREADS = 100;

    public static void main(String[] args) throws Exception {
        StopWatch stopWatch = new StopWatch().start();
        System.out.println("Running " + NUMBER_OF_ITERATIONS);
        for (long i = 0; i < NUMBER_OF_ITERATIONS; i++) {
            System.currentTimeMillis();
        }
        System.out.println("Took " + stopWatch.stop().totalTime() + " TP Millis " + (NUMBER_OF_ITERATIONS / stopWatch.totalTime().millisFrac()));

        System.out.println("Running using " + NUMBER_OF_THREADS + " threads with " + NUMBER_OF_ITERATIONS + " iterations");
        final CountDownLatch latch = new CountDownLatch(NUMBER_OF_THREADS);
        Thread[] threads = new Thread[NUMBER_OF_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    if (USE_NANO_TIME) {
                        for (long i = 0; i < NUMBER_OF_ITERATIONS; i++) {
                            System.nanoTime();
                        }
                    } else {
                        for (long i = 0; i < NUMBER_OF_ITERATIONS; i++) {
                            System.currentTimeMillis();
                        }
                    }
                    latch.countDown();
                }
            });
        }
        stopWatch = new StopWatch().start();
        for (Thread thread : threads) {
            thread.start();
        }
        latch.await();
        stopWatch.stop();
        System.out.println("Took " + stopWatch.totalTime() + " TP Millis " + ((NUMBER_OF_ITERATIONS * NUMBER_OF_THREADS) / stopWatch.totalTime().millisFrac()));
    }
}

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
package org.elasticsearch.search.facet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentDuel<T> {

    
    private final ExecutorService pool;
    private final int numExecutorThreads;
    
    public ConcurrentDuel(int numThreads) {
        pool = Executors.newFixedThreadPool(numThreads);
        this.numExecutorThreads = numThreads;
    }

    public void close() {
        pool.shutdown();
    }
    
    public List<T> runDuel(final DuelExecutor<T> executor, int iterations, int numTasks) throws InterruptedException, ExecutionException {
        List<T> results = new ArrayList<>();
        T firstRun = executor.run();
        results.add(firstRun);
        for (int i = 0; i < 3; i++) {
        
        }
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicLong count = new AtomicLong(iterations);
        List<Future<List<T>>> futures = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            futures.add(pool.submit(new Callable<List<T>>() {

                @Override
                public List<T> call() throws Exception {
                    List<T> results = new ArrayList<>();
                    latch.await();
                    while(count.decrementAndGet() >= 0) {
                        results.add(executor.run());    
                    }
                    return results;
                }
            }));
        }
        latch.countDown();
        for (Future<List<T>> future : futures) {
            results.addAll(future.get());
        }
        return results;
    }
    public void duel(DuelJudge<T> judge, final DuelExecutor<T> executor, int iterations) throws InterruptedException, ExecutionException {
        duel(judge, executor, iterations, numExecutorThreads);
    }
    
    public void duel(DuelJudge<T> judge, final DuelExecutor<T> executor, int iterations, int threadCount) throws InterruptedException, ExecutionException {
        T firstRun = executor.run();
        List<T> runDuel = runDuel(executor, iterations, threadCount);
        for (T t : runDuel) {
            judge.judge(firstRun, t);
        }
    }
    
    public static interface DuelExecutor<T> {
        public T run();
    }
    
    public static interface DuelJudge<T> {
        public void judge(T firstRun, T result);
    }
}

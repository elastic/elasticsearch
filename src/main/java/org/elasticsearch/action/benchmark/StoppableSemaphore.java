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

package org.elasticsearch.action.benchmark;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
* Stoppable semaphore for controlling benchmark execution
*/
final class StoppableSemaphore {

    private final int        permits;
    private final Semaphore  semaphore;
    private volatile boolean stopped;

    public StoppableSemaphore(int permits) {
        this.permits   = permits;
        this.semaphore = new Semaphore(permits);
    }

    public void acquire() throws InterruptedException {
        if (stopped) {
            throw new InterruptedException("Benchmark interrupted");
        }
        semaphore.acquire();
    }

    public boolean tryAcquireAll(long timeout, TimeUnit unit) throws InterruptedException {
        if (stopped) {
            throw new InterruptedException("Benchmark interrupted");
        }
        return semaphore.tryAcquire(permits, timeout, unit);
    }

    public void release() {
        semaphore.release();
    }

    public void releaseAll() {
        semaphore.release(permits);
    }

    public void stop() {
        stopped = true;
    }
}

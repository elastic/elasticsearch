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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.benchmark.exception.BenchmarkPauseTimedOutException;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartResponse;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark runtime state
 */
public class BenchmarkState {

    protected static final ESLogger logger = Loggers.getLogger(BenchmarkState.class);

    private static final long TIMEOUT = 60;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;

    final String benchmarkId;
    final BenchmarkStartResponse response;

    private final ImmutableOpenMap<String, StoppableSemaphore> semaphores;
    private final Object lock = new Object();

    private volatile boolean stopped = false;
    private volatile boolean paused  = false;

    BenchmarkState(BenchmarkStartRequest request, BenchmarkStartResponse response) {

        this.benchmarkId = request.benchmarkId();
        this.response = response;

        ImmutableOpenMap.Builder<String, StoppableSemaphore> builder = ImmutableOpenMap.builder();
        for (BenchmarkCompetitor competitor : request.competitors()) {
            builder.put(competitor.name(), new StoppableSemaphore(competitor.settings().concurrency()));
        }
        semaphores = builder.build();
    }

    void stopAllCompetitors() {
        synchronized (lock) {
            if (!stopped) {
                for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                    entry.value.stop();
                }
                stopped = true;
            }
        }
    }

    void abortAllCompetitors() {
        synchronized (lock) {
            if (!stopped) {
                for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                    entry.value.stop();
                }
                response.state(BenchmarkStartResponse.State.ABORTED);
                stopped = true;
            }
        }
    }

    void pauseAllCompetitors() {
        synchronized (lock) {
            if (!paused && !stopped) {
                for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                    try {
                        entry.value.tryAcquireAll(TIMEOUT, TIMEUNIT);
                        logger.debug("benchmark [{}]: competitor [{}] paused", benchmarkId, entry.key);
                    } catch (InterruptedException e) {
                        throw new BenchmarkPauseTimedOutException("Timed out attempting to pause [" + benchmarkId + "] [" + entry.key + "]", e);
                    }
                }
                response.state(BenchmarkStartResponse.State.PAUSED);
                paused = true;
            }
        }
    }

    void resumeAllCompetitors() {
        synchronized (lock) {
            if (paused && !stopped) {
                for (ObjectObjectCursor<String, StoppableSemaphore> entry : semaphores) {
                    entry.value.releaseAll();
                    logger.debug("benchmark [{}]: competitor [{}] resumed", benchmarkId, entry.key);
                }
                response.state(BenchmarkStartResponse.State.RUNNING);
                paused = false;
            }
        }
    }

    StoppableSemaphore competitorSemaphore(String name) {
        return semaphores.get(name);
    }
}

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

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The {@link CooldownEsThreadPoolExecutor} extends our regular fixed {@link EsThreadPoolExecutor}
 * to allow single-threaded execution of a {@link Runnable} separated by a configurable cooldown
 * period.
 */
public class CooldownEsThreadPoolExecutor extends EsThreadPoolExecutor {
    private static final Logger logger = LogManager.getLogger(CooldownEsThreadPoolExecutor.class);

    private final long cooldownMillis;
    private final LongSupplier nowSupplier;

    private volatile long lastExecutionFinished = 0;

    CooldownEsThreadPoolExecutor(String name, TimeValue cooldown, LongSupplier nowSupplier,
                                 BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, ThreadContext contextHolder) {
        super(name, 1, 1, 0, TimeUnit.SECONDS, workQueue, threadFactory, contextHolder);
        this.cooldownMillis = cooldown.millis();
        this.nowSupplier = nowSupplier;
    }

    void pause(long now, long cooldownRemainingMillis) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("running task at [{}], last run finished [{}]," +
                        " cooldown of [{}], pausing for remaining cooldown [{}/{}s]",
                    now, lastExecutionFinished,
                    TimeValue.timeValueMillis(cooldownMillis),
                    TimeValue.timeValueMillis(cooldownRemainingMillis),
                    TimeValue.timeValueMillis(cooldownRemainingMillis).seconds());
            }
            Thread.sleep(cooldownRemainingMillis);
        } catch (InterruptedException e) {
            logger.error("interrupted while waiting for [{}/{}s] cooldown to elapse (max cooldown: {})",
                TimeValue.timeValueMillis(cooldownRemainingMillis),
                TimeValue.timeValueMillis(cooldownRemainingMillis).seconds(),
                TimeValue.timeValueMillis(cooldownMillis));
        }
    }

    long getLastExecutionFinished() {
        return this.lastExecutionFinished;
    }

    @Override
    protected Runnable wrapRunnable(Runnable command) {
        return () -> {
            final long now = nowSupplier.getAsLong();
            final long cooldownRemainingMillis = cooldownMillis - (now - lastExecutionFinished);
            if (cooldownRemainingMillis > 0) {
                pause(now, cooldownRemainingMillis);
            } else {
                logger.trace("running task at [{}], last run finished [{}]," +
                        " cooldown of [{}], no pausing remaining cooldown",
                    now, lastExecutionFinished,
                    TimeValue.timeValueMillis(cooldownMillis));
            }
            command.run();
        };
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            super.afterExecute(r, t);
        } finally {
            long now = nowSupplier.getAsLong();
            logger.trace("updating last run information to [{}]", now);
            lastExecutionFinished = now;
        }
    }
}

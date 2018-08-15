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

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with probability
 * that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send a message to
 * another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by attempting elections
 * randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an upper bound on the backoff
 * so that if elections are failing due to a network partition that lasts for a long time then when the partition heals there is an election
 * attempt reasonably quickly.
 */
public class ElectionSchedulerFactory extends AbstractComponent {

    // bounds on the time between election attempts
    private static final String ELECTION_INITIAL_TIMEOUT_SETTING_KEY = "cluster.election.initial_timeout";
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";

    public static final Setting<TimeValue> ELECTION_INITIAL_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_INITIAL_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), Property.NodeScope);

    private final TimeValue initialTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final ThreadPool threadPool;
    private final Random random;

    public ElectionSchedulerFactory(Settings settings, Random random, ThreadPool threadPool) {
        super(settings);

        this.random = random;
        this.threadPool = threadPool;

        initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);
    }

    /**
     * Start the process to schedule repeated election attempts.
     *
     * @param gracePeriod       An initial period to wait before attempting the first election.
     * @param scheduledRunnable The action to run each time an election should be attempted.
     */
    public Releasable startElectionScheduler(TimeValue gracePeriod, Runnable scheduledRunnable) {
        final ElectionScheduler currentScheduler = new ElectionScheduler();
        currentScheduler.scheduleNextElection(gracePeriod, scheduledRunnable);
        return currentScheduler;
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n);
    }

    /**
     * @param randomSupplier supplier of randomly-chosen longs
     * @param upperBound     exclusive upper bound
     */
    // package-private for testing
    static long randomPositiveLongLessThan(LongSupplier randomSupplier, long upperBound) {
        assert 1 < upperBound : upperBound;
        return nonNegative(randomSupplier.getAsLong()) % (upperBound - 1) + 1;
    }

    @Override
    public String toString() {
        return "ElectionSchedulerFactory{" +
            "initialTimeout=" + initialTimeout +
            ", backoffTime=" + backoffTime +
            ", maxTimeout=" + maxTimeout +
            '}';
    }

    private class ElectionScheduler implements Releasable {
        /**
         * The current maximum timeout: the next election is scheduled randomly no later than this number of milliseconds in the future. On
         * each election attempt this value is increased by `backoffTime`, up to the `maxTimeout`, to adapt to higher-than-expected latency.
         */
        private final AtomicLong currentMaxTimeoutMillis = new AtomicLong(initialTimeout.millis());
        private final AtomicBoolean isClosed = new AtomicBoolean();

        /**
         * Calculate the next maximum timeout by backing off the current value by `backoffTime` up to the `maxTimeout`.
         */
        private long backOffCurrentMaxTimeout(long currentMaxTimeoutMillis) {
            return Math.min(maxTimeout.getMillis(), currentMaxTimeoutMillis + backoffTime.getMillis());
        }

        void scheduleNextElection(final TimeValue gracePeriod, final Runnable scheduledRunnable) {
            if (isClosed.get()) {
                logger.debug("{} not scheduling election", this);
                return;
            }

            final long maxDelayMillis = currentMaxTimeoutMillis.getAndUpdate(this::backOffCurrentMaxTimeout);
            final long delayMillis = randomPositiveLongLessThan(random::nextLong, maxDelayMillis + 1) + gracePeriod.millis();
            final Runnable runnable = new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception in wakeup", e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    if (isClosed.get()) {
                        logger.debug("{} not starting election", this);
                        return;
                    }
                    logger.debug("{} starting election", this);
                    scheduledRunnable.run();
                }

                @Override
                public void onAfter() {
                    scheduleNextElection(TimeValue.ZERO, scheduledRunnable);
                }

                @Override
                public String toString() {
                    return "scheduleNextElection{gracePeriod=" + gracePeriod
                        + ", maxDelayMillis=" + maxDelayMillis
                        + ", delayMillis=" + delayMillis
                        + ", " + ElectionScheduler.this + "}";
                }

                @Override
                public boolean isForceExecution() {
                    // There are very few of these scheduled, and they back off, but it's important that they're not rejected as
                    // this could prevent a cluster from ever forming.
                    return true;
                }
            };

            logger.debug("scheduling {}", runnable);
            threadPool.schedule(TimeValue.timeValueMillis(delayMillis), Names.GENERIC, runnable);
        }

        @Override
        public String toString() {
            return "ElectionScheduler{currentMaxTimeoutMillis=" + currentMaxTimeoutMillis
                + ", " + ElectionSchedulerFactory.this + "}";
        }

        @Override
        public void close() {
            boolean isClosedChanged = isClosed.compareAndSet(false, true);
            assert isClosedChanged;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;

/**
 * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with probability
 * that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send a message to
 * another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by attempting elections
 * randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an upper bound on the backoff
 * so that if elections are failing due to a network partition that lasts for a long time then when the partition heals there is an election
 * attempt reasonably quickly.
 */
public class ElectionSchedulerFactory {

    private static final Logger logger = LogManager.getLogger(ElectionSchedulerFactory.class);

    private static final String ELECTION_INITIAL_TIMEOUT_SETTING_KEY = "cluster.election.initial_timeout";
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";
    private static final String ELECTION_DURATION_SETTING_KEY = "cluster.election.duration";

    /*
     * The first election is scheduled to occur a random number of milliseconds after the scheduler is started, where the random number of
     * milliseconds is chosen uniformly from
     *
     *     (0, min(ELECTION_INITIAL_TIMEOUT_SETTING, ELECTION_MAX_TIMEOUT_SETTING)]
     *
     * For `n > 1`, the `n`th election is scheduled to occur a random number of milliseconds after the `n - 1`th election, where the random
     * number of milliseconds is chosen uniformly from
     *
     *     (0, min(ELECTION_INITIAL_TIMEOUT_SETTING + (n-1) * ELECTION_BACK_OFF_TIME_SETTING, ELECTION_MAX_TIMEOUT_SETTING)]
     *
     * Each election lasts up to ELECTION_DURATION_SETTING.
     */

    public static final Setting<TimeValue> ELECTION_INITIAL_TIMEOUT_SETTING = Setting.timeSetting(
        ELECTION_INITIAL_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(10),
        Property.NodeScope
    );

    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(
        ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(60),
        Property.NodeScope
    );

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(
        ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueMillis(200),
        TimeValue.timeValueSeconds(600),
        Property.NodeScope
    );

    public static final Setting<TimeValue> ELECTION_DURATION_SETTING = Setting.timeSetting(
        ELECTION_DURATION_SETTING_KEY,
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueSeconds(600),
        Property.NodeScope
    );

    private final TimeValue initialTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final TimeValue duration;
    private final ThreadPool threadPool;
    private final Random random;

    public ElectionSchedulerFactory(Settings settings, Random random, ThreadPool threadPool) {
        this.random = random;
        this.threadPool = threadPool;

        initialTimeout = ELECTION_INITIAL_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);
        duration = ELECTION_DURATION_SETTING.get(settings);

        if (maxTimeout.millis() < initialTimeout.millis()) {
            throw new IllegalArgumentException(
                format(
                    "[%s] is [%s], but must be at least [%s] which is [%s]",
                    ELECTION_MAX_TIMEOUT_SETTING_KEY,
                    maxTimeout,
                    ELECTION_INITIAL_TIMEOUT_SETTING_KEY,
                    initialTimeout
                )
            );
        }
    }

    /**
     * Start the process to schedule repeated election attempts.
     *
     * @param gracePeriod       An initial period to wait before attempting the first election.
     * @param scheduledRunnable The action to run each time an election should be attempted.
     */
    public Releasable startElectionScheduler(TimeValue gracePeriod, Runnable scheduledRunnable) {
        final ElectionScheduler scheduler = new ElectionScheduler();
        scheduler.scheduleNextElection(gracePeriod, scheduledRunnable);
        return scheduler;
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n);
    }

    /**
     * @param randomNumber a randomly-chosen long
     * @param upperBound   inclusive upper bound
     * @return a number in the range (0, upperBound]
     */
    // package-private for testing
    static long toPositiveLongAtMost(long randomNumber, long upperBound) {
        assert 0 < upperBound : upperBound;
        return nonNegative(randomNumber) % upperBound + 1;
    }

    @Override
    public String toString() {
        return "ElectionSchedulerFactory{"
            + "initialTimeout="
            + initialTimeout
            + ", backoffTime="
            + backoffTime
            + ", maxTimeout="
            + maxTimeout
            + '}';
    }

    private class ElectionScheduler implements Releasable {
        private final AtomicBoolean isClosed = new AtomicBoolean();
        private final AtomicLong attempt = new AtomicLong();

        void scheduleNextElection(final TimeValue gracePeriod, final Runnable scheduledRunnable) {
            if (isClosed.get()) {
                logger.debug("{} not scheduling election", this);
                return;
            }

            final long thisAttempt = attempt.getAndIncrement();
            // to overflow here would take over a million years of failed election attempts, so we won't worry about that:
            final long maxDelayMillis = Math.min(maxTimeout.millis(), initialTimeout.millis() + thisAttempt * backoffTime.millis());
            final long delayMillis = toPositiveLongAtMost(random.nextLong(), maxDelayMillis) + gracePeriod.millis();
            final Runnable runnable = new AbstractRunnable() {

                @Override
                public void onRejection(Exception e) {
                    logger.debug("threadpool was shut down", e);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> format("unexpected exception in wakeup of %s", this), e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    if (isClosed.get()) {
                        logger.debug("{} not starting election", this);
                    } else {
                        logger.debug("{} starting election", this);
                        scheduleNextElection(duration, scheduledRunnable);
                        scheduledRunnable.run();
                    }
                }

                @Override
                public String toString() {
                    return "scheduleNextElection{gracePeriod="
                        + gracePeriod
                        + ", thisAttempt="
                        + thisAttempt
                        + ", maxDelayMillis="
                        + maxDelayMillis
                        + ", delayMillis="
                        + delayMillis
                        + ", "
                        + ElectionScheduler.this
                        + "}";
                }
            };

            logger.debug("scheduling {}", runnable);
            threadPool.scheduleUnlessShuttingDown(TimeValue.timeValueMillis(delayMillis), Names.CLUSTER_COORDINATION, runnable);
        }

        @Override
        public String toString() {
            return "ElectionScheduler{attempt=" + attempt + ", " + ElectionSchedulerFactory.this + "}";
        }

        @Override
        public void close() {
            boolean wasNotPreviouslyClosed = isClosed.compareAndSet(false, true);
            assert wasNotPreviouslyClosed;
        }
    }
}

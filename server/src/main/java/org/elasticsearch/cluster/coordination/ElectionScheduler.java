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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Random;
import java.util.function.BooleanSupplier;

public abstract class ElectionScheduler extends AbstractComponent {

    /*
     * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with
     * probability that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send
     * a message to another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by
     * attempting elections randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an
     * upper bound on the backoff so that if elections are failing due to a network partition that lasts for a long time then when the
     * partition heals there is an election attempt reasonably quickly.
     */

    // bounds on the time between election attempts
    private static final String ELECTION_MIN_TIMEOUT_SETTING_KEY = "cluster.election.min_timeout";
    private static final String ELECTION_BACK_OFF_TIME_SETTING_KEY = "cluster.election.back_off_time";
    private static final String ELECTION_MAX_TIMEOUT_SETTING_KEY = "cluster.election.max_timeout";

    public static final Setting<TimeValue> ELECTION_MIN_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MIN_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_BACK_OFF_TIME_SETTING = Setting.timeSetting(ELECTION_BACK_OFF_TIME_SETTING_KEY,
        TimeValue.timeValueMillis(100), TimeValue.timeValueMillis(1), Property.NodeScope);

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), Property.NodeScope);

    static String validationExceptionMessage(final String electionMinTimeout, final String electionMaxTimeout) {
        return new ParameterizedMessage(
            "Invalid election retry timeouts: [{}] is [{}] and [{}] is [{}], but [{}] should be at least 100ms longer than [{}]",
            ELECTION_MIN_TIMEOUT_SETTING_KEY, electionMinTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, electionMaxTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, ELECTION_MIN_TIMEOUT_SETTING_KEY).getFormattedMessage();
    }

    private final TimeValue minTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final ThreadPool threadPool;
    private final Random random;

    private final Object mutex = new Object();
    private Object currentScheduler; // only care about its identity
    private long nextSchedulerIdentity;
    private long currentDelayMillis;

    ElectionScheduler(Settings settings, Random random, ThreadPool threadPool) {
        super(settings);

        this.random = random;
        this.threadPool = threadPool;

        minTimeout = ELECTION_MIN_TIMEOUT_SETTING.get(settings);
        backoffTime = ELECTION_BACK_OFF_TIME_SETTING.get(settings);
        maxTimeout = ELECTION_MAX_TIMEOUT_SETTING.get(settings);

        if (maxTimeout.millis() < minTimeout.millis() + 100) {
            throw new IllegalArgumentException(validationExceptionMessage(minTimeout.toString(), maxTimeout.toString()));
        }
    }

    public void start() {
        final BooleanSupplier isRunningSupplier;
        synchronized (mutex) {
            assert currentScheduler == null;
            final long schedulerIdentity = nextSchedulerIdentity++;
            currentDelayMillis = minTimeout.millis();
            currentScheduler = isRunningSupplier = new BooleanSupplier() {
                @Override
                public boolean getAsBoolean() {
                    assert Thread.holdsLock(mutex) : "ElectionScheduler mutex not held";
                    return this == currentScheduler;
                }

                @Override
                public String toString() {
                    return "isRunningSupplier[" + schedulerIdentity + "]";
                }
            };
            logger.debug("starting {}", currentScheduler);
        }

        scheduleNextElection(isRunningSupplier);
    }

    public void stop() {
        synchronized (mutex) {
            assert currentScheduler != null;
            logger.debug("stopping {}", currentScheduler);
            currentScheduler = null;
        }
    }

    private void scheduleNextElection(final BooleanSupplier isRunningSupplier) {
        final long delay;
        synchronized (mutex) {
            if (isRunningSupplier.getAsBoolean() == false) {
                logger.debug("{} not scheduling election", isRunningSupplier);
                return;
            }
            currentDelayMillis = Math.min(maxTimeout.getMillis(), currentDelayMillis + backoffTime.getMillis());
            delay = randomLongBetween(minTimeout.getMillis(), currentDelayMillis + 1);
            logger.debug("{} scheduling election with delay [{}ms] (min={}, current={}, backoff={}, max={})",
                isRunningSupplier, delay, minTimeout, currentDelayMillis, backoffTime, maxTimeout);
        }
        threadPool.schedule(TimeValue.timeValueMillis(delay), Names.GENERIC, new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.debug("unexpected exception in wakeup", e);
                assert false : e;
            }

            @Override
            protected void doRun() {
                synchronized (mutex) {
                    if (isRunningSupplier.getAsBoolean() == false) {
                        logger.debug("{} not starting election", isRunningSupplier);
                        return;
                    }
                }
                logger.debug("{} starting election", isRunningSupplier);
                startElection();
            }

            @Override
            public void onAfter() {
                scheduleNextElection(isRunningSupplier);
            }

            @Override
            public String toString() {
                return "scheduleNextElection[" + isRunningSupplier + "]";
            }

            @Override
            public boolean isForceExecution() {
                // There are very few of these scheduled, and they back off, but it's important that they're not rejected as
                // this could prevent a cluster from ever forming.
                return true;
            }
        });
    }

    @SuppressForbidden(reason = "Argument to Math.abs() is definitely not Long.MIN_VALUE")
    private static long nonNegative(long n) {
        return n == Long.MIN_VALUE ? 0 : Math.abs(n);
    }

    /**
     * @param lowerBound inclusive lower bound
     * @param upperBound exclusive upper bound
     */
    private long randomLongBetween(long lowerBound, long upperBound) {
        assert 0 < upperBound - lowerBound;
        return nonNegative(random.nextLong()) % (upperBound - lowerBound) + lowerBound;
    }

    /**
     * Start an election. Calls to this method are not completely synchronised with the start/stop state of the scheduler.
     */
    protected abstract void startElection();

    @Override
    public String toString() {
        return "ElectionScheduler{" +
            "minTimeout=" + minTimeout +
            ", maxTimeout=" + maxTimeout +
            ", backoffTime=" + backoffTime +
            ", currentDelayMillis=" + currentDelayMillis +
            ", currentScheduler=" + currentScheduler +
            '}';
    }
}

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

    public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING= Setting.timeSetting(ELECTION_MAX_TIMEOUT_SETTING_KEY,
        TimeValue.timeValueSeconds(10), TimeValue.timeValueMillis(200), Property.NodeScope);

    static String validationExceptionMessage(final String electionMinTimeout, final String electionMaxTimeout) {
        return new ParameterizedMessage(
            "Invalid election retry timeouts: [{}] is [{}] and [{}] is [{}], but [{}] should be at least 100ms longer than [{}]",
            ELECTION_MIN_TIMEOUT_SETTING_KEY, electionMinTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, electionMaxTimeout,
            ELECTION_MAX_TIMEOUT_SETTING_KEY, ELECTION_MIN_TIMEOUT_SETTING_KEY).getFormattedMessage();
    }

    private Object currentScheduler; // only care about its identity
    private final TimeValue minTimeout;
    private final TimeValue backoffTime;
    private final TimeValue maxTimeout;
    private final Object mutex = new Object();
    private final ThreadPool threadPool;
    private final Random random;

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
        logger.trace("starting");
        final Runnable newScheduler;
        synchronized (mutex) {
            currentDelayMillis = minTimeout.millis();
            assert currentScheduler == null;
            currentScheduler = newScheduler = new Runnable() {
                @Override
                public String toString() {
                    return "ElectionScheduler: next election scheduler";
                }

                private boolean isCurrentScheduler() {
                    return this == currentScheduler;
                }

                private void scheduleNextElection() {
                    final long delay;
                    synchronized (mutex) {
                        if (isCurrentScheduler() == false) {
                            logger.trace("not scheduling election");
                            return;
                        }
                        currentDelayMillis = Math.min(maxTimeout.getMillis(), currentDelayMillis + backoffTime.getMillis());
                        delay = randomLongBetween(minTimeout.getMillis(), currentDelayMillis + 1);
                    }
                    logger.trace("scheduling election after delay of [{}ms]", delay);
                    threadPool.schedule(TimeValue.timeValueMillis(delay), Names.GENERIC, new AbstractRunnable() {
                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("unexpected exception in wakeup", e);
                            assert false : e;
                        }

                        @Override
                        protected void doRun() {
                            synchronized (mutex) {
                                if (isCurrentScheduler() == false) {
                                    return;
                                }
                            }
                            startElection();
                        }

                        @Override
                        public void onAfter() {
                            scheduleNextElection();
                        }

                        @Override
                        public String toString() {
                            return "ElectionScheduler: do election and schedule retry";
                        }

                        @Override
                        public boolean isForceExecution() {
                            // There are very few of these scheduled, and they back off, but it's important that they're not rejected as
                            // this could prevent a cluster from ever forming.
                            return true;
                        }
                    });
                }

                @Override
                public void run() {
                    scheduleNextElection();
                }
            };
        }
        newScheduler.run();
    }

    public void stop() {
        logger.trace("stopping");
        synchronized (mutex) {
            assert currentScheduler != null;
            currentScheduler = null;
        }
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
            ", currentDelayMillis=" + currentDelayMillis +
            '}';
    }
}

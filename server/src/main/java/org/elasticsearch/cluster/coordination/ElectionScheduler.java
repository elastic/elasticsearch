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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

public abstract class ElectionScheduler extends AbstractLifecycleComponent {

    /*
     * It's provably impossible to guarantee that any leader election algorithm ever elects a leader, but they generally work (with
     * probability that approaches 1 over time) as long as elections occur sufficiently infrequently, compared to the time it takes to send
     * a message to another node and receive a response back. We do not know the round-trip latency here, but we can approximate it by
     * attempting elections randomly at reasonably high frequency and backing off (linearly) until one of them succeeds. We also place an
     * upper bound on the backoff so that if elections are failing due to a network partition that lasts for a long time then when the
     * partition heals there is an election attempt reasonably quickly.
     */

    // bounds on the time between election attempts
    private static final String ELECTION_MIN_RETRY_INTERVAL_SETTING_KEY = "discovery.election.min_retry_interval";
    private static final String ELECTION_MAX_RETRY_INTERVAL_SETTING_KEY = "discovery.election.max_retry_interval";

    public static final Setting<TimeValue> ELECTION_MIN_RETRY_INTERVAL_SETTING
        = new Setting<>(ELECTION_MIN_RETRY_INTERVAL_SETTING_KEY, "300ms",
        reasonableTimeParser(ELECTION_MIN_RETRY_INTERVAL_SETTING_KEY),
        new ElectionMinRetryIntervalSettingValidator(), Property.NodeScope, Property.Dynamic);

    public static final Setting<TimeValue> ELECTION_MAX_RETRY_INTERVAL_SETTING
        = new Setting<>(ELECTION_MAX_RETRY_INTERVAL_SETTING_KEY, "10s",
        reasonableTimeParser(ELECTION_MAX_RETRY_INTERVAL_SETTING_KEY),
        new ElectionMaxRetryIntervalSettingValidator(), Property.NodeScope, Property.Dynamic);

    private static Function<String, TimeValue> reasonableTimeParser(final String settingKey) {
        return s -> {
            TimeValue timeValue = TimeValue.parseTimeValue(s, null, settingKey);
            if (timeValue.millis() < 1) {
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + settingKey + "] must be >= [1ms]");
            }
            if (timeValue.millis() > 60000) {
                throw new IllegalArgumentException("Failed to parse value [" + s + "] for setting [" + settingKey + "] must be <= [60s]");
            }
            return timeValue;
        };
    }

    private static final class ElectionMinRetryIntervalSettingValidator implements Setting.Validator<TimeValue> {
        @Override
        public void validate(final TimeValue value, final Map<Setting<TimeValue>, TimeValue> settings) {
            validateSettings(value, settings.get(ELECTION_MAX_RETRY_INTERVAL_SETTING));
        }

        @Override
        public Iterator<Setting<TimeValue>> settings() {
            return Collections.singletonList(ELECTION_MAX_RETRY_INTERVAL_SETTING).iterator();
        }
    }

    private static final class ElectionMaxRetryIntervalSettingValidator implements Setting.Validator<TimeValue> {
        @Override
        public void validate(final TimeValue value, final Map<Setting<TimeValue>, TimeValue> settings) {
            validateSettings(settings.get(ELECTION_MIN_RETRY_INTERVAL_SETTING), value);
        }

        @Override
        public Iterator<Setting<TimeValue>> settings() {
            return Collections.singletonList(ELECTION_MIN_RETRY_INTERVAL_SETTING).iterator();
        }
    }

    static void validateSettings(final TimeValue electionMinRetryInterval, final TimeValue electionMaxRetryInterval) {
        if (electionMaxRetryInterval.millis() < electionMinRetryInterval.millis() + 100) {
            throw new IllegalArgumentException(validationExceptionMessage(
                electionMinRetryInterval.toString(), electionMaxRetryInterval.toString()));
        }
    }

    static String validationExceptionMessage(final String electionMinRetryInterval, final String electionMaxRetryInterval) {
        return new ParameterizedMessage(
            "Invalid election retry intervals: [{}] is [{}] and [{}] is [{}], but [{}] should be at least 100ms longer than [{}]",
            ELECTION_MIN_RETRY_INTERVAL_SETTING_KEY, electionMinRetryInterval,
            ELECTION_MAX_RETRY_INTERVAL_SETTING_KEY, electionMaxRetryInterval,
            ELECTION_MAX_RETRY_INTERVAL_SETTING_KEY, ELECTION_MIN_RETRY_INTERVAL_SETTING_KEY).getFormattedMessage();
    }

    private TimeValue electionMinRetryInterval;
    private TimeValue electionMaxRetryInterval;
    private final Object mutex = new Object();
    private final ThreadPool threadPool;
    private final Random random;

    private long currentDelayMillis;

    ElectionScheduler(Settings settings, ClusterSettings clusterSettings, Random random, ThreadPool threadPool) {
        super(settings);

        this.random = random;
        this.threadPool = threadPool;

        synchronized (mutex) {
            electionMinRetryInterval = ELECTION_MIN_RETRY_INTERVAL_SETTING.get(settings);
            electionMaxRetryInterval = ELECTION_MAX_RETRY_INTERVAL_SETTING.get(settings);
            currentDelayMillis = electionMinRetryInterval.millis();
        }

        clusterSettings.addSettingsUpdateConsumer(ELECTION_MIN_RETRY_INTERVAL_SETTING, this::setElectionMinRetryInterval);
        clusterSettings.addSettingsUpdateConsumer(ELECTION_MAX_RETRY_INTERVAL_SETTING, this::setElectionMaxRetryInterval);
    }

    private void setElectionMinRetryInterval(TimeValue electionMinRetryInterval) {
        synchronized (mutex) {
            this.electionMinRetryInterval = electionMinRetryInterval;
        }
    }

    private void setElectionMaxRetryInterval(TimeValue electionMaxRetryInterval) {
        synchronized (mutex) {
            this.electionMaxRetryInterval = electionMaxRetryInterval;
        }
    }

    @Override
    protected void doStart() {
        scheduleNextWakeUp();
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
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

    private void scheduleNextWakeUp() {
        final long delay;
        synchronized (mutex) {
            currentDelayMillis = Math.min(electionMaxRetryInterval.getMillis(), currentDelayMillis + electionMinRetryInterval.getMillis());
            delay = randomLongBetween(electionMinRetryInterval.getMillis(), currentDelayMillis + 1);
        }
        logger.trace("scheduling next wake-up after [{}ms]", delay);
        threadPool.schedule(TimeValue.timeValueMillis(delay), Names.GENERIC, new AbstractLifecycleRunnable(lifecycle, logger) {
            @Override
            public void onFailure(Exception e) {
                logger.debug("unexpected exception in wakeup", e);
                assert false : e;
            }

            @Override
            protected void doRunInLifecycle() {
                logger.trace("election triggered");
                startElection();
            }

            @Override
            public void onAfterInLifecycle() {
                scheduleNextWakeUp();
            }

            @Override
            public String toString() {
                return "ElectionScheduler#scheduleNextWakeUp";
            }

            @Override
            public boolean isForceExecution() {
                // There are very few of these scheduled, and they back off, but it's important that they're not rejected as this could
                // prevent a cluster from ever forming.
                return true;
            }
        });
    }

    protected abstract void startElection();

    @Override
    public String toString() {
        return "ElectionScheduler{" +
            "electionMinRetryInterval=" + electionMinRetryInterval +
            ", electionMaxRetryInterval=" + electionMaxRetryInterval +
            ", currentDelayMillis=" + currentDelayMillis +
            '}';
    }
}

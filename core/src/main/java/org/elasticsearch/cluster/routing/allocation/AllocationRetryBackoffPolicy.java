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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Random;
import java.util.function.Function;

/**
 *
 * A policy controls when to retry allocating if shard allocation has failed.
 */
public abstract class AllocationRetryBackoffPolicy {
    public enum PolicyType {
        NO_BACKOFF(5) {
            @Override
            AllocationRetryBackoffPolicy policyForSettings(Settings settings) {
                return noBackOffPolicy();
            }
        },
        EXPONENTIAL_BACKOFF(1000) {
            @Override
            AllocationRetryBackoffPolicy policyForSettings(Settings settings) {
                return exponentialBackoffPolicy(settings);
            }
        };
        private final int defaultMaxRetries;

        abstract AllocationRetryBackoffPolicy policyForSettings(Settings settings);

        PolicyType(int defaultMaxRetries) {
            this.defaultMaxRetries = defaultMaxRetries;
        }

        public int getDefaultMaxRetries() {
            return defaultMaxRetries;
        }

        public static PolicyType fromString(String policyName) {
            if ("exponential_backoff".equals(policyName)) {
                return EXPONENTIAL_BACKOFF;
            } else if ("no_backoff".equals(policyName)) {
                return NO_BACKOFF;
            }
            throw new IllegalStateException("No backoff policy name match for [" + policyName + "]");
        }
    }

    public static final Setting<PolicyType> SETTING_ALLOCATION_RETRY_POLICY =
        new Setting<>("cluster.allocation.retry.policy", "exponential_backoff", PolicyType::fromString, Setting.Property.NodeScope);

    public static final Function<Settings, Integer> SETTING_ALLOCATION_DEFAULT_MAX_RETRIES =
        settings -> SETTING_ALLOCATION_RETRY_POLICY.get(settings).defaultMaxRetries;

    public static final Setting<TimeValue> SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_BASE_DELAY =
        Setting.positiveTimeSetting("cluster.allocation.retry.exponential_backoff.base_delay",
            TimeValue.timeValueMillis(50), Setting.Property.NodeScope);

    public static final Setting<TimeValue> SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_MAX_DELAY =
        Setting.positiveTimeSetting("cluster.allocation.retry.exponential_backoff.max_delay",
            TimeValue.timeValueMinutes(30), Setting.Property.NodeScope);

    /**
     * Determines a delay interval after a shard allocation has failed numOfFailures times.
     * This method may produce different value for each call.
     */
    public abstract TimeValue delayInterval(int numOfFailures);

    /**
     * Constructs the allocation retry policy for the given settings.
     */
    public static AllocationRetryBackoffPolicy policyForSettings(Settings settings) {
        return SETTING_ALLOCATION_RETRY_POLICY.get(settings).policyForSettings(settings);
    }

    public static AllocationRetryBackoffPolicy exponentialBackoffPolicy(Settings settings) {
        return new ExponentialBackOffPolicy(settings);
    }

    public static AllocationRetryBackoffPolicy noBackOffPolicy() {
        return new NoBackoffPolicy();
    }

    static class ExponentialBackOffPolicy extends AllocationRetryBackoffPolicy {
        private final Random random;
        private final long delayUnitMS;
        private final long maxDelayMS;

        ExponentialBackOffPolicy(Settings settings) {
            this.random = new Random(Randomness.get().nextInt());
            this.delayUnitMS = SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_BASE_DELAY.get(settings).millis();
            this.maxDelayMS = SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_MAX_DELAY.get(settings).millis();
        }

        @Override
        public TimeValue delayInterval(int numOfFailures) {
            assert numOfFailures >= 0;
            int bound = numOfFailures > 30 ? Integer.MAX_VALUE : 1 << numOfFailures;
            return TimeValue.timeValueMillis(Math.min(maxDelayMS, delayUnitMS * random.nextInt(bound)));
        }
    }

    static class NoBackoffPolicy extends AllocationRetryBackoffPolicy {
        @Override
        public TimeValue delayInterval(int numOfFailures) {
            return TimeValue.ZERO;
        }
    }
}

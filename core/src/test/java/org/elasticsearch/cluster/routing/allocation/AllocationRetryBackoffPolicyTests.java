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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AllocationRetryBackoffPolicyTests extends ESTestCase {

    public void testNoBackOff() throws Exception {
        Settings noBackOffSettings = Settings.builder().put("cluster.allocation.retry.policy", "no_backoff").build();
        AllocationRetryBackoffPolicy backoffPolicy = AllocationRetryBackoffPolicy.policyForSettings(noBackOffSettings);
        assertThat(backoffPolicy.delayInterval(randomIntBetween(0, 1000)), equalTo(TimeValue.ZERO));
    }

    public void testExponentialBackoff() throws Exception {
        Settings defaultSettings = Settings.EMPTY;
        TimeValue baseInterval = AllocationRetryBackoffPolicy.SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_BASE_DELAY.get(defaultSettings);
        TimeValue maxInterval = AllocationRetryBackoffPolicy.SETTING_ALLOCATION_RETRY_EXPONENTIAL_BACKOFF_MAX_DELAY.get(defaultSettings);
        assertThat(baseInterval, greaterThan(TimeValue.ZERO));
        assertThat(maxInterval, greaterThanOrEqualTo(baseInterval));
        AllocationRetryBackoffPolicy backoffPolicy = AllocationRetryBackoffPolicy.exponentialBackoffPolicy(defaultSettings);
        int numOfFailures = randomIntBetween(10, 30);
        TimeValue longestDelay = TimeValue.ZERO;
        for (int n = 0; n < numOfFailures; n++) {
            TimeValue delayInterval = backoffPolicy.delayInterval(n);
            assertThat(delayInterval, lessThanOrEqualTo(maxInterval));
            if (longestDelay.compareTo(delayInterval) < 0) {
                longestDelay = delayInterval;
            }
            long times = delayInterval.millis() / baseInterval.millis();
            assertThat((double) times, lessThanOrEqualTo(Math.pow(2, n)));
        }
        assertThat(longestDelay, greaterThan(baseInterval));
    }
}

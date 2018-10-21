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

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class BackoffPolicyTests extends ESTestCase {
    public void testWrapBackoffPolicy() {
        TimeValue timeValue = timeValueMillis(between(0, Integer.MAX_VALUE));
        int maxNumberOfRetries = between(1, 1000);
        BackoffPolicy policy = BackoffPolicy.constantBackoff(timeValue, maxNumberOfRetries);
        AtomicInteger retries = new AtomicInteger();
        policy = BackoffPolicy.wrap(policy, retries::getAndIncrement);

        int expectedRetries = 0;
        {
            // Fetching the iterator doesn't call the callback
            Iterator<TimeValue> itr = policy.iterator();
            assertEquals(expectedRetries, retries.get());

            while (itr.hasNext()) {
                // hasNext doesn't trigger the callback
                assertEquals(expectedRetries, retries.get());
                // next does
                itr.next();
                expectedRetries += 1;
                assertEquals(expectedRetries, retries.get());
            }
            // next doesn't call the callback when there isn't a backoff available
            expectThrows(NoSuchElementException.class, () -> itr.next());
            assertEquals(expectedRetries, retries.get());
        }
        {
            // The second iterator also calls the callback
            Iterator<TimeValue> itr = policy.iterator();
            itr.next();
            expectedRetries += 1;
            assertEquals(expectedRetries, retries.get());
        }
    }
}

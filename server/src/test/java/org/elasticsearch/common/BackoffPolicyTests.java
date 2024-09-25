/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.core.TimeValue.timeValueMillis;

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

    public void testExponentialBackOff() {
        long initialDelayMillis = randomLongBetween(0, 100);
        int maxNumberOfRetries = randomIntBetween(0, 10);
        BackoffPolicy exponentialBackoff = BackoffPolicy.exponentialBackoff(timeValueMillis(initialDelayMillis), maxNumberOfRetries);
        int numberOfBackoffsToPerform = randomIntBetween(1, 3);
        for (int i = 0; i < numberOfBackoffsToPerform; i++) {
            Iterator<TimeValue> iterator = exponentialBackoff.iterator();
            TimeValue lastTimeValue = null;
            int counter = 0;
            while (iterator.hasNext()) {
                TimeValue timeValue = iterator.next();
                if (lastTimeValue == null) {
                    assertEquals(timeValueMillis(initialDelayMillis), timeValue);
                } else {
                    // intervals should be always increasing
                    assertTrue(timeValue.compareTo(lastTimeValue) > 0);
                }
                lastTimeValue = timeValue;
                counter++;
            }
            assertEquals(maxNumberOfRetries, counter);
        }
    }

    public void testNoBackoff() {
        BackoffPolicy noBackoff = BackoffPolicy.noBackoff();
        int numberOfBackoffsToPerform = randomIntBetween(1, 3);
        for (int i = 0; i < numberOfBackoffsToPerform; i++) {
            Iterator<TimeValue> iterator = noBackoff.iterator();
            assertFalse(iterator.hasNext());
        }
    }

    public void testConstantBackoff() {
        long delayMillis = randomLongBetween(0, 100);
        int maxNumberOfRetries = randomIntBetween(0, 10);
        BackoffPolicy exponentialBackoff = BackoffPolicy.constantBackoff(timeValueMillis(delayMillis), maxNumberOfRetries);
        int numberOfBackoffsToPerform = randomIntBetween(1, 3);
        for (int i = 0; i < numberOfBackoffsToPerform; i++) {
            final Iterator<TimeValue> iterator = exponentialBackoff.iterator();
            int counter = 0;
            while (iterator.hasNext()) {
                TimeValue timeValue = iterator.next();
                assertEquals(timeValueMillis(delayMillis), timeValue);
                counter++;
            }
            assertEquals(maxNumberOfRetries, counter);
        }
    }
}

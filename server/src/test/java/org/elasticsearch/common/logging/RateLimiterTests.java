/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RateLimiterTests extends ESTestCase {

    /**
     * Check that the limiter will only execute a runnable once when
     * execute twice with the same key.
     */
    public void testOnlyRunsOnceForSameKey() {
        final RateLimiter rateLimiter = new RateLimiter();

        Runnable runnable = mock(Runnable.class);

        rateLimiter.limit("a key", runnable);
        rateLimiter.limit("a key", runnable);

        verify(runnable, times(1)).run();
    }

    /**
     * Check that the limiter will execute a runnable more than once when
     * executed with different keys.
     */
    public void testRunsMoreThanOnceForDifferentKeys() {
        final RateLimiter rateLimiter = new RateLimiter();

        Runnable runnable = mock(Runnable.class);

        rateLimiter.limit("a key", runnable);
        rateLimiter.limit("another key", runnable);

        verify(runnable, times(2)).run();
    }

    /**
     * Check that the limiter will execute a runnable again for a given key if that
     * key is evicted from the key cache.
     */
    public void testKeyCacheEntryEventuallyExpires() {
        final RateLimiter rateLimiter = new RateLimiter();

        Runnable runnable = mock(Runnable.class);
        Runnable otherRunnable = mock(Runnable.class);

        rateLimiter.limit("key0", runnable);

        // Fill the key cache so that "key0" is evicted
        for (int i = 1; i <= 128; i++) {
            rateLimiter.limit("key" + i, otherRunnable);
        }

        rateLimiter.limit("key0", runnable);

        verify(runnable, times(2)).run();
    }
}

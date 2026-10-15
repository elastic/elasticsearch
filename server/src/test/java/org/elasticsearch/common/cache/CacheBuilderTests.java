/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;

public class CacheBuilderTests extends ESTestCase {

    public void testSettingExpireAfterAccess() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> CacheBuilder.builder().setExpireAfterAccess(TimeValue.MINUS_ONE)
        );
        assertThat(iae.getMessage(), containsString("expireAfterAccess <="));
        iae = expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterAccess(TimeValue.ZERO));
        assertThat(iae.getMessage(), containsString("expireAfterAccess <="));
        final TimeValue timeValue = randomPositiveTimeValue();
        Cache<Object, Object> cache = CacheBuilder.builder().setExpireAfterAccess(timeValue).build();
        assertEquals(timeValue.getNanos(), cache.getExpireAfterAccessNanos());
    }

    public void testSettingExpireAfterWrite() {
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> CacheBuilder.builder().setExpireAfterWrite(TimeValue.MINUS_ONE)
        );
        assertThat(iae.getMessage(), containsString("expireAfterWrite <="));
        iae = expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterWrite(TimeValue.ZERO));
        assertThat(iae.getMessage(), containsString("expireAfterWrite <="));
        final TimeValue timeValue = randomPositiveTimeValue();
        Cache<Object, Object> cache = CacheBuilder.builder().setExpireAfterWrite(timeValue).build();
        assertEquals(timeValue.getNanos(), cache.getExpireAfterWriteNanos());
    }

    public void testSettingRelativeTimeProvider() {
        expectThrows(NullPointerException.class, () -> CacheBuilder.builder().setRelativeTimeProvider(null));

        final TimeValue expireAfterAccess = TimeValue.timeValueMinutes(5);
        final AtomicLong relativeTimeNanos = new AtomicLong();
        Cache<String, String> cache = CacheBuilder.<String, String>builder()
            .setExpireAfterAccess(expireAfterAccess)
            .setRelativeTimeProvider(relativeTimeNanos::get)
            .build();

        cache.put("key", "value");
        relativeTimeNanos.addAndGet(expireAfterAccess.getNanos());
        assertEquals("the entry is served while the supplied time stays within the TTL", "value", cache.get("key"));

        // The read above renewed the entry at that instant, so expiry is a further TTL away from there.
        relativeTimeNanos.addAndGet(expireAfterAccess.getNanos() + 1);
        assertNull("the cache expires against the supplied time rather than the wall clock", cache.get("key"));
    }
}

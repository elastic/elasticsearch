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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

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
        assertThat("Type is LRUCache as test depends on it", cache, instanceOf(LRUCache.class));
        assertEquals(timeValue.getNanos(), ((LRUCache<?, ?>) cache).getExpireAfterAccessNanos());
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
        assertThat("Type is LRUCache as test depends on it", cache, instanceOf(LRUCache.class));
        assertEquals(timeValue.getNanos(), ((LRUCache<?, ?>) cache).getExpireAfterWriteNanos());
    }
}

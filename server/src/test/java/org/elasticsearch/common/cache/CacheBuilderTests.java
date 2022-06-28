/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

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
        final TimeValue timeValue = TimeValue.parseTimeValue(randomPositiveTimeValue(), "");
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
        final TimeValue timeValue = TimeValue.parseTimeValue(randomPositiveTimeValue(), "");
        Cache<Object, Object> cache = CacheBuilder.builder().setExpireAfterWrite(timeValue).build();
        assertEquals(timeValue.getNanos(), cache.getExpireAfterWriteNanos());
    }
}

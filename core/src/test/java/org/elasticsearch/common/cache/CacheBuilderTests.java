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

package org.elasticsearch.common.cache;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class CacheBuilderTests extends ESTestCase {

    public void testSettingExpireAfterAccess() {
        IllegalArgumentException iae =
            expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterAccess(TimeValue.MINUS_ONE));
        assertThat(iae.getMessage(), containsString("expireAfterAccess <="));
        iae = expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterAccess(TimeValue.ZERO));
        assertThat(iae.getMessage(), containsString("expireAfterAccess <="));
        final TimeValue timeValue = TimeValue.parseTimeValue(randomPositiveTimeValue(), "");
        Cache<Object, Object> cache = CacheBuilder.builder().setExpireAfterAccess(timeValue).build();
        assertEquals(timeValue.getNanos(), cache.getExpireAfterAccessNanos());
    }

    public void testSettingExpireAfterWrite() {
        IllegalArgumentException iae =
            expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterWrite(TimeValue.MINUS_ONE));
        assertThat(iae.getMessage(), containsString("expireAfterWrite <="));
        iae = expectThrows(IllegalArgumentException.class, () -> CacheBuilder.builder().setExpireAfterWrite(TimeValue.ZERO));
        assertThat(iae.getMessage(), containsString("expireAfterWrite <="));
        final TimeValue timeValue = TimeValue.parseTimeValue(randomPositiveTimeValue(), "");
        Cache<Object, Object> cache = CacheBuilder.builder().setExpireAfterWrite(timeValue).build();
        assertEquals(timeValue.getNanos(), cache.getExpireAfterWriteNanos());
    }
}

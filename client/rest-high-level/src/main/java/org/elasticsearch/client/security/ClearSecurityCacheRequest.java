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

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;

import java.util.Arrays;
import java.util.Objects;

/**
 * The request used to clear the specified cache.
 */
public final class ClearSecurityCacheRequest implements Validatable {

    private final String cacheName;
    private final String[] keys;

    /**
     * @param cacheName Name of the cache
     * @param keys      An array of keys to be cleared from the specified cache.
     *                  If not specified, all entries will be cleared for the given cache.
     */
    public ClearSecurityCacheRequest(String cacheName, String... keys) {
        this.cacheName = cacheName;
        this.keys = keys;
    }

    /**
     * @return The name of the target cache to be invalidated
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return an array of key names that will be evicted
     */
    public String[] keys() {
        return keys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ClearSecurityCacheRequest that = (ClearSecurityCacheRequest) o;
        return cacheName.equals(that.cacheName) && Arrays.equals(keys, that.keys);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(cacheName);
        result = 31 * result + Arrays.hashCode(keys);
        return result;
    }
}

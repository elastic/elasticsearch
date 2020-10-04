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

/**
 * The request used to clear the specified cache.
 */
public final class ClearApiKeyCacheRequest extends ClearSecurityCacheRequest {

    private final String[] keys;

    /**
     * @param keys      An array of keys to be cleared from the specified cache.
     *                  If not specified, all entries will be cleared for the given cache.
     */
    public ClearApiKeyCacheRequest(String... keys) {
        this.keys = keys;
    }

    /**
     * @return The name of the target cache to be invalidated
     */
    public String cacheName() {
        return "api_key";
    }

    /**
     * @return an array of key names that will be evicted
     */
    public String[] keys() {
        return keys;
    }
}

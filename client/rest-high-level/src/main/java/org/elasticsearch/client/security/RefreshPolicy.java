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
 * Enumeration of values that control the refresh policy for a request that
 * supports specifying a refresh policy.
 */
public enum RefreshPolicy {

    /**
     * Don't refresh after this request. The default.
     */
    NONE("false"),
    /**
     * Force a refresh as part of this request. This refresh policy does not scale for high indexing or search throughput but is useful
     * to present a consistent view to for indices with very low traffic. And it is wonderful for tests!
     */
    IMMEDIATE("true"),
    /**
     * Leave this request open until a refresh has made the contents of this request visible to search. This refresh policy is
     * compatible with high indexing and search throughput but it causes the request to wait to reply until a refresh occurs.
     */
    WAIT_UNTIL("wait_for");

    private final String value;

    RefreshPolicy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Get the default refresh policy, which is <code>NONE</code>
     */
    public static RefreshPolicy getDefault() {
        return RefreshPolicy.NONE;
    }
}

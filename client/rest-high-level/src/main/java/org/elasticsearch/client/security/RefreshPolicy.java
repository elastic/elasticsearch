/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

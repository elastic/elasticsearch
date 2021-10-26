/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

public interface WriteRequestBuilder<B extends WriteRequestBuilder<B>> {
    WriteRequest<?> request();

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     */
    @SuppressWarnings("unchecked")
    default B setRefreshPolicy(RefreshPolicy refreshPolicy) {
        request().setRefreshPolicy(refreshPolicy);
        return (B) this;
    }

    /**
     * Parse the refresh policy from a string, only modifying it if the string is non null. Convenient to use with request parsing.
     */
    @SuppressWarnings("unchecked")
    default B setRefreshPolicy(String refreshPolicy) {
        request().setRefreshPolicy(refreshPolicy);
        return (B) this;
    }
}

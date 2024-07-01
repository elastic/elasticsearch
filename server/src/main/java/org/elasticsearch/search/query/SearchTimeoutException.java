/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
 * Specific instance of {@link SearchException} that indicates that a search timeout occurred.
 * Always returns http status 504 (Gateway Timeout)
 */
public class SearchTimeoutException extends SearchException {
    public SearchTimeoutException(SearchShardTarget shardTarget, String msg) {
        super(shardTarget, msg);
    }

    public SearchTimeoutException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.GATEWAY_TIMEOUT;
    }
}

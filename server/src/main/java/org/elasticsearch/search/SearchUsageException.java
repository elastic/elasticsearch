/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A subclass of SearchException that indicates a user error when constructing a search
 * request. These will always return HTTP status code 400 (BAD REQUEST).
 */
public class SearchUsageException extends SearchException {

    public SearchUsageException(SearchShardTarget shardTarget, String msg) {
        super(shardTarget, msg, null);
    }

    public SearchUsageException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}

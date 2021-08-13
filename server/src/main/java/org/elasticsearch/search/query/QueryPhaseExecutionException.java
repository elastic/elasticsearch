/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

public class QueryPhaseExecutionException extends SearchException {

    public QueryPhaseExecutionException(SearchShardTarget shardTarget, String msg, Throwable cause) {
        super(shardTarget, "Query Failed [" + msg + "]", cause);
    }

    public QueryPhaseExecutionException(StreamInput in) throws IOException {
        super(in);
    }

    public QueryPhaseExecutionException(SearchShardTarget shardTarget, String msg) {
        super(shardTarget, msg);
    }
}

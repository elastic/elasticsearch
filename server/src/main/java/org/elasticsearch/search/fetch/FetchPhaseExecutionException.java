/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

public class FetchPhaseExecutionException extends SearchException {

    public FetchPhaseExecutionException(SearchShardTarget shardTarget, String msg, Throwable t) {
        super(shardTarget, "Fetch Failed [" + msg + "]", t);
    }

    public FetchPhaseExecutionException(StreamInput in) throws IOException {
        super(in);
    }
}

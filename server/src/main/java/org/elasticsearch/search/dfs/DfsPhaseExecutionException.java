/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

public class DfsPhaseExecutionException extends SearchException {

    public DfsPhaseExecutionException(SearchShardTarget shardTarget, String msg, Throwable t) {
        super(shardTarget, "Dfs Failed [" + msg + "]", t);
    }

    public DfsPhaseExecutionException(SearchShardTarget shardTarget, String msg) {
        super(shardTarget, "Dfs Failed [" + msg + "]");
    }

    public DfsPhaseExecutionException(StreamInput in) throws IOException {
        super(in);
    }
}

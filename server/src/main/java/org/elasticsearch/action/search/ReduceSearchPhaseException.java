/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * A failure during a reduce phase (when receiving results from several shards, and reducing them
 * into one or more results and possible actions).
 *
 *
 */
public class ReduceSearchPhaseException extends SearchPhaseExecutionException {

    public ReduceSearchPhaseException(String phaseName, String msg, Throwable cause, ShardSearchFailure[] shardFailures) {
        super(phaseName, "[reduce] " + msg, cause, shardFailures);
    }

    public ReduceSearchPhaseException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        final ShardSearchFailure[] shardFailures = shardFailures();
        if (shardFailures.length == 0) {
            return getCause() == null ? RestStatus.INTERNAL_SERVER_ERROR : ExceptionsHelper.status(getCause());
        }
        return super.status();
    }
}

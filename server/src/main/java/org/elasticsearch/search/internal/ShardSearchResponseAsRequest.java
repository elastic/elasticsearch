/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ShardSearchResponseAsRequest extends TransportRequest {
    private final QuerySearchResult result;
    private final Exception error;
    private final ShardId shardId;

    public ShardSearchResponseAsRequest(StreamInput in) throws IOException {
        super(in);
        result = in.readOptionalWriteable(QuerySearchResult::new);
        if (result == null) {
            error = in.readException();
            shardId = new ShardId(in);
        } else {
            error = null;
            shardId = result.getSearchShardTarget().getShardId();
        }
    }

    public ShardSearchResponseAsRequest(QuerySearchResult result) {
        this.result = result;
        error = null;
        shardId = result.getSearchShardTarget().getShardId();
    }

    public ShardSearchResponseAsRequest(Exception error, ShardId shardId) {
        result = null;
        this.error = error;
        this.shardId = shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (result == null) {
            out.writeException(error);
            out.writeWriteable(shardId);
        } else {
            out.writeOptionalWriteable(result);
        }
    }

    public SearchPhaseResult getResult() {
        return result;
    }

    public Exception getError() {
        return error;
    }

    public ShardId getShardId() {
        return shardId;
    }

}

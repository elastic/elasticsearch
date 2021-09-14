/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal validate response of a shard validate request executed directly against a specific shard.
 *
 *
 */
class ShardValidateQueryResponse extends BroadcastShardResponse {

    private boolean valid;

    private String explanation;

    private String error;

    ShardValidateQueryResponse(StreamInput in) throws IOException {
        super(in);
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    ShardValidateQueryResponse(ShardId shardId, boolean valid, String explanation, String error) {
        super(shardId);
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public boolean isValid() {
        return this.valid;
    }

    public String getExplanation() {
        return explanation;
    }

    public String getError() {
        return error;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeOptionalString(explanation);
        out.writeOptionalString(error);
    }
}

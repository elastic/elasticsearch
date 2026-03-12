/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public abstract class ChunkedBroadcastResponse extends BaseBroadcastResponse implements ChunkedToXContentObject {
    public ChunkedBroadcastResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ChunkedBroadcastResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    public final Iterator<ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((b, p) -> {
            b.startObject();
            RestActions.buildBroadcastShardsHeader(b, p, this);
            return b;
        }), customXContentChunks(params), ChunkedToXContentHelper.endObject());
    }

    protected abstract Iterator<ToXContent> customXContentChunks(ToXContent.Params params);
}

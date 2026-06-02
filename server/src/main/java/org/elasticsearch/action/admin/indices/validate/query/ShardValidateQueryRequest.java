/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal validate request executed directly against a specific index shard.
 */
public class ShardValidateQueryRequest extends BroadcastShardRequest {

    private final QueryBuilder query;
    private final boolean explain;
    private final boolean rewrite;
    private final long nowInMillis;
    private final AliasFilter filteringAliases;
    @Nullable
    private final String sliceRouting;

    public ShardValidateQueryRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readNamedWriteable(QueryBuilder.class);
        filteringAliases = AliasFilter.readFrom(in);
        explain = in.readBoolean();
        rewrite = in.readBoolean();
        nowInMillis = in.readVLong();
        if (in.getTransportVersion().supports(SliceIndexing.VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION)) {
            sliceRouting = in.readOptionalString();
        } else {
            sliceRouting = null;
        }
    }

    public ShardValidateQueryRequest(ShardId shardId, AliasFilter filteringAliases, ValidateQueryRequest request) {
        super(shardId, request);
        this.query = request.query();
        this.explain = request.explain();
        this.rewrite = request.rewrite();
        this.filteringAliases = Objects.requireNonNull(filteringAliases, "filteringAliases must not be null");
        this.nowInMillis = request.nowInMillis;
        this.sliceRouting = request.isRoutingFromSlice() ? request.routing() : null;
    }

    public QueryBuilder query() {
        return query;
    }

    public boolean explain() {
        return this.explain;
    }

    public boolean rewrite() {
        return this.rewrite;
    }

    public AliasFilter filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    @Nullable
    public String sliceRouting() {
        return sliceRouting;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        filteringAliases.writeTo(out);
        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        out.writeVLong(nowInMillis);
        if (out.getTransportVersion().supports(SliceIndexing.VALIDATE_QUERY_SLICE_ROUTING_STATE_VERSION)) {
            out.writeOptionalString(sliceRouting);
        }
    }
}

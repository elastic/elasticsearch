/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A failure during paginated search. Like {@link ShardSearchFailure} but useful for reindex from remote as well.
 */
public class PaginatedSearchFailure implements Writeable, ToXContentObject {
    private final Throwable reason;
    private final RestStatus status;
    @Nullable
    private final String index;
    @Nullable
    private final Integer shardId;
    @Nullable
    private final String nodeId;

    public static final String INDEX_FIELD = "index";
    public static final String SHARD_FIELD = "shard";
    public static final String NODE_FIELD = "node";
    public static final String REASON_FIELD = "reason";
    public static final String STATUS_FIELD = BulkItemResponse.Failure.STATUS_FIELD;

    public PaginatedSearchFailure(Throwable reason, @Nullable String index, @Nullable Integer shardId, @Nullable String nodeId) {
        this(reason, index, shardId, nodeId, ExceptionsHelper.status(reason));
    }

    public PaginatedSearchFailure(
        Throwable reason,
        @Nullable String index,
        @Nullable Integer shardId,
        @Nullable String nodeId,
        RestStatus status
    ) {
        this.index = index;
        this.shardId = shardId;
        this.reason = requireNonNull(reason, "reason cannot be null");
        this.nodeId = nodeId;
        this.status = status;
    }

    /**
     * Build a search failure that doesn't have shard information available.
     */
    public PaginatedSearchFailure(Throwable reason) {
        this(reason, null, null, null);
    }

    /**
     * Read from a stream.
     */
    public PaginatedSearchFailure(StreamInput in) throws IOException {
        reason = in.readException();
        index = in.readOptionalString();
        shardId = in.readOptionalVInt();
        nodeId = in.readOptionalString();
        status = ExceptionsHelper.status(reason);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeException(reason);
        out.writeOptionalString(index);
        out.writeOptionalVInt(shardId);
        out.writeOptionalString(nodeId);
    }

    public String getIndex() {
        return index;
    }

    public Integer getShardId() {
        return shardId;
    }

    public RestStatus getStatus() {
        return this.status;
    }

    public Throwable getReason() {
        return reason;
    }

    @Nullable
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (index != null) {
            builder.field(INDEX_FIELD, index);
        }
        if (shardId != null) {
            builder.field(SHARD_FIELD, shardId);
        }
        if (nodeId != null) {
            builder.field(NODE_FIELD, nodeId);
        }
        builder.field(STATUS_FIELD, status.getStatus());
        builder.field(REASON_FIELD);
        {
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, reason);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

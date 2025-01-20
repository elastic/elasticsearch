/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents a failure to search on a specific shard.
 */
public class ShardSearchFailure extends ShardOperationFailedException {

    public static final String REASON_FIELD = "reason";
    public static final String NODE_FIELD = "node";
    public static final String INDEX_FIELD = "index";
    public static final String SHARD_FIELD = "shard";

    public static final ShardSearchFailure[] EMPTY_ARRAY = new ShardSearchFailure[0];

    private SearchShardTarget shardTarget;

    ShardSearchFailure(StreamInput in) throws IOException {
        shardTarget = in.readOptionalWriteable(SearchShardTarget::new);
        if (shardTarget != null) {
            index = shardTarget.getFullyQualifiedIndexName();
            shardId = shardTarget.getShardId().getId();
        }
        reason = in.readString();
        status = RestStatus.readFrom(in);
        cause = in.readException();
    }

    public ShardSearchFailure(Exception e) {
        this(e, null);
    }

    public ShardSearchFailure(Exception e, @Nullable SearchShardTarget shardTarget) {
        super(
            shardTarget == null ? null : shardTarget.getFullyQualifiedIndexName(),
            shardTarget == null ? -1 : shardTarget.getShardId().getId(),
            ExceptionsHelper.stackTrace(e),
            ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)),
            ExceptionsHelper.unwrapCause(e)
        );

        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        if (actual instanceof SearchException) {
            this.shardTarget = ((SearchException) actual).shard();
        } else if (shardTarget != null) {
            this.shardTarget = shardTarget;
        }
    }

    /**
     * The search shard target the failure occurred on.
     * @return The shardTarget, may be null
     */
    @Nullable
    public SearchShardTarget shard() {
        return this.shardTarget;
    }

    @Override
    public String toString() {
        return "shard ["
            + (shardTarget == null ? "_na" : shardTarget)
            + "], reason ["
            + reason
            + "], cause ["
            + (cause == null ? "_na" : ExceptionsHelper.stackTrace(cause))
            + "]";
    }

    public static ShardSearchFailure readShardSearchFailure(StreamInput in) throws IOException {
        return new ShardSearchFailure(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(shardTarget);
        out.writeString(reason);
        RestStatus.writeTo(out, status);
        out.writeException(cause);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(SHARD_FIELD, shardId());
            builder.field(INDEX_FIELD, index());
            if (shardTarget != null) {
                builder.field(NODE_FIELD, shardTarget.getNodeId());
            }
            builder.field(REASON_FIELD);
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, params, cause);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

}

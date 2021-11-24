/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Represents a failure to search on a specific shard.
 */
public class ShardSearchFailure extends ShardOperationFailedException {

    private static final String REASON_FIELD = "reason";
    private static final String NODE_FIELD = "node";
    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";

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

    public static ShardSearchFailure fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String currentFieldName = null;
        int shardId = -1;
        String indexName = null;
        String clusterAlias = null;
        String nodeId = null;
        ElasticsearchException exception = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SHARD_FIELD.equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if (INDEX_FIELD.equals(currentFieldName)) {
                    indexName = parser.text();
                    int indexOf = indexName.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                    if (indexOf > 0) {
                        clusterAlias = indexName.substring(0, indexOf);
                        indexName = indexName.substring(indexOf + 1);
                    }
                } else if (NODE_FIELD.equals(currentFieldName)) {
                    nodeId = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (REASON_FIELD.equals(currentFieldName)) {
                    exception = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchShardTarget searchShardTarget = null;
        if (nodeId != null) {
            searchShardTarget = new SearchShardTarget(
                nodeId,
                new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
                clusterAlias
            );
        }
        return new ShardSearchFailure(exception, searchShardTarget);
    }
}

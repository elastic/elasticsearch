/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

/**
 * Represents a failure to search on a specific shard.
 */
public class ShardSearchFailure implements ShardOperationFailedException {

    public static final ShardSearchFailure[] EMPTY_ARRAY = new ShardSearchFailure[0];

    private SearchShardTarget shardTarget;
    private String reason;
    private RestStatus status;
    private Throwable cause;

    private ShardSearchFailure() {

    }

    public ShardSearchFailure(Throwable t) {
        this(t, null);
    }

    public ShardSearchFailure(Throwable t, @Nullable SearchShardTarget shardTarget) {
        Throwable actual = ExceptionsHelper.unwrapCause(t);
        if (actual != null && actual instanceof SearchException) {
            this.shardTarget = ((SearchException) actual).shard();
        } else if (shardTarget != null) {
            this.shardTarget = shardTarget;
        }
        status = ExceptionsHelper.status(actual);
        this.reason = ExceptionsHelper.detailedMessage(t);
        this.cause = actual;
    }

    public ShardSearchFailure(String reason, SearchShardTarget shardTarget) {
        this(reason, shardTarget, RestStatus.INTERNAL_SERVER_ERROR);
    }

    public ShardSearchFailure(String reason, SearchShardTarget shardTarget, RestStatus status) {
        this.shardTarget = shardTarget;
        this.reason = reason;
        this.status = status;
    }

    /**
     * The search shard target the failure occurred on.
     */
    @Nullable
    public SearchShardTarget shard() {
        return this.shardTarget;
    }

    @Override
    public RestStatus status() {
        return this.status;
    }

    /**
     * The index the search failed on.
     */
    @Override
    public String index() {
        if (shardTarget != null) {
            return shardTarget.index();
        }
        return null;
    }

    /**
     * The shard id the search failed on.
     */
    @Override
    public int shardId() {
        if (shardTarget != null) {
            return shardTarget.shardId().id();
        }
        return -1;
    }

    /**
     * The reason of the failure.
     */
    @Override
    public String reason() {
        return this.reason;
    }

    @Override
    public String toString() {
        return "shard [" + (shardTarget == null ? "_na" : shardTarget) + "], reason [" + reason + "], cause [" + (cause == null ? "_na" : ExceptionsHelper.stackTrace(cause)) + "]";
    }

    public static ShardSearchFailure readShardSearchFailure(StreamInput in) throws IOException {
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure();
        shardSearchFailure.readFrom(in);
        return shardSearchFailure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            shardTarget = new SearchShardTarget(in);
        }
        reason = in.readString();
        status = RestStatus.readFrom(in);
        cause = in.readThrowable();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (shardTarget == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            shardTarget.writeTo(out);
        }
        out.writeString(reason);
        RestStatus.writeTo(out, status);
        out.writeThrowable(cause);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("shard", shardId());
        builder.field("index", index());
        if (shardTarget != null) {
            builder.field("node", shardTarget.nodeId());
        }
        if (cause != null) {
            builder.field("reason");
            builder.startObject();
            ElasticsearchException.toXContent(builder, params, cause);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }
}

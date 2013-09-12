/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;

import static org.elasticsearch.search.SearchShardTarget.readSearchShardTarget;

/**
 * Represents a failure to search on a specific shard.
 */
public class ShardSearchFailure implements ShardOperationFailedException {

    public static final ShardSearchFailure[] EMPTY_ARRAY = new ShardSearchFailure[0];

    private SearchShardTarget shardTarget;
    private String reason;
    private RestStatus status;
    private transient Throwable failure;

    private ShardSearchFailure() {

    }

    @Nullable
    public Throwable failure() {
        return failure;
    }

    public ShardSearchFailure(Throwable t) {
        this(t, null);
    }

    public ShardSearchFailure(Throwable t, @Nullable SearchShardTarget shardTarget) {
        this.failure = t;
        Throwable actual = ExceptionsHelper.unwrapCause(t);
        if (actual != null && actual instanceof SearchException) {
            this.shardTarget = ((SearchException) actual).shard();
        } else if (shardTarget != null) {
            this.shardTarget = shardTarget;
        }
        if (actual != null && actual instanceof ElasticSearchException) {
            status = ((ElasticSearchException) actual).status();
        } else {
            status = RestStatus.INTERNAL_SERVER_ERROR;
        }
        this.reason = ExceptionsHelper.detailedMessage(t);
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
            return shardTarget.shardId();
        }
        return -1;
    }

    /**
     * The reason of the failure.
     */
    public String reason() {
        return this.reason;
    }

    @Override
    public String toString() {
        return "shard [" + (shardTarget == null ? "_na" : shardTarget) + "], reason [" + reason + "]";
    }

    public static ShardSearchFailure readShardSearchFailure(StreamInput in) throws IOException {
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure();
        shardSearchFailure.readFrom(in);
        return shardSearchFailure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            shardTarget = readSearchShardTarget(in);
        }
        reason = in.readString();
        status = RestStatus.readFrom(in);
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
    }
}

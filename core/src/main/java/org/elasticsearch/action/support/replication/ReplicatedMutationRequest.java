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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Base class for requests that modify data in some shard like delete, index, and shardBulk.
 */
public class ReplicatedMutationRequest<R extends ReplicatedMutationRequest<R>> extends ReplicationRequest<R> {
    private boolean refresh;
    private boolean blockUntilRefresh;

    /**
     * Create an empty request.
     */
    public ReplicatedMutationRequest() {
    }

    /**
     * Creates a new request with resolved shard id.
     */
    public ReplicatedMutationRequest(ShardId shardId) {
        super(shardId);
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    @SuppressWarnings("unchecked")
    public R setRefresh(boolean refresh) {
        this.refresh = refresh;
        return (R) this;
    }

    public boolean isRefresh() {
        return this.refresh;
    }

    /**
     * Should this request block until it has been made visible for search by a refresh? Unlike {@link #setRefresh(boolean)} this is quite safe
     * to use under heavy indexing so long as few total operations use it. See {@link IndexSettings#MAX_REFRESH_LISTENERS_PER_SHARD} for
     * the limit. A bulk request counts as one request on each shard that it touches.
     */
    @SuppressWarnings("unchecked")
    public R setBlockUntilRefresh(boolean blockUntilRefresh) {
        this.blockUntilRefresh = blockUntilRefresh;
        return (R) this;
    }

    public boolean shouldBlockUntilRefresh() {
        return blockUntilRefresh;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        refresh = in.readBoolean();
        blockUntilRefresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(refresh);
        out.writeBoolean(blockUntilRefresh);
    }
}

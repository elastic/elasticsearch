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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A flush request to flush one or more indices. The flush process of an index basically frees memory from the index
 * by flushing data to the index storage and clearing the internal transaction log. By default, ElasticSearch uses
 * memory heuristics in order to automatically trigger flush operations as required in order to clear memory.
 * <p/>
 * <p>Best created with {@link org.elasticsearch.client.Requests#flushRequest(String...)}.
 *
 *
 * @see org.elasticsearch.client.Requests#flushRequest(String...)
 * @see org.elasticsearch.client.IndicesAdminClient#flush(FlushRequest)
 * @see FlushResponse
 */
public class FlushRequest extends BroadcastOperationRequest {

    private boolean refresh = false;

    private boolean force = false;

    private boolean full = false;

    FlushRequest() {

    }

    /**
     * Constructs a new flush request against one or more indices. If nothing is provided, all indices will
     * be flushed.
     */
    public FlushRequest(String... indices) {
        super(indices);
        // we want to do the refresh in parallel on local shards...
        operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD);
    }

    /**
     * Should a refresh be performed once the flush is done. Defaults to <tt>false</tt>.
     */
    public boolean refresh() {
        return this.refresh;
    }

    /**
     * Should a refresh be performed once the flush is done. Defaults to <tt>false</tt>.
     */
    public FlushRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    /**
     * Should a "full" flush be performed.
     */
    public boolean full() {
        return this.full;
    }

    /**
     * Should a "full" flush be performed.
     */
    public FlushRequest full(boolean full) {
        this.full = full;
        return this;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public boolean force() {
        return force;
    }

    /**
     * Force flushing, even if one is possibly not needed.
     */
    public FlushRequest force(boolean force) {
        this.force = force;
        return this;
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override
    public FlushRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    @Override
    public FlushRequest operationThreading(BroadcastOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(refresh);
        out.writeBoolean(full);
        out.writeBoolean(force);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        refresh = in.readBoolean();
        full = in.readBoolean();
        force = in.readBoolean();
    }
}

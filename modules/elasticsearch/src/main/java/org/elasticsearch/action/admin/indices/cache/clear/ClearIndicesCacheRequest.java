/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.action.support.broadcast.BroadcastOperationThreading;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ClearIndicesCacheRequest extends BroadcastOperationRequest {

    private boolean filterCache = true;

    ClearIndicesCacheRequest() {
    }

    public ClearIndicesCacheRequest(String... indices) {
        super(indices, null);
        // we want to do the refresh in parallel on local shards...
        operationThreading(BroadcastOperationThreading.THREAD_PER_SHARD);
    }

    /**
     * Should the listener be called on a separate thread if needed.
     */
    @Override public ClearIndicesCacheRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    /**
     * Controls the operation threading model.
     */
    @Override public ClearIndicesCacheRequest operationThreading(BroadcastOperationThreading operationThreading) {
        super.operationThreading(operationThreading);
        return this;
    }

    public boolean filterCache() {
        return filterCache;
    }

    /**
     * Should the filter cache be cleared or not. Defaults to <tt>true</tt>.
     */
    public ClearIndicesCacheRequest filterCache(boolean filterCache) {
        this.filterCache = filterCache;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        filterCache = in.readBoolean();
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(filterCache);
    }
}
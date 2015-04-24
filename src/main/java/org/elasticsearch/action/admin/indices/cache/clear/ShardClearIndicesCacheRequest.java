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

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 *
 */
class ShardClearIndicesCacheRequest extends BroadcastShardOperationRequest {

    private boolean filterCache = false;
    private boolean fieldDataCache = false;
    private boolean idCache = false;
    private boolean recycler;
    private boolean queryCache = false;

    private String[] fields = null;
    private String[] filterKeys = null;

    ShardClearIndicesCacheRequest() {
    }

    ShardClearIndicesCacheRequest(ShardId shardId, ClearIndicesCacheRequest request) {
        super(shardId, request);
        filterCache = request.filterCache();
        fieldDataCache = request.fieldDataCache();
        idCache = request.idCache();
        fields = request.fields();
        filterKeys = request.filterKeys();
        recycler = request.recycler();
        queryCache = request.queryCache();
    }

    public boolean filterCache() {
        return filterCache;
    }

    public boolean queryCache() {
        return queryCache;
    }

    public boolean fieldDataCache() {
        return this.fieldDataCache;
    }

    public boolean idCache() {
        return this.idCache;
    }
    
    public boolean recycler() {
        return this.recycler;
    }

    public String[] fields() {
        return this.fields;
    }

    public String[] filterKeys() {
        return this.filterKeys;
    }

    public ShardClearIndicesCacheRequest waitForOperations(boolean waitForOperations) {
        this.filterCache = waitForOperations;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        filterCache = in.readBoolean();
        fieldDataCache = in.readBoolean();
        idCache = in.readBoolean();
        recycler = in.readBoolean();
        fields = in.readStringArray();
        filterKeys = in.readStringArray();
        queryCache = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(filterCache);
        out.writeBoolean(fieldDataCache);
        out.writeBoolean(idCache);
        out.writeBoolean(recycler);
        out.writeStringArrayNullable(fields);
        out.writeStringArrayNullable(filterKeys);
        out.writeBoolean(queryCache);
    }
}

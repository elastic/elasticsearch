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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete by query request to execute on a specific shard.
 */
public class ShardDeleteByQueryRequest extends ShardReplicationOperationRequest<ShardDeleteByQueryRequest> {

    private int shardId;
    private BytesReference source;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable
    private Set<String> routing;
    @Nullable
    private String[] filteringAliases;
    private long nowInMillis;

    private OriginalIndices originalIndices;

    ShardDeleteByQueryRequest(IndexDeleteByQueryRequest request, int shardId) {
        super(request);
        this.index = request.index();
        this.source = request.source();
        this.types = request.types();
        this.shardId = shardId;
        consistencyLevel(request.consistencyLevel());
        timeout = request.timeout();
        this.routing = request.routing();
        filteringAliases = request.filteringAliases();
        nowInMillis = request.nowInMillis();
        this.originalIndices = new OriginalIndices(request);
    }

    ShardDeleteByQueryRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    public int shardId() {
        return this.shardId;
    }

    BytesReference source() {
        return source;
    }

    public String[] types() {
        return this.types;
    }

    public Set<String> routing() {
        return this.routing;
    }

    public String[] filteringAliases() {
        return filteringAliases;
    }

    long nowInMillis() {
        return nowInMillis;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        shardId = in.readVInt();
        types = in.readStringArray();
        int routingSize = in.readVInt();
        if (routingSize > 0) {
            routing = new HashSet<>(routingSize);
            for (int i = 0; i < routingSize; i++) {
                routing.add(in.readString());
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readString();
            }
        }

        nowInMillis = in.readVLong();
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
        out.writeVInt(shardId);
        out.writeStringArray(types);
        if (routing != null) {
            out.writeVInt(routing.size());
            for (String r : routing) {
                out.writeString(r);
            }
        } else {
            out.writeVInt(0);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeString(alias);
            }
        } else {
            out.writeVInt(0);
        }
        out.writeVLong(nowInMillis);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "delete_by_query {[" + index + "]" + Arrays.toString(types) + ", query [" + sSource + "]}";
    }
}

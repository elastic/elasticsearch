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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Delete by query request to execute on a specific index.
 */
public class IndexDeleteByQueryRequest extends IndexReplicationOperationRequest<IndexDeleteByQueryRequest> {

    private BytesReference source;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable
    private Set<String> routing;
    @Nullable
    private String[] filteringAliases;
    private long nowInMillis;

    IndexDeleteByQueryRequest(DeleteByQueryRequest request, String index, @Nullable Set<String> routing, @Nullable String[] filteringAliases,
                              long nowInMillis
    ) {
        this.index = index;
        this.timeout = request.timeout();
        this.source = request.source();
        this.types = request.types();
        this.replicationType = request.replicationType();
        this.consistencyLevel = request.consistencyLevel();
        this.routing = routing;
        this.filteringAliases = filteringAliases;
        this.nowInMillis = nowInMillis;
    }

    IndexDeleteByQueryRequest() {
    }

    BytesReference source() {
        return source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        return validationException;
    }

    Set<String> routing() {
        return this.routing;
    }

    String[] types() {
        return this.types;
    }

    String[] filteringAliases() {
        return filteringAliases;
    }

    long nowInMillis() {
        return nowInMillis;
    }

    public IndexDeleteByQueryRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readBytesReference();
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
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
        if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
            nowInMillis = in.readVLong();
        } else {
            nowInMillis = System.currentTimeMillis();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(source);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
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
        if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
            out.writeVLong(nowInMillis);
        }
    }
}

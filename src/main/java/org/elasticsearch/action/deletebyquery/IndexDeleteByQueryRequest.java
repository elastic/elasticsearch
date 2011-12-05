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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Required;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.set.hash.THashSet;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.action.Actions.*;

/**
 * Delete by query request to execute on a specific index.
 *
 * @author kimchy (shay.banon)
 */
public class IndexDeleteByQueryRequest extends IndexReplicationOperationRequest {

    private byte[] querySource;
    private String[] types = Strings.EMPTY_ARRAY;
    @Nullable private Set<String> routing;
    @Nullable private String[] filteringAliases;

    IndexDeleteByQueryRequest(DeleteByQueryRequest request, String index, @Nullable Set<String> routing, @Nullable String[] filteringAliases) {
        this.index = index;
        this.timeout = request.timeout();
        this.querySource = request.querySource();
        this.types = request.types();
        this.replicationType = request.replicationType();
        this.consistencyLevel = request.consistencyLevel();
        this.routing = routing;
        this.filteringAliases = filteringAliases;
    }

    IndexDeleteByQueryRequest() {
    }

    byte[] querySource() {
        return querySource;
    }

    @Override public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (querySource == null) {
            validationException = addValidationError("querySource is missing", validationException);
        }
        return validationException;
    }

    @Required public IndexDeleteByQueryRequest querySource(QueryBuilder queryBuilder) {
        return querySource(queryBuilder.buildAsBytes());
    }

    @Required public IndexDeleteByQueryRequest querySource(byte[] querySource) {
        this.querySource = querySource;
        return this;
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

    public IndexDeleteByQueryRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        querySource = new byte[in.readVInt()];
        in.readFully(querySource);
        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readUTF();
            }
        }
        int routingSize = in.readVInt();
        if (routingSize > 0) {
            routing = new THashSet<String>(routingSize);
            for (int i = 0; i < routingSize; i++) {
                routing.add(in.readUTF());
            }
        }
        int aliasesSize = in.readVInt();
        if (aliasesSize > 0) {
            filteringAliases = new String[aliasesSize];
            for (int i = 0; i < aliasesSize; i++) {
                filteringAliases[i] = in.readUTF();
            }
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(querySource.length);
        out.writeBytes(querySource);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeUTF(type);
        }
        if (routing != null) {
            out.writeVInt(routing.size());
            for (String r : routing) {
                out.writeUTF(r);
            }
        } else {
            out.writeVInt(0);
        }
        if (filteringAliases != null) {
            out.writeVInt(filteringAliases.length);
            for (String alias : filteringAliases) {
                out.writeUTF(alias);
            }
        } else {
            out.writeVInt(0);
        }
    }
}
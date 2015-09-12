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
package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.EnumSet;

/**
 * Request for {@link IndicesShardStoresAction}
 */
public class IndicesShardStoresRequest extends MasterNodeReadRequest<IndicesShardStoresRequest> implements IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpand();
    private EnumSet<ClusterHealthStatus> statuses = EnumSet.of(ClusterHealthStatus.YELLOW, ClusterHealthStatus.RED);

    /**
     * Create a request for shard stores info for <code>indices</code>
     */
    public IndicesShardStoresRequest(String... indices) {
        this.indices = indices;
    }

    public IndicesShardStoresRequest() {
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * see {@link ClusterHealthStatus} for details.
     * Defaults to "yellow" and "red" status
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     */
    public IndicesShardStoresRequest shardStatuses(String... shardStatuses) {
        statuses = EnumSet.noneOf(ClusterHealthStatus.class);
        for (String statusString : shardStatuses) {
            if ("all".equalsIgnoreCase(statusString)) {
                statuses = EnumSet.allOf(ClusterHealthStatus.class);
                return this;
            }
            statuses.add(ClusterHealthStatus.fromString(statusString));
        }
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardStoresRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets the indices for the shard stores request
     */
    @Override
    public IndicesShardStoresRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Returns the shard criteria to get store information on
     */
    public EnumSet<ClusterHealthStatus> shardStatuses() {
        return statuses;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeVInt(statuses.size());
        for (ClusterHealthStatus status : statuses) {
            out.writeByte(status.value());
        }
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        int nStatus = in.readVInt();
        statuses = EnumSet.noneOf(ClusterHealthStatus.class);
        for (int i = 0; i < nStatus; i++) {
            statuses.add(ClusterHealthStatus.fromValue(in.readByte()));
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }
}

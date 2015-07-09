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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Request for {@link IndicesShardsStoresAction}
 */
public class IndicesShardsStoresRequest extends MasterNodeReadRequest<IndicesShardsStoresRequest> implements IndicesRequest.Replaceable {

    /**
     * Status used to choose shards to get store information on
     */
    public enum Status {

        /**
         * Shards with all assigned copies
         */
        GREEN((byte) 0),

        /**
         * Shards with assigned primary copy
         * and at least one unassigned replica copy
         */
        YELLOW((byte) 1),

        /**
         * Shards with unassigned primary copy
         */
        RED((byte) 2),

        /**
         * All shards
         */
        ALL((byte) 3);

        private final byte id;

        Status(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Status fromId(byte id) {
            switch (id) {
                case 0:
                    return GREEN;
                case 1:
                    return YELLOW;
                case 2:
                    return RED;
                case 3:
                    return ALL;
                default:
                    throw new IllegalArgumentException("No status for id [" + id + "]");
            }
        }

        public static Status fromString(String status) {
            if (status.equalsIgnoreCase("green")) {
                return GREEN;
            } else if (status.equalsIgnoreCase("yellow")) {
                return YELLOW;
            } else if (status.equalsIgnoreCase("red")) {
                return RED;
            } else if (status.equalsIgnoreCase("all")) {
                return ALL;
            } else {
                throw new IllegalArgumentException("unknown status [" + status + "]");
            }
        }

        public static Status readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id());
        }
    }

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpand();
    private EnumSet<Status> statuses = EnumSet.of(Status.YELLOW, Status.RED);

    /**
     * Create a request for shards stores info for <code>indices</code>
     */
    public IndicesShardsStoresRequest(String... indices) {
        this.indices = indices;
    }

    IndicesShardsStoresRequest() {
    }

    /**
     * Set statuses to filter shards to get stores info on.
     * see {@link Status} for details.
     * Defaults to "yellow" and "red" status
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     */
    public IndicesShardsStoresRequest shardStatuses(String... shardStatuses) {
        statuses = EnumSet.noneOf(Status.class);
        for (String statusString : shardStatuses) {
            Status status = Status.fromString(statusString);
            if (status == Status.ALL) {
                statuses = EnumSet.of(Status.ALL);
                return this;
            }
            statuses.add(status);
        }
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions
     * By default, expands wildcards to both open and closed indices
     */
    public IndicesShardsStoresRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * Sets the indices for the shard stores request
     */
    @Override
    public IndicesShardsStoresRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Returns the shard criteria to get store information on
     */
    public EnumSet<Status> shardStatuses() {
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
        for (Status shardStatus : statuses) {
            shardStatus.writeTo(out);
        }
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        int nStatus = in.readVInt();
        statuses = EnumSet.noneOf(Status.class);
        for (int i = 0; i < nStatus; i++) {
            statuses.add(Status.readFrom(in));
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }
}

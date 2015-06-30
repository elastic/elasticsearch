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
import java.util.List;

public class IndicesShardsStoresRequest extends MasterNodeReadRequest<IndicesShardsStoresRequest> implements IndicesRequest.Replaceable {

    /**
     * Status used to choose shards to get store information on
     */
    public enum Status {

        /**
         * Status to get store information on all active shards
         */
        GREEN((byte) 0),

        /**
         * Status to get store information on shards with
         * at least one unassigned replica
         */
        YELLOW((byte) 1),

        /**
         * Status to get store information on shards with
         * unassigned primary or replica
         */
        RED((byte) 2),

        /**
         * Status to get store information on all shards
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
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();
    private Status[] shardStatuses = new Status[] { Status.YELLOW };

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
     * @param shardStatuses acceptable values are "green", "yellow", "red" and "all"
     * see {@link Status} for details
     */
    public IndicesShardsStoresRequest shardStatuses(String... shardStatuses) {
        this.shardStatuses = new Status[shardStatuses.length];
        for (int i = 0; i < shardStatuses.length; i++) {
            this.shardStatuses[i] = Status.fromString(shardStatuses[i]);
        }
        return this;
    }

    /**
     * Returns the shard criteria to get store information on
     */
    public Status[] shardStatuses() {
        return shardStatuses;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public IndicesShardsStoresRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public IndicesShardsStoresRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        out.writeVInt(shardStatuses.length);
        for (Status shardStatus : shardStatuses) {
            shardStatus.writeTo(out);
        }
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        int nStatus = in.readVInt();
        shardStatuses = new Status[nStatus];
        for (int i = 0; i < nStatus; i++) {
            shardStatuses[i] = Status.readFrom(in);
        }
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }
}

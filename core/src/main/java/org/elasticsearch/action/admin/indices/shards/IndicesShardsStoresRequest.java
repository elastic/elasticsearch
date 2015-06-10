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

import java.io.IOException;

public class IndicesShardsStoresRequest extends MasterNodeReadRequest<IndicesShardsStoresRequest> implements IndicesRequest.Replaceable {

    /**
     * Shard criteria to get store information on
     */
    public enum ShardState {
        /**
         * Shard that has at least one
         * active copy
         */
        ALLOCATED((byte) 0),

        /**
         * Shard that has at least one
         * unallocated (un-assigned) copy
         * (primary or replica)
         */
        UNALLOCATED((byte) 1),

        /**
         * All Shards
         */
        ALL((byte) 2);

        private final byte id;

        ShardState(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static ShardState fromId(byte id) {
            switch (id) {
                case 0:
                    return ALLOCATED;
                case 1:
                    return UNALLOCATED;
                case 2:
                    return ALL;
                default:
                    throw new IllegalArgumentException("No shard state for id [" + id + "]");
            }
        }

        public static ShardState fromString(String shardState) {
            if (shardState.equalsIgnoreCase("allocated")) {
                return ALLOCATED;
            } else if (shardState.equalsIgnoreCase("unallocated")) {
                return UNALLOCATED;
            } else if (shardState.equalsIgnoreCase("all")) {
                return ALL;
            } else {
                throw new IllegalArgumentException("unknown shard state [" + shardState + "]");
            }
        }
    }

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpenAndForbidClosed();
    private ShardState shardState = ShardState.UNALLOCATED;

    /**
     * Create a request for shards stores info for <code>indices</code>
     */
    public IndicesShardsStoresRequest(String... indices) {
        this.indices = indices;
    }

    IndicesShardsStoresRequest() {
    }

    /**
     * Set the criteria for shards to get stores info on.
     * @param shardState one of "allocated", "unallocated" or "all"
     * see {@link ShardState} for details
     */
    public IndicesShardsStoresRequest shardState(String shardState) {
        return this.shardState(ShardState.fromString(shardState));
    }

    /**
     * Set the criteria for shards to get stores info on.
     * see {@link ShardState}
     */
    public IndicesShardsStoresRequest shardState(ShardState shardState) {
        this.shardState = shardState;
        return this;
    }

    /**
     * Returns the shard criteria to get store information on
     */
    public ShardState shardState() {
        return shardState;
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
        out.writeByte(shardState.id());
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        shardState = ShardState.fromId(in.readByte());
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }
}

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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Uniquely identifies an allocation. An allocation is a shard moving from unassigned to initializing,
 * or relocation.
 * <p/>
 * Relocation is a special case, where the origin shard is relocating with a relocationId and same id, and
 * the target shard (only materialized in RoutingNodes) is initializing with the id set to the origin shard
 * relocationId. Once relocation is done, the new allocation id is set to the relocationId. This is similar
 * behavior to how ShardRouting#currentNodeId is used.
 */
public class AllocationId implements ToXContent {

    private final String id;
    private final String relocationId;

    AllocationId(StreamInput in) throws IOException {
        this.id = in.readString();
        this.relocationId = in.readOptionalString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeOptionalString(this.relocationId);
    }

    private AllocationId(String id, String relocationId) {
        this.id = id;
        this.relocationId = relocationId;
    }

    /**
     * Creates a new allocation id for initializing allocation.
     */
    public static AllocationId newInitializing() {
        return new AllocationId(Strings.randomBase64UUID(), null);
    }

    /**
     * Creates a new allocation id for the target initializing shard that is the result
     * of a relocation.
     */
    public static AllocationId newTargetRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getRelocationId(), allocationId.getId());
    }

    /**
     * Creates a new allocation id for a shard that moves to be relocated, populating
     * the transient holder for relocationId.
     */
    public static AllocationId newRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() == null;
        return new AllocationId(allocationId.getId(), Strings.randomBase64UUID());
    }

    /**
     * Creates a new allocation id representing a cancelled relocation.
     * <p/>
     * Note that this is expected to be called on the allocation id
     * of the *source* shard
     */
    public static AllocationId cancelRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getId(), null);
    }

    /**
     * Creates a new allocation id finalizing a relocation.
     * <p/>
     * Note that this is expected to be called on the allocation id
     * of the *target* shard and thus it only needs to clear the relocating id.
     */
    public static AllocationId finishRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getId(), null);
    }

    /**
     * The allocation id uniquely identifying an allocation, note, if it is relocation
     * the {@link #getRelocationId()} need to be taken into account as well.
     */
    public String getId() {
        return id;
    }

    /**
     * The transient relocation id holding the unique id that is used for relocation.
     */
    public String getRelocationId() {
        return relocationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        AllocationId that = (AllocationId) o;
        if (!id.equals(that.id)) {
            return false;
        }
        return !(relocationId != null ? !relocationId.equals(that.relocationId) : that.relocationId != null);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (relocationId != null ? relocationId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[id=" + id + (relocationId == null ? "" : ", rId=" + relocationId) + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("allocation_id");
        builder.field("id", id);
        if (relocationId != null) {
            builder.field("relocation_id", relocationId);
        }
        builder.endObject();
        return builder;
    }
}

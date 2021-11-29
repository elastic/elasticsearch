/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Uniquely identifies an allocation. An allocation is a shard moving from unassigned to initializing,
 * or relocation.
 * <p>
 * Relocation is a special case, where the origin shard is relocating with a relocationId and same id, and
 * the target shard (only materialized in RoutingNodes) is initializing with the id set to the origin shard
 * relocationId. Once relocation is done, the new allocation id is set to the relocationId. This is similar
 * behavior to how ShardRouting#currentNodeId is used.
 */
public class AllocationId implements ToXContentObject, Writeable {
    private static final String ID_KEY = "id";
    private static final String RELOCATION_ID_KEY = "relocation_id";

    private static final ObjectParser<AllocationId.Builder, Void> ALLOCATION_ID_PARSER = new ObjectParser<>("allocationId");

    static {
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setId, new ParseField(ID_KEY));
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setRelocationId, new ParseField(RELOCATION_ID_KEY));
    }

    private static class Builder {
        private String id;
        private String relocationId;

        public void setId(String id) {
            this.id = id;
        }

        public void setRelocationId(String relocationId) {
            this.relocationId = relocationId;
        }

        public AllocationId build() {
            return new AllocationId(id, relocationId);
        }
    }

    private final String id;
    @Nullable
    private final String relocationId;

    AllocationId(StreamInput in) throws IOException {
        this.id = in.readString();
        this.relocationId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeOptionalString(this.relocationId);
    }

    private AllocationId(String id, String relocationId) {
        Objects.requireNonNull(id, "Argument [id] must be non-null");
        this.id = id;
        this.relocationId = relocationId;
    }

    /**
     * Creates a new allocation id for initializing allocation.
     */
    public static AllocationId newInitializing() {
        return new AllocationId(UUIDs.randomBase64UUID(), null);
    }

    /**
     * Creates a new allocation id for initializing allocation based on an existing id.
     */
    public static AllocationId newInitializing(String existingAllocationId) {
        return new AllocationId(existingAllocationId, null);
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
        return new AllocationId(allocationId.getId(), UUIDs.randomBase64UUID());
    }

    /**
     * Creates a new allocation id representing a cancelled relocation.
     * <p>
     * Note that this is expected to be called on the allocation id
     * of the *source* shard
     */
    public static AllocationId cancelRelocation(AllocationId allocationId) {
        assert allocationId.getRelocationId() != null;
        return new AllocationId(allocationId.getId(), null);
    }

    /**
     * Creates a new allocation id finalizing a relocation.
     * <p>
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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllocationId that = (AllocationId) o;
        return Objects.equals(id, that.id) && Objects.equals(relocationId, that.relocationId);
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
        builder.startObject();
        builder.field(ID_KEY, id);
        if (relocationId != null) {
            builder.field(RELOCATION_ID_KEY, relocationId);
        }
        builder.endObject();
        return builder;
    }

    public static AllocationId fromXContent(XContentParser parser) throws IOException {
        return ALLOCATION_ID_PARSER.parse(parser, new AllocationId.Builder(), null).build();
    }
}

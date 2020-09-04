/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm.step.info;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class AllocationInfo implements ToXContentObject {
    private final long actualReplicas;
    private final long numberShardsLeftToAllocate;
    private final boolean allShardsActive;
    private final String message;

    static final ParseField ACTUAL_REPLICAS = new ParseField("actual_replicas");
    static final ParseField SHARDS_TO_ALLOCATE = new ParseField("shards_left_to_allocate");
    static final ParseField ALL_SHARDS_ACTIVE = new ParseField("all_shards_active");
    static final ParseField MESSAGE = new ParseField("message");
    static final ConstructingObjectParser<AllocationInfo, Void> PARSER = new ConstructingObjectParser<>("allocation_routed_step_info",
        a -> new AllocationInfo((long) a[0], (long) a[1], (boolean) a[2], (String) a[3]));

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), ACTUAL_REPLICAS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), SHARDS_TO_ALLOCATE);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), ALL_SHARDS_ACTIVE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MESSAGE);
    }

    public AllocationInfo(long actualReplicas, long numberShardsLeftToAllocate, boolean allShardsActive, String message) {
        this.actualReplicas = actualReplicas;
        this.numberShardsLeftToAllocate = numberShardsLeftToAllocate;
        this.allShardsActive = allShardsActive;
        this.message = message;
    }

    /**
     * Builds the AllocationInfo representing a cluster state with a routing table that does not have enough shards active for a
     * particular index.
     */
    public static AllocationInfo waitingForActiveShardsAllocationInfo(long actualReplicas) {
        return new AllocationInfo(actualReplicas, -1, false,
            "Waiting for all shard copies to be active");
    }

    /**
     * Builds the AllocationInfo representing a cluster state with a routing table that has all the shards active for a particular index
     * but there are still {@link #numberShardsLeftToAllocate} left to be allocated.
     */
    public static AllocationInfo allShardsActiveAllocationInfo(long actualReplicas, long numberShardsLeftToAllocate) {
        return new AllocationInfo(actualReplicas, numberShardsLeftToAllocate, true, "Waiting for [" + numberShardsLeftToAllocate +
            "] shards to be allocated to nodes matching the given filters");
    }

    public long getActualReplicas() {
        return actualReplicas;
    }

    public long getNumberShardsLeftToAllocate() {
        return numberShardsLeftToAllocate;
    }

    public boolean allShardsActive() {
        return allShardsActive;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MESSAGE.getPreferredName(), message);
        builder.field(SHARDS_TO_ALLOCATE.getPreferredName(), numberShardsLeftToAllocate);
        builder.field(ALL_SHARDS_ACTIVE.getPreferredName(), allShardsActive);
        builder.field(ACTUAL_REPLICAS.getPreferredName(), actualReplicas);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(actualReplicas, numberShardsLeftToAllocate, allShardsActive);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AllocationInfo other = (AllocationInfo) obj;
        return Objects.equals(actualReplicas, other.actualReplicas) &&
            Objects.equals(numberShardsLeftToAllocate, other.numberShardsLeftToAllocate) &&
            Objects.equals(message, other.message) &&
            Objects.equals(allShardsActive, other.allShardsActive);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

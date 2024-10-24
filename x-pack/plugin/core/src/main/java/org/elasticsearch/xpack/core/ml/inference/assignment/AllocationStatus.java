/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

public class AllocationStatus implements Writeable, ToXContentObject {

    public enum State {
        STARTING,
        STARTED,
        FULLY_ALLOCATED;

        public static State fromString(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }

        public boolean isAnyOf(State... candidates) {
            return Arrays.stream(candidates).anyMatch(candidate -> this == candidate);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static ParseField ALLOCATION_COUNT = new ParseField("allocation_count");
    public static ParseField TARGET_ALLOCATION_COUNT = new ParseField("target_allocation_count");
    public static ParseField STATE = new ParseField("state");

    private static final ConstructingObjectParser<AllocationStatus, Void> PARSER = new ConstructingObjectParser<>(
        "allocation_health",
        a -> new AllocationStatus((int) a[0], (int) a[1])
    );
    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), ALLOCATION_COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), TARGET_ALLOCATION_COUNT);
        // NOTE: We ignore this as we calculate it given allocation and target allocation counts
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), STATE);
    }

    public static AllocationStatus fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final int allocationCount;
    private final int targetAllocationCount;

    public AllocationStatus(int allocationCount, int targetAllocationCount) {
        this.allocationCount = allocationCount;
        this.targetAllocationCount = targetAllocationCount;
        if (allocationCount < 0) {
            throw new IllegalArgumentException("[" + ALLOCATION_COUNT.getPreferredName() + "] must be greater than or equal to 0");
        }
        if (targetAllocationCount < 0) {
            throw new IllegalArgumentException("[" + TARGET_ALLOCATION_COUNT.getPreferredName() + "] must be greater than or equal to 0");
        }
    }

    public AllocationStatus(StreamInput in) throws IOException {
        this.allocationCount = in.readVInt();
        this.targetAllocationCount = in.readVInt();
    }

    public State calculateState() {
        if (allocationCount == 0 && targetAllocationCount > 0) {
            return State.STARTING;
        }
        if (allocationCount < targetAllocationCount) {
            return State.STARTED;
        }
        return State.FULLY_ALLOCATED;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ALLOCATION_COUNT.getPreferredName(), allocationCount);
        builder.field(TARGET_ALLOCATION_COUNT.getPreferredName(), targetAllocationCount);
        builder.field(STATE.getPreferredName(), calculateState());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(allocationCount);
        out.writeVInt(targetAllocationCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AllocationStatus that = (AllocationStatus) o;
        return allocationCount == that.allocationCount && targetAllocationCount == that.targetAllocationCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(allocationCount, targetAllocationCount);
    }
}

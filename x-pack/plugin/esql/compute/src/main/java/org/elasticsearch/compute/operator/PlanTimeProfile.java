/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Profile information for plan optimization phases.
 * Captures timing information for logical and physical optimization steps.
 *
 */
public final class PlanTimeProfile implements Writeable, ToXContentObject {
    public static final ProfileSchema<PlanTimeProfile> PROFILE_SCHEMA = ProfileSchema.fragment(
        PlanTimeProfile.class,
        (builder, value, params) -> {
            if (value.logicalOptimizationNanos > 0) {
                builder.field("logical_optimization_nanos", value.logicalOptimizationNanos);
            }
            if (value.physicalOptimizationNanos > 0) {
                builder.field("physical_optimization_nanos", value.physicalOptimizationNanos);
            }
            if (value.reductionPlanNanos > 0) {
                builder.field("reduction_nanos", value.reductionPlanNanos);
            }
            return builder;
        }
    )
        .optional("logical_optimization_nanos", ProfileSchema.integer())
        .optional("physical_optimization_nanos", ProfileSchema.integer())
        .optional("reduction_nanos", ProfileSchema.integer())
        .build();

    private long reductionPlanNanos;
    private long logicalOptimizationNanos;
    private long physicalOptimizationNanos;

    /**
     * @param logicalOptimizationNanos  Time spent on local logical plan optimization (in nanoseconds)
     * @param physicalOptimizationNanos Time spent on local physical plan optimization (in nanoseconds)
     * @param reductionPlanNanos Time spent on reduction plan for node_reduce phase (in nanoseconds)
     */
    public PlanTimeProfile(long logicalOptimizationNanos, long physicalOptimizationNanos, long reductionPlanNanos) {
        this.logicalOptimizationNanos = logicalOptimizationNanos;
        this.physicalOptimizationNanos = physicalOptimizationNanos;
        this.reductionPlanNanos = reductionPlanNanos;
    }

    public PlanTimeProfile() {
        this.logicalOptimizationNanos = 0L;
        this.physicalOptimizationNanos = 0L;
        this.reductionPlanNanos = 0L;
    }

    public PlanTimeProfile(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(logicalOptimizationNanos);
        out.writeVLong(physicalOptimizationNanos);
        out.writeVLong(reductionPlanNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return PROFILE_SCHEMA.write(builder, this, params);
    }

    public void addLogicalOptimizationPlanTime(long logicalOptimizationPlanTime) {
        this.logicalOptimizationNanos = this.logicalOptimizationNanos + logicalOptimizationPlanTime;
    }

    public void addPhysicalOptimizationPlanTime(long physicalOptimizationPlanTime) {
        this.physicalOptimizationNanos = this.physicalOptimizationNanos + physicalOptimizationPlanTime;
    }

    public void addReductionPlanNanos(long reductionPlanNanos) {
        this.reductionPlanNanos = this.reductionPlanNanos + reductionPlanNanos;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PlanTimeProfile) obj;
        return this.logicalOptimizationNanos == that.logicalOptimizationNanos
            && this.physicalOptimizationNanos == that.physicalOptimizationNanos
            && this.reductionPlanNanos == that.reductionPlanNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalOptimizationNanos, physicalOptimizationNanos, reductionPlanNanos);
    }

    @Override
    public String toString() {
        return "PlanTimeProfile["
            + "logicalOptimizationNanos="
            + logicalOptimizationNanos
            + ", "
            + "physicalOptimizationNanos="
            + physicalOptimizationNanos
            + ", "
            + "reductionPlanNanos="
            + reductionPlanNanos
            + ']';
    }

}

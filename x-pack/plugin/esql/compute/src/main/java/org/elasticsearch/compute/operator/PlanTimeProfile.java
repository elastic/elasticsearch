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
    private long planNanos;
    private long logicalOptimizationNanos;
    private long physicalOptimizationNanos;

    /**
     * @param logicalOptimizationNanos  Time spent on local logical plan optimization (in nanoseconds)
     * @param physicalOptimizationNanos Time spent on local physical plan optimization (in nanoseconds)
     */
    public PlanTimeProfile(long planNanos, long logicalOptimizationNanos, long physicalOptimizationNanos) {
        this.planNanos = planNanos;
        this.logicalOptimizationNanos = logicalOptimizationNanos;
        this.physicalOptimizationNanos = physicalOptimizationNanos;
    }

    public PlanTimeProfile() {
        this.planNanos = 0L;
        this.logicalOptimizationNanos = 0L;
        this.physicalOptimizationNanos = 0L;
    }

    public PlanTimeProfile(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(planNanos);
        out.writeVLong(logicalOptimizationNanos);
        out.writeVLong(physicalOptimizationNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (planNanos > 0) {
            builder.field("plan_nanos", planNanos);
        }
        if (logicalOptimizationNanos > 0) {
            builder.field("logical_optimization_nanos", logicalOptimizationNanos);
        }
        if (physicalOptimizationNanos > 0) {
            builder.field("physical_optimization_nanos", physicalOptimizationNanos);
        }
        return builder;
    }

    public void addPlanTime(long planNanos) {
        this.planNanos = this.planNanos + planNanos;
    }

    public void addLogicalOptimizationPlanTime(long logicalOptimizationPlanTime) {
        this.logicalOptimizationNanos = this.logicalOptimizationNanos + logicalOptimizationPlanTime;
    }

    public void addPhysicalOptimizationPlanTime(long physicalOptimizationPlanTime) {
        this.physicalOptimizationNanos = this.physicalOptimizationNanos + physicalOptimizationPlanTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PlanTimeProfile) obj;
        return this.planNanos == that.planNanos
            && this.logicalOptimizationNanos == that.logicalOptimizationNanos
            && this.physicalOptimizationNanos == that.physicalOptimizationNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(planNanos, logicalOptimizationNanos, physicalOptimizationNanos);
    }

    @Override
    public String toString() {
        return "PlanTimeProfile["
            + "planNanos="
            + planNanos
            + ", "
            + "logicalOptimizationNanos="
            + logicalOptimizationNanos
            + ", "
            + "physicalOptimizationNanos="
            + physicalOptimizationNanos
            + ']';
    }

}

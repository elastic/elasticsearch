/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Status of an {@link Operator}.
 *
 * @param operator String representation of the {@link Operator}.
 * @param status Status as reported by the {@link Operator}.
 * @param planNodeId Id of the physical plan node that created this operator, or null when there is none
 *                   (output, exchange plumbing, tests). Omitted on the wire before {@link PlanProfile#JSON_PLANS_VERSION}.
 */
public record OperatorStatus(String operator, @Nullable Operator.Status status, @Nullable String planNodeId)
    implements
        Writeable,
        ToXContentObject {

    /** Operators profiled before plan-node ids existed. */
    public OperatorStatus(String operator, @Nullable Operator.Status status) {
        this(operator, status, null);
    }

    public static OperatorStatus readFrom(StreamInput in) throws IOException {
        String operator = in.readString();
        Operator.Status status = in.readOptionalNamedWriteable(Operator.Status.class);
        String planNodeId = null;
        if (in.getTransportVersion().supports(PlanProfile.JSON_PLANS_VERSION)) {
            planNodeId = in.readOptionalString();
        }
        return new OperatorStatus(operator, status, planNodeId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(operator);
        out.writeOptionalNamedWriteable(status != null && status.supportsVersion(out.getTransportVersion()) ? status : null);
        if (out.getTransportVersion().supports(PlanProfile.JSON_PLANS_VERSION)) {
            out.writeOptionalString(planNodeId);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("operator", operator);
        if (planNodeId != null) {
            builder.field("plan_node_id", planNodeId);
        }
        if (status != null) {
            builder.field("status", status);
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * The number of documents found by this operator. Most operators
     * don't find documents and will return {@code 0} here.
     */
    public long documentsFound() {
        if (status == null) {
            return 0;
        }
        return status.documentsFound();
    }

    /**
     * The number of values loaded by this operator. Most operators
     * don't load values and will return {@code 0} here.
     */
    public long valuesLoaded() {
        if (status == null) {
            return 0;
        }
        return status.valuesLoaded();
    }

    /**
     * Rows this operator emitted to its downstream consumer. Source operators populate;
     * pass-through operators return {@code 0}.
     */
    public long rowsEmitted() {
        if (status == null) {
            return 0;
        }
        return status.rowsEmitted();
    }

    /**
     * Pre-decompression bytes pulled from the storage layer. Populated by external-source
     * operators; others return {@code 0}.
     */
    public long bytesRead() {
        if (status == null) {
            return 0;
        }
        return status.bytesRead();
    }

    /**
     * Wall time the format reader spent reading on the producer thread, in nanoseconds.
     * Populated by external-source operators; others return {@code 0}.
     */
    public long readNanos() {
        if (status == null) {
            return 0;
        }
        return status.readNanos();
    }

    /**
     * CPU time the format reader spent on the producer thread (no IO wait), in nanoseconds.
     * Populated by external-source operators; others return {@code 0}.
     */
    public long readCpuNanos() {
        if (status == null) {
            return 0;
        }
        return status.readCpuNanos();
    }
}

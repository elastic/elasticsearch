/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Profile results from a single {@link Driver}.
 */
public class DriverProfile implements Writeable, ChunkedToXContentObject {
    /**
     * Nanos between creation and completion of the {@link Driver}.
     */
    private final long tookNanos;

    /**
     * Nanos this {@link Driver} has been running on the cpu. Does not
     * include async or waiting time.
     */
    private final long cpuNanos;

    /**
     * The number of times the driver has moved a single page up the
     * chain of operators as far as it'll go.
     */
    private final long iterations;

    /**
     * Status of each {@link Operator} in the driver when it finishes.
     */
    private final List<DriverStatus.OperatorStatus> operators;

    public DriverProfile(long tookNanos, long cpuNanos, long iterations, List<DriverStatus.OperatorStatus> operators) {
        this.tookNanos = tookNanos;
        this.cpuNanos = cpuNanos;
        this.iterations = iterations;
        this.operators = operators;
    }

    public DriverProfile(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            this.tookNanos = in.readVLong();
            this.cpuNanos = in.readVLong();
            this.iterations = in.readVLong();
        } else {
            this.tookNanos = 0;
            this.cpuNanos = 0;
            this.iterations = 0;
        }
        this.operators = in.readCollectionAsImmutableList(DriverStatus.OperatorStatus::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeVLong(tookNanos);
            out.writeVLong(cpuNanos);
            out.writeVLong(iterations);
        }
        out.writeCollection(operators);
    }

    /**
     * Nanos between creation and completion of the {@link Driver}.
     */
    public long tookNanos() {
        return tookNanos;
    }

    /**
     * Nanos this {@link Driver} has been running on the cpu. Does not
     * include async or waiting time.
     */
    public long cpuNanos() {
        return cpuNanos;
    }

    /**
     * The number of times the driver has moved a single page up the
     * chain of operators as far as it'll go.
     */
    public long iterations() {
        return iterations;
    }

    public List<DriverStatus.OperatorStatus> operators() {
        return operators;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startObject(), Iterators.single((b, p) -> {
            b.field("took_nanos", tookNanos);
            if (b.humanReadable()) {
                b.field("took_time", TimeValue.timeValueNanos(tookNanos));
            }
            b.field("cpu_nanos", cpuNanos);
            if (b.humanReadable()) {
                b.field("cpu_time", TimeValue.timeValueNanos(cpuNanos));
            }
            b.field("iterations", iterations);
            return b;
        }), ChunkedToXContentHelper.array("operators", operators.iterator()), ChunkedToXContentHelper.endObject());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DriverProfile that = (DriverProfile) o;
        return tookNanos == that.tookNanos
            && cpuNanos == that.cpuNanos
            && iterations == that.iterations
            && Objects.equals(operators, that.operators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tookNanos, cpuNanos, iterations, operators);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

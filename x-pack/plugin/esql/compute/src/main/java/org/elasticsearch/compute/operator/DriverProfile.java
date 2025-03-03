/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
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
     * Millis since epoch when the driver started.
     */
    private final long startMillis;

    /**
     * Millis since epoch when the driver stopped.
     */
    private final long stopMillis;

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
     * Status of each {@link Operator} in the driver when it finished.
     */
    private final List<DriverStatus.OperatorStatus> operators;

    private final DriverSleeps sleeps;

    public DriverProfile(
        long startMillis,
        long stopMillis,
        long tookNanos,
        long cpuNanos,
        long iterations,
        List<DriverStatus.OperatorStatus> operators,
        DriverSleeps sleeps
    ) {
        this.startMillis = startMillis;
        this.stopMillis = stopMillis;
        this.tookNanos = tookNanos;
        this.cpuNanos = cpuNanos;
        this.iterations = iterations;
        this.operators = operators;
        this.sleeps = sleeps;
    }

    public DriverProfile(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            this.startMillis = in.readVLong();
            this.stopMillis = in.readVLong();
        } else {
            this.startMillis = 0;
            this.stopMillis = 0;
        }
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
        this.sleeps = DriverSleeps.read(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeVLong(startMillis);
            out.writeVLong(stopMillis);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeVLong(tookNanos);
            out.writeVLong(cpuNanos);
            out.writeVLong(iterations);
        }
        out.writeCollection(operators);
        sleeps.writeTo(out);
    }

    /**
     * Millis since epoch when the driver started.
     */
    public long startMillis() {
        return startMillis;
    }

    /**
     * Millis since epoch when the driver stopped.
     */
    public long stopMillis() {
        return stopMillis;
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

    /**
     * Status of each {@link Operator} in the driver when it finished.
     */
    public List<DriverStatus.OperatorStatus> operators() {
        return operators;
    }

    /**
     * Records of the times the driver has slept.
     */
    public DriverSleeps sleeps() {
        return sleeps;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params).object(ob -> {
            ob.append((b, p) -> {
                b.timestampFieldsFromUnixEpochMillis("start_millis", "start", startMillis);
                b.timestampFieldsFromUnixEpochMillis("stop_millis", "stop", stopMillis);
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
            });
            ob.array("operators", operators.iterator());
            ob.field("sleeps", sleeps);
        });
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
        return startMillis == that.startMillis
            && stopMillis == that.stopMillis
            && tookNanos == that.tookNanos
            && cpuNanos == that.cpuNanos
            && iterations == that.iterations
            && Objects.equals(operators, that.operators)
            && sleeps.equals(that.sleeps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startMillis, stopMillis, tookNanos, cpuNanos, iterations, operators, sleeps);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

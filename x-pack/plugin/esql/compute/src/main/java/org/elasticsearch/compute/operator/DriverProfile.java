/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.compute.operator.DriverStatus.ESQL_DRIVER_NANOS;

/**
 * Profile results from a single {@link Driver}.
 *
 * @param description Description of the driver. This description should be short and meaningful as a grouping identifier.
 *                    We use the phase of the query right now: "data", "node_reduce", "final".
 * @param clusterName The name of the cluster this driver is running on.
 * @param nodeName The name of the node this driver is running on.
 * @param startMillis Millis since epoch when the driver started.
 * @param stopMillis Millis since epoch when the driver stopped.
 * @param tookNanos Nanos between creation and completion of the {@link Driver}.
 * @param cpuNanos Thread CPU nanos this {@link Driver} has used. Does not include async or waiting time, time spent
 *                 waiting for a core, or time blocked inside an operator. 0 if the JVM can't measure thread CPU time.
 *                 Includes the thread CPU time its operators used on other threads
 *                 ({@link DriverContext#meteredCpu}) and the driver's own work just outside the span
 *                 {@code driverNanos} covers, such as finishing after its loop, so it can exceed {@code driverNanos}.
 * @param driverNanos Nanos this {@link Driver} has held a thread. Unlike {@code cpuNanos} this includes time the thread
 *                    waited for a core, GC pauses and time blocked inside an operator. Does not include async or
 *                    waiting time.
 * @param iterations The number of times the driver has moved a single page up the chain of operators as far as it'll go.
 * @param operators Status of each {@link Operator} in the driver when it finished.
 */
public record DriverProfile(
    String description,
    String clusterName,
    String nodeName,
    long startMillis,
    long stopMillis,
    long tookNanos,
    long cpuNanos,
    long driverNanos,
    long iterations,
    List<OperatorStatus> operators,
    DriverSleeps sleeps
) implements Writeable, ChunkedToXContentObject {

    public static final ProfileSchema<DriverProfile> PROFILE_SCHEMA = ProfileSchema.chunked(
        DriverProfile.class,
        DriverProfile::profileChunks
    )
        .required("description", ProfileSchema.string())
        .required("cluster_name", ProfileSchema.string())
        .required("node_name", ProfileSchema.string())
        .optional("start", ProfileSchema.string())
        .required("start_millis", ProfileSchema.integer())
        .optional("stop", ProfileSchema.string())
        .required("stop_millis", ProfileSchema.integer())
        .required("took_nanos", ProfileSchema.integer())
        .optional("took_time", ProfileSchema.string())
        .required("cpu_nanos", ProfileSchema.integer())
        .optional("cpu_time", ProfileSchema.string())
        .required("driver_nanos", ProfileSchema.integer())
        .optional("driver_time", ProfileSchema.string())
        .required("documents_found", ProfileSchema.integer())
        .required("values_loaded", ProfileSchema.integer())
        .required("rows_emitted", ProfileSchema.integer())
        .required("bytes_read", ProfileSchema.integer())
        .required("read_nanos", ProfileSchema.integer())
        .required("read_cpu_nanos", ProfileSchema.integer())
        .required("iterations", ProfileSchema.integer())
        .required("operators", ProfileSchema.array(OperatorStatus.PROFILE_SCHEMA.schema()))
        .required("sleeps", DriverSleeps.PROFILE_SCHEMA.schema())
        .build();

    private static Iterator<? extends ToXContent> profileChunks(DriverProfile value, ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((builder, chunkParams) -> writeProfileFields(builder, value)),
            ChunkedToXContentHelper.array("operators", value.operators.iterator()),
            ChunkedToXContentHelper.chunk((builder, chunkParams) -> builder.field("sleeps", value.sleeps)),
            ChunkedToXContentHelper.endObject()
        );
    }

    private static XContentBuilder writeProfileFields(XContentBuilder builder, DriverProfile value) throws IOException {
        builder.field("description", value.description);
        builder.field("cluster_name", value.clusterName);
        builder.field("node_name", value.nodeName);
        builder.timestampFieldsFromUnixEpochMillis("start_millis", "start", value.startMillis);
        builder.timestampFieldsFromUnixEpochMillis("stop_millis", "stop", value.stopMillis);
        builder.field("took_nanos", value.tookNanos);
        if (builder.humanReadable()) {
            builder.field("took_time", TimeValue.timeValueNanos(value.tookNanos));
        }
        builder.field("cpu_nanos", value.cpuNanos);
        if (builder.humanReadable()) {
            builder.field("cpu_time", TimeValue.timeValueNanos(value.cpuNanos));
        }
        builder.field("driver_nanos", value.driverNanos);
        if (builder.humanReadable()) {
            builder.field("driver_time", TimeValue.timeValueNanos(value.driverNanos));
        }
        builder.field("documents_found", value.operators.stream().mapToLong(OperatorStatus::documentsFound).sum());
        builder.field("values_loaded", value.operators.stream().mapToLong(OperatorStatus::valuesLoaded).sum());
        builder.field("rows_emitted", value.operators.stream().mapToLong(OperatorStatus::rowsEmitted).sum());
        builder.field("bytes_read", value.operators.stream().mapToLong(OperatorStatus::bytesRead).sum());
        builder.field("read_nanos", value.operators.stream().mapToLong(OperatorStatus::readNanos).sum());
        builder.field("read_cpu_nanos", value.operators.stream().mapToLong(OperatorStatus::readCpuNanos).sum());
        builder.field("iterations", value.iterations);
        return builder;
    }

    private static final TransportVersion ESQL_DRIVER_NODE_DESCRIPTION = TransportVersion.fromName("esql_driver_node_description");
    private static final TransportVersion ESQL_DRIVER_TASK_DESCRIPTION = TransportVersion.fromName("esql_driver_task_description");

    public static DriverProfile readFrom(StreamInput in) throws IOException {
        return new DriverProfile(
            in.getTransportVersion().supports(ESQL_DRIVER_TASK_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().supports(ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().supports(ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.getTransportVersion().supports(ESQL_DRIVER_NANOS) ? in.readVLong() : 0,
            in.readVLong(),
            in.readCollectionAsImmutableList(OperatorStatus::readFrom),
            DriverSleeps.read(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_DRIVER_TASK_DESCRIPTION)) {
            out.writeString(description);
        }
        if (out.getTransportVersion().supports(ESQL_DRIVER_NODE_DESCRIPTION)) {
            out.writeString(clusterName);
            out.writeString(nodeName);
        }
        out.writeVLong(startMillis);
        out.writeVLong(stopMillis);
        out.writeVLong(tookNanos);
        out.writeVLong(cpuNanos);
        if (out.getTransportVersion().supports(ESQL_DRIVER_NANOS)) {
            out.writeVLong(driverNanos);
        }
        out.writeVLong(iterations);
        out.writeCollection(operators);
        sleeps.writeTo(out);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return PROFILE_SCHEMA.toXContentChunked(this, params);
    }

    @Override
    public String toString() {
        return Strings.toTruncatedString(this);
    }
}

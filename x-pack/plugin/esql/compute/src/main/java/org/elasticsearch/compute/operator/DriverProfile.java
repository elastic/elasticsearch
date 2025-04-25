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
 * @param cpuNanos Nanos this {@link Driver} has been running on the cpu. Does not include async or waiting time.
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
    long iterations,
    List<OperatorStatus> operators,
    DriverSleeps sleeps
) implements Writeable, ChunkedToXContentObject {

    public static DriverProfile readFrom(StreamInput in) throws IOException {
        return new DriverProfile(
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION)
                || in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readVLong() : 0,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readVLong() : 0,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0,
            in.readCollectionAsImmutableList(OperatorStatus::readFrom),
            DriverSleeps.read(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)) {
            out.writeString(description);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION)) {
            out.writeString(clusterName);
            out.writeString(nodeName);
        }
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

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startObject(), Iterators.single((b, p) -> {
            b.field("description", description);
            b.field("cluster_name", clusterName);
            b.field("node_name", nodeName);
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
            b.field("documents_found", operators.stream().mapToLong(OperatorStatus::documentsFound).sum());
            b.field("values_loaded", operators.stream().mapToLong(OperatorStatus::valuesLoaded).sum());
            b.field("iterations", iterations);
            return b;
        }),
            ChunkedToXContentHelper.array("operators", operators.iterator()),
            ChunkedToXContentHelper.chunk((b, p) -> b.field("sleeps", sleeps)),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}

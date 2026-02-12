/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A partition for coordinator-only data sources that don't distribute work.
 *
 * <p>Coordinator-only data sources (e.g., SQL/JDBC) execute the entire query on the
 * coordinator node — there's no splitting across data nodes. This partition simply
 * wraps the plan with no additional partition-specific state (no file lists, no
 * shard assignments, no split offsets).
 *
 * <p>Since this partition never leaves the coordinator, serialization is not
 * supported. Distributed data sources (data lake, sharded databases) should implement
 * {@link DataSourcePartition} directly with their own serializable partition type.
 *
 * @see DataSourcePartition#single
 * @see DataSourceCapabilities#forCoordinatorOnly()
 */
record CoordinatorPartition(DataSourcePlan plan) implements DataSourcePartition {

    // No nodeAffinity, estimatedRows, or estimatedBytes — coordinator-only
    // execution doesn't need scheduling hints. The default methods on
    // DataSourcePartition return null/empty, which is correct here.

    @Override
    public String getWriteableName() {
        // Required by NamedWriteable but never actually used — this partition
        // stays on the coordinator and is never serialized over the wire.
        return "esql.partition.coordinator";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Coordinator-only partitions are created and consumed on the same node.
        // If this is ever called, it means a coordinator-only data source was
        // incorrectly routed through the distributed execution path.
        throw new UnsupportedOperationException(
            "CoordinatorPartition should not be serialized — coordinator-only data sources execute on the coordinator node only"
        );
    }
}

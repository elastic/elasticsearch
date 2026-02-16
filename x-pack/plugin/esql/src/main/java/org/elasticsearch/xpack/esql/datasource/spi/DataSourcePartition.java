/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.datasource.spi.partitioning.NodeAffinity;

import java.io.IOException;
import java.util.OptionalLong;

/**
 * A partition of work that can be executed independently on a cluster data node.
 *
 * <p>Each data source defines its own implementation carrying whatever partition-specific
 * state it needs (file lists, scan tasks, shard IDs, etc.). The framework only uses the
 * common accessors defined here for scheduling and load balancing.
 *
 * <h2>Lifecycle</h2>
 *
 * <p><b>Created by:</b> {@link DataSource#planPartitions}, which is called by the physical
 * planner on the coordinator after physical planning completes.
 *
 * <p><b>Transported:</b> Serialized via {@link NamedWriteable} and sent from coordinator
 * to the target data node.
 *
 * <p><b>Consumed by:</b> {@link DataSource#createSourceOperator}, which is called by
 * {@code LocalExecutionPlanner} on the data node to create the actual operator that
 * reads data.
 *
 * <h2>Examples</h2>
 * <ul>
 *   <li>JDBC: Single partition wrapping the plan with built SQL</li>
 *   <li>Parquet: Partition containing a subset of files to read</li>
 *   <li>Iceberg: Partition containing file scan tasks with partition pruning info</li>
 *   <li>Sharded database: Partition targeting a specific shard</li>
 * </ul>
 *
 * @see DataSource#planPartitions
 * @see DataSource#createSourceOperator
 */
public interface DataSourcePartition extends NamedWriteable {

    /**
     * The data source plan for this partition.
     *
     * <p>This is the fully-optimized plan with all pushed-down operations
     * (filters, limits, SQL, etc.).
     */
    DataSourcePlan plan();

    /**
     * Node affinity for this partition — where it should or must execute.
     *
     * <p>Typically derived from the splits contained in this partition.
     * {@link NodeAffinity#require} partitions must be routed to the specified node.
     * {@link NodeAffinity#prefer} partitions should be routed there when possible.
     *
     * @see NodeAffinity
     */
    default NodeAffinity nodeAffinity() {
        return NodeAffinity.NONE;
    }

    /**
     * Estimated rows in this partition (for load balancing).
     */
    default OptionalLong estimatedRows() {
        return OptionalLong.empty();
    }

    /**
     * Estimated bytes in this partition (for memory planning).
     */
    default OptionalLong estimatedBytes() {
        return OptionalLong.empty();
    }

    /**
     * Create a single partition wrapping the plan (for coordinator-only execution).
     *
     * <p>This is a convenience for data sources that don't need partition-specific state
     * (e.g., SQL data sources that execute a single query on the coordinator).
     */
    static DataSourcePartition single(DataSourcePlan plan) {
        return new CoordinatorOnlyPartition(plan);
    }

    /**
     * A partition for coordinator-only data sources that don't distribute work.
     *
     * <p>Coordinator-only data sources (e.g., SQL/JDBC) execute the entire query on the
     * coordinator node — there's no splitting across data nodes. This partition simply
     * wraps the plan with no additional partition-specific state.
     *
     * <p>Since this partition never leaves the coordinator, serialization is not supported.
     */
    record CoordinatorOnlyPartition(DataSourcePlan plan) implements DataSourcePartition {

        @Override
        public String getWriteableName() {
            return "esql.partition.coordinator";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException(
                "CoordinatorOnlyPartition should not be serialized — coordinator-only data sources execute on the coordinator node only"
            );
        }
    }
}

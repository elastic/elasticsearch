/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.common.io.stream.NamedWriteable;

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
     * Preferred data node for execution (for data locality), or null if no preference.
     */
    default String nodeAffinity() {
        return null;
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
        return new CoordinatorPartition(plan);
    }
}

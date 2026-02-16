/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

/**
 * Declares what a data source supports in terms of execution.
 *
 * <h2>Current Scope</h2>
 *
 * <p>Currently this only contains a single boolean ({@link #distributed}) because that's
 * the only execution characteristic we need to distinguish right now. As we implement
 * more data sources and discover additional behaviors that matter, we can add fields here.
 * We intentionally keep this minimal until real use cases emerge.
 *
 * <h2>Call Sites</h2>
 *
 * <p><b>Returned by:</b> {@link DataSource#capabilities()}, which is called by:
 * <ul>
 *   <li>Physical planner - checks {@link #distributed()} to decide whether to call
 *       {@link DataSource#planPartitions} or run coordinator-only</li>
 *   <li>{@code Mapper} - uses execution mode to set physical plan node properties</li>
 * </ul>
 *
 * <p>Note: We deliberately DO NOT enumerate "pushdown capabilities" here.
 * What a data source can optimize is determined by the data source's own
 * {@link DataSource#optimizationRules()}. This avoids coupling the SPI
 * to specific optimization types.
 *
 * @param distributed Whether this data source can distribute work across data nodes.
 *                    If true, {@link DataSource#planPartitions} will be called.
 *                    If false, execution runs on coordinator only.
 */
public record DataSourceCapabilities(boolean distributed) {

    /**
     * Whether this data source must run on the coordinator only.
     */
    public boolean isCoordinatorOnly() {
        return distributed == false;
    }

    /**
     * Whether this data source supports distributed execution across data nodes.
     */
    public boolean isDistributed() {
        return distributed;
    }

    // =========================================================================
    // FACTORY METHODS
    // =========================================================================

    /**
     * Capabilities for data sources that support distributed execution (e.g., Iceberg, Parquet).
     * Work can be partitioned and executed across multiple data nodes.
     */
    public static DataSourceCapabilities forDistributed() {
        return new DataSourceCapabilities(true);
    }

    /**
     * Capabilities for data sources that run on coordinator only (e.g., databases via JDBC).
     * All data flows through the coordinator node.
     */
    public static DataSourceCapabilities forCoordinatorOnly() {
        return new DataSourceCapabilities(false);
    }
}

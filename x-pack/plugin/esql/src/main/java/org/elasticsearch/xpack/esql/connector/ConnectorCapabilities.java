/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

/**
 * Declares what a connector supports in terms of execution.
 *
 * <h2>Current Scope</h2>
 *
 * <p>Currently this only contains a single boolean ({@link #distributed}) because that's
 * the only execution characteristic we need to distinguish right now. As we implement
 * more connectors and discover additional behaviors that matter, we can add fields here.
 * We intentionally keep this minimal until real use cases emerge.
 *
 * <h2>Call Sites</h2>
 *
 * <p><b>Returned by:</b> {@link Connector#capabilities()}, which is called by:
 * <ul>
 *   <li>Physical planner - checks {@link #distributed()} to decide whether to call
 *       {@link Connector#planPartitions} or run coordinator-only</li>
 *   <li>{@code Mapper} - uses execution mode to set physical plan node properties</li>
 * </ul>
 *
 * <p>Note: We deliberately DO NOT enumerate "pushdown capabilities" here.
 * What a connector can optimize is determined by the connector's own
 * {@link Connector#optimizationRules()}. This avoids coupling the SPI
 * to specific optimization types.
 *
 * @param distributed Whether this connector can distribute work across data nodes.
 *                    If true, {@link Connector#planPartitions} will be called.
 *                    If false, execution runs on coordinator only.
 */
public record ConnectorCapabilities(boolean distributed) {

    /**
     * Whether this connector must run on the coordinator only.
     */
    public boolean isCoordinatorOnly() {
        return distributed == false;
    }

    /**
     * Whether this connector supports distributed execution across data nodes.
     */
    public boolean isDistributed() {
        return distributed;
    }

    // =========================================================================
    // FACTORY METHODS
    // =========================================================================

    /**
     * Capabilities for connectors that support distributed execution (e.g., Iceberg, Parquet).
     * Work can be partitioned and executed across multiple data nodes.
     */
    public static ConnectorCapabilities forDistributed() {
        return new ConnectorCapabilities(true);
    }

    /**
     * Capabilities for connectors that run on coordinator only (e.g., databases via JDBC).
     * All data flows through the coordinator node.
     */
    public static ConnectorCapabilities forCoordinatorOnly() {
        return new ConnectorCapabilities(false);
    }
}

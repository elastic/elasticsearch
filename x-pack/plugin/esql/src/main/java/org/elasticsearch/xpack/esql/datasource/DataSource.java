/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * A DataSource is the top-level SPI for external data sources.
 * It provides hooks for every phase of ES|QL query processing.
 *
 * <h2>Data Source Model</h2>
 *
 * <p>The syntax is {@code where:what}, where {@code where} defines the data source
 * and {@code what} defines the query/expression.
 *
 * <p><b>Registered data source:</b>
 * <pre>
 * FROM s3_logs:logs                                -- unquoted shorthand
 * FROM s3_logs:"logs/*.parquet"                    -- quoted string
 * FROM my_postgres:users                           -- unquoted shorthand
 * FROM my_postgres:query("SELECT * FROM users")    -- query function
 * </pre>
 *
 * <p><b>Inline data source:</b>
 * <pre>
 * FROM EXTERNAL({"type": "s3", "configuration": {"bucket": "my-bucket", "access_key": "...", "secret_key": "..."}, "settings": {}}):logs
 * FROM EXTERNAL({"type": "s3", "configuration": {...}, "settings": {}}):"logs/*.parquet"
 * </pre>
 *
 * <p>The expression after {@code :} is data source-specific and opaque to ES|QL.
 * It can be an unquoted identifier, a quoted string (for patterns, paths, special chars),
 * or a {@code query()} pseudo-function (equivalent to quoted).
 *
 * <h2>Invocation Flow</h2>
 *
 * <p>Methods are called by specific ES|QL components at defined phases:
 * <ol>
 *   <li><b>Resolution</b> ({@link #resolve}) - Called by {@code PreAnalyzer} when it encounters
 *       an external source reference. Returns the data source's logical plan node with schema.</li>
 *   <li><b>Logical Optimization</b> ({@link #optimizationRules}) - DataSource-provided rules run
 *       as a separate pass after the main {@code LogicalPlanOptimizer}. Rules pattern-match on
 *       standard ES|QL plan nodes (Filter, Limit, etc.) above data source plan leaves and fold
 *       operations into the leaf.</li>
 *   <li><b>Physical Planning</b> ({@link #createPhysicalPlan}) - Called by {@code Mapper} to
 *       convert the optimized logical node into a physical plan node.</li>
 *   <li><b>Work Distribution</b> ({@link #planPartitions}) - Called by physical planner
 *       to split work into partitions for parallel execution across data nodes.</li>
 *   <li><b>Execution</b> ({@link #createSourceOperator}) - Called by {@code LocalExecutionPlanner}
 *       on each data node to create the actual operator that reads data.</li>
 * </ol>
 *
 * <p>The key integration point is {@link #optimizationRules()}. Instead of enumerating
 * "pushdown types", data sources provide their own tree-rewriting rules that fold operations
 * (filters, limits, etc.) directly into their plan nodes.
 */
public interface DataSource {

    // =========================================================================
    // IDENTIFICATION
    // =========================================================================

    /**
     * Unique identifier for this data source type.
     *
     * <p>This is the value used in the {@code "type"} field of data source definitions.
     * Examples: "s3", "postgres", "iceberg", "mysql"
     */
    String type();

    // =========================================================================
    // CAPABILITIES
    // =========================================================================

    /**
     * Declare what this data source supports for execution planning.
     *
     * <p><b>Called by:</b> Physical planner to decide whether to run on coordinator only
     * or distribute across data nodes.
     */
    DataSourceCapabilities capabilities();

    // =========================================================================
    // PHASE 1: RESOLUTION
    // =========================================================================

    /**
     * Resolve an external reference and create the logical plan node.
     *
     * <p><b>Called by:</b> {@code PreAnalyzer} when it encounters an external source reference
     * in the parsed query (e.g., {@code FROM my_s3:logs/*.parquet}).
     *
     * <p><b>Purpose:</b> Connect to the external source using the provided configuration,
     * interpret the expression (table name, pattern, query), read schema, and create
     * the data source-specific {@link DataSourcePlan} node that will serve as the leaf
     * in the logical plan tree.
     *
     * <p><b>Expression interpretation:</b> The {@link DataSourceDescriptor#expression()} is
     * data source-specific and opaque to ES|QL. By the time it reaches the data source,
     * the parser has resolved all three expression forms (unquoted, quoted, query function)
     * into a plain string. Examples:
     * <ul>
     *   <li>S3/Parquet: {@code logs} or {@code logs/*.parquet} - file pattern</li>
     *   <li>Postgres: {@code users} - table name</li>
     *   <li>Postgres: {@code SELECT * FROM users WHERE active = true} - SQL query</li>
     *   <li>Iceberg: {@code my_catalog.my_schema.orders} - catalog path</li>
     * </ul>
     *
     * @param source The unresolved source with type, configuration, settings, and expression
     * @param context Resolution context providing cluster state and configuration
     * @return DataSource-specific logical plan node with schema
     */
    DataSourcePlan resolve(DataSourceDescriptor source, ResolutionContext context);

    // =========================================================================
    // PHASE 2: LOGICAL OPTIMIZATION
    // =========================================================================

    /**
     * Return optimization rules that push ES|QL operations into this data source's plan nodes.
     *
     * <p><b>Called by:</b> {@link DataSourceOptimizer}, which runs as a separate pass after
     * the main {@code LogicalPlanOptimizer} completes. This ensures data source rules see a
     * fully simplified/normalized tree (constant folding done, boolean logic simplified).
     *
     * <p><b>Rule pattern:</b> Each rule pattern-matches on a standard ES|QL plan node
     * (e.g., {@code Filter}, {@code Limit}) whose child is a data source plan node, and
     * rewrites the tree by folding the operation into the data source plan.
     *
     * <p><b>Identity guard:</b> Rules MUST verify that the child data source plan belongs
     * to this data source instance (via {@code plan.dataSource() == this}) to avoid
     * interfering with other data sources in the same query.
     *
     * <p>Base classes ({@link org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseDataSource},
     * {@link org.elasticsearch.xpack.esql.datasource.sql.SqlDataSource}) provide default
     * rules. Subclasses can override to add, remove, or replace rules.
     *
     * @return List of optimization rules, or empty list if no pushdown is supported
     */
    default List<Rule<?, LogicalPlan>> optimizationRules() {
        return List.of();
    }

    /**
     * Run all data source-provided optimization rules on a plan.
     *
     * <p><b>Called by:</b> {@code LogicalPlanOptimizer} after the main optimization pass.
     * This is internal wiring, not part of the data source SPI.
     *
     * <p>Walks the plan tree, collects {@link #optimizationRules()} from each data source
     * found in the tree, and runs them as a single batch. No-op if the plan contains
     * no data source plan nodes.
     *
     * @param plan The plan already processed by the main optimizer
     * @return The plan with data source optimizations applied
     */
    static LogicalPlan applyOptimizationRules(LogicalPlan plan) {
        return DataSourceOptimizer.optimize(plan);
    }

    // =========================================================================
    // PHASE 3: PHYSICAL PLANNING
    // =========================================================================

    /**
     * Convert the logical plan node to a physical execution plan.
     *
     * <p><b>Called by:</b> {@code Mapper} when it visits the data source's logical plan node
     * during logical-to-physical plan conversion.
     *
     * <p>Default implementation creates a {@link DataSourceExec} wrapping the
     * data source plan. The {@link DataSourceExec} carries the fully-optimized plan
     * (with any pushed-down filters, limits, SQL, etc.) through physical planning
     * and into execution.
     *
     * <p>Data sources can override this if they need a custom physical plan node,
     * but the default is sufficient for most cases.
     *
     * @param plan The DataSourcePlan with all pushed operations
     * @param context Physical planning context (coordinator-only flag, etc.)
     * @return Physical plan node for this source
     */
    default PhysicalPlan createPhysicalPlan(DataSourcePlan plan, PhysicalPlanningContext context) {
        return new DataSourceExec(plan.source(), plan);
    }

    // =========================================================================
    // PHASE 4: WORK DISTRIBUTION
    // =========================================================================

    /**
     * Plan how to distribute work across cluster data nodes.
     *
     * <p><b>Called by:</b> Physical planner after physical planning completes. Only called
     * for data sources where {@link #capabilities()}.{@link DataSourceCapabilities#distributed()}
     * is true.
     *
     * <p>Default returns a single partition for coordinator-only execution.
     *
     * @param plan The data source plan to partition
     * @param hints Distribution hints from query planner (target parallelism, available nodes)
     * @return List of partitions; each will be executed on a (potentially different) data node
     */
    default List<DataSourcePartition> planPartitions(DataSourcePlan plan, DistributionHints hints) {
        // Default: single partition (coordinator-only execution)
        return List.of(DataSourcePartition.single(plan));
    }

    // =========================================================================
    // PHASE 5: EXECUTION
    // =========================================================================

    /**
     * Create a source operator factory for executing a partition.
     *
     * <p><b>Called by:</b> {@code LocalExecutionPlanner} on the data node where this partition
     * will execute. The returned factory is called once per driver to create a
     * {@link SourceOperator} that reads data and produces {@link org.elasticsearch.compute.data.Page}s.
     *
     * <p><b>What data sources implement:</b> Data sources extend {@link SourceOperator} directly
     * to open connections to external data sources and stream results as columnar Pages.
     * See {@link org.elasticsearch.xpack.esql.datasource.sql.JdbcDataSource} for an example.
     *
     * <p><b>Page construction:</b> Use {@link DriverContext#blockFactory()} to create
     * {@link org.elasticsearch.compute.data.Block.Builder}s for each output column, append
     * values as you read them, then assemble into a {@link org.elasticsearch.compute.data.Page}.
     *
     * @param partition The partition to execute (received from coordinator via transport)
     * @param context Execution context providing BlockFactory for memory-managed page construction
     * @return Factory that creates source operators for each driver
     */
    SourceOperator.SourceOperatorFactory createSourceOperator(DataSourcePartition partition, ExecutionContext context);

    // =========================================================================
    // CONTEXT INTERFACES
    // =========================================================================

    /**
     * Context available during source resolution.
     */
    interface ResolutionContext {
        // Placeholder for cluster state, settings, etc.
    }

    /**
     * Context available during physical planning.
     */
    interface PhysicalPlanningContext {
        /**
         * Whether this query runs on coordinator only.
         */
        boolean coordinatorOnly();
    }

    /**
     * Context available during execution on a data node.
     *
     * <p>Provides access to the compute engine's memory management infrastructure.
     * Data sources use the {@link #blockFactory()} to allocate Blocks and Vectors
     * that are tracked by the circuit breaker.
     */
    interface ExecutionContext {
        /**
         * Factory for creating memory-tracked Blocks and Vectors.
         *
         * <p>Use this to create {@link org.elasticsearch.compute.data.Block.Builder}s
         * for assembling output Pages. All allocations are tracked by the circuit breaker
         * to prevent OOM.
         */
        BlockFactory blockFactory();

        /**
         * The driver context for this execution.
         *
         * <p>Advanced use only — most data sources only need {@link #blockFactory()}.
         * Provides access to BigArrays and other driver-scoped resources.
         */
        DriverContext driverContext();
    }
}

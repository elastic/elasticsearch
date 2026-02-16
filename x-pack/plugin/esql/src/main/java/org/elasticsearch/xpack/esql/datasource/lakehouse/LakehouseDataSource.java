/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasource.DataSource;
import org.elasticsearch.xpack.esql.datasource.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasource.DataSourceDescriptor;
import org.elasticsearch.xpack.esql.datasource.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.DataSourcePlan;
import org.elasticsearch.xpack.esql.datasource.DataSourcePushdownRule;
import org.elasticsearch.xpack.esql.datasource.partitioning.DataSourceSplit;
import org.elasticsearch.xpack.esql.datasource.partitioning.DistributionHints;
import org.elasticsearch.xpack.esql.datasource.partitioning.SplitPartitioner;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Base class for lakehouse data sources that read from file-based storage.
 *
 * <h2>Architecture: Storage + Format Separation</h2>
 *
 * <p>This data source composes two pluggable components:
 * <ul>
 *   <li>{@link StorageProvider} — accesses files in a storage system (S3, GCS, HDFS, local)</li>
 *   <li>{@link FormatReader} — reads a file format (Parquet, ORC, CSV, Avro)</li>
 * </ul>
 *
 * <p>Optionally, a {@link TableCatalog} can be provided for table-based sources
 * (Iceberg, Delta Lake, Hudi) that manage table structure, partitioning, and snapshots.
 *
 * <p>This separation allows any storage to be combined with any format:
 * <ul>
 *   <li>S3 + Parquet — raw Parquet files on Amazon S3</li>
 *   <li>GCS + ORC — ORC files on Google Cloud Storage</li>
 *   <li>Local FS + CSV — CSV files on local filesystem</li>
 *   <li>S3 + Parquet + Iceberg catalog — Iceberg tables on S3</li>
 * </ul>
 *
 * <h2>What This Base Class Provides</h2>
 *
 * <p><b>{@link #resolve} implementation:</b> If a {@link TableCatalog} is provided and can handle
 * the expression, resolves schema from catalog metadata. Otherwise, opens a file via
 * {@link StorageProvider} and reads metadata via {@link FormatReader#metadata}, then calls
 * {@link #createPlan} to build the data source-specific plan node.
 *
 * <p><b>Optimization rules:</b> Provides reusable rule classes ({@link #pushFilterRule()},
 * {@link #pushLimitRule()}) that subclasses can include in their
 * {@link #optimizationRules()} override. The base class returns no rules by default
 * since not all formats support filter or limit pushdown.
 *
 * <p><b>Split-based partitioning:</b> Composes a {@link SplitPartitioner} that implements
 * the discover → group → wrap pipeline. Discovery delegates to {@link #getFileTasks};
 * grouping uses {@link org.elasticsearch.xpack.esql.datasource.partitioning.SizeAwareBinPacking SizeAwareBinPacking};
 * wrapping delegates to {@link #createPartition(LakehousePlan, List)}.
 *
 * <h2>Subclass Responsibilities</h2>
 * <ul>
 *   <li>Define a data source-specific {@link LakehousePlan} implementation</li>
 *   <li>{@link #getStorageProvider} — return the storage provider for this data source</li>
 *   <li>{@link #getFormatReader} — return the format reader for this data source</li>
 *   <li>{@link #createPlan} — create the data source-specific plan node</li>
 *   <li>{@link #createPhysicalPlan} — create physical plan node</li>
 *   <li>{@link #createSourceOperator} — create the actual file reader operator</li>
 * </ul>
 *
 * <h2>Optional Overrides</h2>
 * <ul>
 *   <li>{@link #getTableCatalog()} — provide a table catalog for catalog-managed sources</li>
 *   <li>{@link #getFilterPushdownSupport()} — provide filter pushdown capability</li>
 * </ul>
 *
 * @see LakehousePlan
 * @see StorageProvider
 * @see FormatReader
 * @see TableCatalog
 * @see FilterPushdownSupport
 * @see SplitPartitioner
 */
public abstract class LakehouseDataSource implements DataSource {

    private final Logger logger = LogManager.getLogger(getClass());

    private final SplitPartitioner<FileTask> partitioner;

    /**
     * Create a lakehouse data source with default partitioning (FFD bin-packing).
     */
    @SuppressWarnings("this-escape")
    protected LakehouseDataSource() {
        this.partitioner = new SplitPartitioner<>(this::discoverFileTasks, this::wrapPartition);
    }

    /**
     * Create a lakehouse data source with a custom split partitioner.
     *
     * <p>Use this constructor when the default FFD bin-packing is not appropriate
     * (e.g., locality-first grouping, fixed-size partitions, or catalog-driven partitioning).
     *
     * @param partitioner The custom split partitioner to use
     */
    protected LakehouseDataSource(SplitPartitioner<FileTask> partitioner) {
        this.partitioner = partitioner;
    }

    @Override
    public DataSourceCapabilities capabilities() {
        return DataSourceCapabilities.forDistributed();
    }

    // =========================================================================
    // PHASE 1: RESOLUTION
    // =========================================================================

    /**
     * Default resolution: uses table catalog if available, otherwise infers schema from files.
     *
     * <p>If a {@link TableCatalog} is provided (via {@link #getTableCatalog()}) and can handle
     * the expression, resolves schema from catalog metadata. Otherwise, opens a single file
     * via {@link StorageProvider} and reads metadata (including schema) via {@link FormatReader#metadata}.
     */
    @Override
    public DataSourcePlan resolve(DataSourceDescriptor source, ResolutionContext context) {
        String expression = source.expression();

        // Try table catalog first (Iceberg, Delta Lake, Hudi)
        TableCatalog catalog = getTableCatalog();
        if (catalog != null && catalog.canHandle(expression)) {
            logger.debug("Resolving [{}] via [{}] table catalog", expression, catalog.catalogType());
            try {
                SourceMetadata metadata = catalog.metadata(expression, Map.of());
                logger.debug(
                    "Resolved schema with [{}] columns from [{}] catalog for [{}]",
                    metadata.schema().size(),
                    catalog.catalogType(),
                    expression
                );
                return createPlan(source.describe(), metadata.schema(), expression, null, null);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to resolve table metadata for: " + expression, e);
            }
        }

        // Raw file fallback: open file, read metadata
        StorageProvider storage = getStorageProvider();
        FormatReader format = getFormatReader();
        logger.debug("Resolving [{}] via [{}] format reader", expression, format.formatName());

        try {
            StoragePath path = StoragePath.of(expression);
            StorageObject object = storage.newObject(path);
            SourceMetadata metadata = format.metadata(object);
            logger.debug("Inferred schema with [{}] columns from [{}]", metadata.schema().size(), path);
            return createPlan(source.describe(), metadata.schema(), expression, null, null);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read metadata for: " + expression, e);
        }
    }

    // =========================================================================
    // PHASE 2: LOGICAL OPTIMIZATION
    // =========================================================================

    /**
     * Create a rule that pushes Filter into a LakehousePlan leaf.
     *
     * <p>Subclasses that support filter pushdown should include this in their
     * {@link #optimizationRules()} override. The rule uses {@link FilterPushdownSupport}
     * from {@link #getFilterPushdownSupport()} to determine what can be pushed.
     *
     * <p>If no {@link FilterPushdownSupport} is provided, the rule leaves the plan unchanged.
     *
     * <p>The rule handles three outcomes:
     * <ul>
     *   <li>Fully pushable: removes Filter node, applies native filter to plan</li>
     *   <li>Partially pushable: keeps Filter with remainder, applies pushable part</li>
     *   <li>Not pushable: leaves plan unchanged (ES|QL evaluates the filter)</li>
     * </ul>
     */
    protected Rule<?, LogicalPlan> pushFilterRule() {
        return new PushFilterToLakehouse();
    }

    /**
     * Create a rule that pushes Limit into a LakehousePlan leaf.
     *
     * <p>Subclasses that support limit pushdown should include this in their
     * {@link #optimizationRules()} override and also implement {@link #applyLimit}.
     *
     * <p>The rule absorbs the limit into the data source plan and removes the Limit node.
     */
    protected Rule<?, LogicalPlan> pushLimitRule() {
        return new PushLimitToLakehouse();
    }

    private class PushFilterToLakehouse extends DataSourcePushdownRule<Filter, LakehousePlan> {
        PushFilterToLakehouse() {
            super(LakehouseDataSource.this, LakehousePlan.class);
        }

        @Override
        protected LogicalPlan pushDown(Filter filter, LakehousePlan lakePlan) {
            FilterPushdownSupport fps = getFilterPushdownSupport();
            if (fps == null) {
                return filter;
            }

            FilterPushdownSupport.PushdownResult result = fps.pushFilters(List.of(filter.condition()));

            if (result.hasPushedFilter() && result.hasRemainder() == false) {
                // Fully pushed — remove Filter node
                return applyFilter(lakePlan, result.pushedFilter());
            } else if (result.hasPushedFilter()) {
                // Partially pushed — keep Filter with remainder
                LakehousePlan updated = applyFilter(lakePlan, result.pushedFilter());
                Expression remainder = result.remainder().size() == 1 ? result.remainder().get(0) : combineWithAnd(result.remainder());
                return new Filter(filter.source(), updated, remainder);
            }
            // Not pushable — leave unchanged, ES|QL will evaluate
            return filter;
        }
    }

    private class PushLimitToLakehouse extends DataSourcePushdownRule<Limit, LakehousePlan> {
        PushLimitToLakehouse() {
            super(LakehouseDataSource.this, LakehousePlan.class);
        }

        @Override
        protected LogicalPlan pushDown(Limit limit, LakehousePlan lakePlan) {
            if (limit.limit().foldable() == false) {
                return limit;
            }
            int limitValue = ((Number) limit.limit().fold(org.elasticsearch.xpack.esql.core.expression.FoldContext.small())).intValue();
            return applyLimit(lakePlan, limitValue);
        }
    }

    // =========================================================================
    // PHASE 3: PHYSICAL PLANNING
    // =========================================================================

    // createPhysicalPlan() is inherited from DataSource — creates a DataSourceExec
    // wrapping the LakehousePlan with all pushed-down operations (filters, limits).

    // =========================================================================
    // PHASE 3.5: EXECUTION
    // =========================================================================

    /**
     * Create a source operator that reads files via StorageProvider + FormatReader.
     *
     * <p>Default implementation uses the composable storage/format architecture:
     * <ol>
     *   <li>Gets the file list from the partition</li>
     *   <li>For each file, opens it via {@link StorageProvider#newObject}</li>
     *   <li>Reads data via {@link FormatReader#read} (which handles format-specific reading)</li>
     *   <li>Produces Pages of columnar data</li>
     * </ol>
     *
     * <p>This is where the storage/format separation pays off: the same operator
     * works for any combination (S3+Parquet, GCS+ORC, local+CSV, etc.).
     *
     * <p>Subclasses must override {@link #createSourceOperator} or provide
     * a concrete {@link SourceOperator} implementation that reads from the
     * storage/format components.
     */
    @Override
    public SourceOperator.SourceOperatorFactory createSourceOperator(DataSourcePartition partition, ExecutionContext context) {
        throw new UnsupportedOperationException("Subclass must override createSourceOperator");
    }

    // =========================================================================
    // PHASE 4: WORK DISTRIBUTION (via composed SplitPartitioner)
    // =========================================================================

    /**
     * Delegates to a composed {@link SplitPartitioner} that runs the
     * discover → group → wrap pipeline.
     */
    @Override
    public List<DataSourcePartition> planPartitions(DataSourcePlan plan, DistributionHints hints) {
        return partitioner.planPartitions(plan, hints);
    }

    /**
     * Discover file tasks for the plan.
     *
     * <p>Delegates to {@link #getFileTasks(LakehousePlan)} if the plan is a
     * {@link LakehousePlan}, otherwise returns empty.
     */
    private List<FileTask> discoverFileTasks(DataSourcePlan plan) {
        if (plan instanceof LakehousePlan == false) {
            return List.of();
        }
        LakehousePlan lakehousePlan = (LakehousePlan) plan;
        List<FileTask> tasks = getFileTasks(lakehousePlan);
        if (tasks.isEmpty()) {
            logger.debug("No file tasks for [{}], returning empty partitions", lakehousePlan.location());
        }
        return tasks;
    }

    /**
     * Wrap a group of file tasks into a partition.
     *
     * <p>Bridges the generic {@link SplitPartitioner} callback to the typed
     * {@link #createPartition(LakehousePlan, List)} overload.
     */
    private DataSourcePartition wrapPartition(DataSourcePlan plan, List<FileTask> splits) {
        return createPartition((LakehousePlan) plan, splits);
    }

    // =========================================================================
    // ABSTRACT METHODS - Components
    // =========================================================================

    /**
     * Return the storage provider for accessing files.
     *
     * <p>Subclasses create and configure this in their constructor based on the
     * data source configuration (bucket, credentials, endpoint, etc.).
     */
    protected abstract StorageProvider getStorageProvider();

    /**
     * Return the format reader for reading files.
     *
     * <p>Subclasses create this in their constructor. The format determines
     * schema inference and data reading capabilities.
     */
    protected abstract FormatReader getFormatReader();

    // =========================================================================
    // OPTIONAL METHODS - Components
    // =========================================================================

    /**
     * Return a table catalog for catalog-managed sources (Iceberg, Delta Lake, Hudi).
     *
     * <p>Default returns null (raw file mode). Override to provide catalog integration.
     * When a catalog is provided, {@link #resolve} will use it for schema resolution
     * if it can handle the expression.
     */
    protected TableCatalog getTableCatalog() {
        return null;
    }

    /**
     * Return filter pushdown support for this data source.
     *
     * <p>Default returns null (no filter pushdown). Override to enable filter pushdown.
     * When provided, the {@link #pushFilterRule()} will use it to determine which
     * filter expressions can be pushed to the data source.
     */
    protected FilterPushdownSupport getFilterPushdownSupport() {
        return null;
    }

    // =========================================================================
    // ABSTRACT METHODS - Plan
    // =========================================================================

    /**
     * Create the data source-specific plan node.
     *
     * <p>Called by {@link #resolve} after schema inference. Subclasses return
     * their specific {@link LakehousePlan} implementation.
     *
     * @param location Source location for error messages
     * @param schema Inferred schema as ES|QL attributes
     * @param expression The original expression from the query
     * @param filter Initial filter, or null
     * @param limit Initial limit, or null
     * @return DataSource-specific plan node
     */
    protected abstract LakehousePlan createPlan(
        String location,
        List<Attribute> schema,
        String expression,
        Expression filter,
        Integer limit
    );

    // =========================================================================
    // ABSTRACT METHODS - Operations
    // =========================================================================

    /**
     * Apply a translated filter to the plan.
     *
     * <p>Must be implemented by subclasses that include {@link #pushFilterRule()} in their
     * {@link #optimizationRules()}.
     *
     * @param plan Current plan
     * @param pushedFilter The source-native filter object (opaque, from {@link FilterPushdownSupport})
     * @return Updated plan with filter applied
     */
    protected LakehousePlan applyFilter(LakehousePlan plan, Object pushedFilter) {
        throw new UnsupportedOperationException("Override applyFilter when using pushFilterRule()");
    }

    /**
     * Apply a limit to the plan.
     *
     * <p>Must be implemented by subclasses that include {@link #pushLimitRule()} in their
     * {@link #optimizationRules()}.
     *
     * @param plan Current plan
     * @param limit The limit value
     * @return Updated plan with limit applied
     */
    protected LakehousePlan applyLimit(LakehousePlan plan, int limit) {
        throw new UnsupportedOperationException("Override applyLimit when using pushLimitRule()");
    }

    /**
     * Get the list of file tasks to read for this plan.
     *
     * <p>Default implementation calls {@link StorageProvider#listObjects} with the
     * plan's {@link LakehousePlan#location() location} parsed as a {@link StoragePath},
     * and wraps each {@link StorageEntry} as a {@link FileTask}.
     *
     * <p>Subclasses can override for custom file discovery (e.g., using an Iceberg
     * catalog to get data files with partition pruning).
     *
     * @param plan The plan (with any filters applied)
     * @return List of file tasks
     */
    protected List<FileTask> getFileTasks(LakehousePlan plan) {
        StoragePath path = StoragePath.of(plan.location());
        List<FileTask> tasks = new ArrayList<>();
        try (StorageIterator iterator = getStorageProvider().listObjects(path)) {
            while (iterator.hasNext()) {
                tasks.add(new StorageFileTask(iterator.next()));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list files for: " + plan.location(), e);
        }
        return tasks;
    }

    /**
     * Create a partition from a group of file tasks.
     *
     * <p>Subclasses return their own {@link DataSourcePartition} implementation carrying
     * the file tasks and any other partition-specific state needed for execution.
     *
     * @param plan The original plan
     * @param tasks The file tasks for this partition
     * @return A DataSourcePartition ready for execution
     */
    protected abstract DataSourcePartition createPartition(LakehousePlan plan, List<FileTask> tasks);

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    /**
     * Combine multiple filter expressions with AND.
     */
    private static Expression combineWithAnd(List<Expression> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }
        Expression result = expressions.get(0);
        for (int i = 1; i < expressions.size(); i++) {
            result = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(result.source(), result, expressions.get(i));
        }
        return result;
    }

    // =========================================================================
    // HELPER TYPES
    // =========================================================================

    /**
     * Adapts a {@link StorageEntry} to the {@link FileTask} interface.
     * Used by the default {@link #getFileTasks} implementation.
     */
    private record StorageFileTask(StorageEntry entry) implements FileTask {
        @Override
        public String path() {
            return entry.path().toString();
        }

        @Override
        public OptionalLong estimatedBytes() {
            return OptionalLong.of(entry.length());
        }

        @Override
        public OptionalLong estimatedRows() {
            return OptionalLong.empty();
        }
    }

    /**
     * Represents a unit of work (typically a file or file segment) to be read.
     *
     * <p>Extends {@link DataSourceSplit} so file tasks can be grouped by
     * {@link SplitPartitioner}. The {@link #estimatedBytes()} and
     * {@link #estimatedRows()} methods are inherited from {@link DataSourceSplit}.
     */
    public interface FileTask extends DataSourceSplit {
        /**
         * Path to the file/resource.
         */
        String path();
    }
}

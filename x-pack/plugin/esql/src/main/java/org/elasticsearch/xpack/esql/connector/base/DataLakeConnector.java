/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.connector.Connector;
import org.elasticsearch.xpack.esql.connector.ConnectorCapabilities;
import org.elasticsearch.xpack.esql.connector.ConnectorPartition;
import org.elasticsearch.xpack.esql.connector.ConnectorPlan;
import org.elasticsearch.xpack.esql.connector.ConnectorSourceDescriptor;
import org.elasticsearch.xpack.esql.connector.DistributionHints;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;
import java.util.OptionalLong;

/**
 * Base class for data lake connectors that read from file-based storage.
 *
 * <h2>Architecture: Storage + Format Separation</h2>
 *
 * <p>This connector composes two pluggable components:
 * <ul>
 *   <li>{@link StorageProvider} — accesses files in a storage system (S3, GCS, HDFS, local)</li>
 *   <li>{@link FormatReader} — reads a file format (Parquet, ORC, CSV, Avro)</li>
 * </ul>
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
 * <p><b>{@link #resolve} implementation:</b> Lists files via {@link StorageProvider#listObjects},
 * infers schema from the first file via {@link FormatReader#inferSchema}, and calls
 * {@link #createPlan} to build the connector-specific plan node. Subclasses with table
 * catalogs (Iceberg, Delta Lake) should override this method.
 *
 * <p><b>Optimization rules:</b> Provides reusable rule classes ({@link #pushFilterRule()},
 * {@link #pushLimitRule()}) that subclasses can include in their
 * {@link #optimizationRules()} override. The base class returns no rules by default
 * since not all data lake formats support filter or limit pushdown.
 *
 * <p><b>{@link #planPartitions} implementation:</b> Lists files via {@link StorageProvider#listObjects},
 * wraps them as {@link FileTask}s, groups them based on {@link DistributionHints#targetPartitions()},
 * and creates {@link ConnectorPartition}s for each group.
 *
 * <h2>Subclass Responsibilities</h2>
 * <ul>
 *   <li>Define a connector-specific {@link DataLakePlan} implementation</li>
 *   <li>{@link #getStorageProvider} — return the storage provider for this connector</li>
 *   <li>{@link #getFormatReader} — return the format reader for this connector</li>
 *   <li>{@link #createPlan} — create the connector-specific plan node</li>
 *   <li>{@link #createPhysicalPlan} — create physical plan node</li>
 *   <li>{@link #createSourceOperator} — create the actual file reader operator</li>
 * </ul>
 *
 * @see DataLakePlan
 * @see StorageProvider
 * @see FormatReader
 */
public abstract class DataLakeConnector implements Connector {

    @Override
    public ConnectorCapabilities capabilities() {
        return ConnectorCapabilities.forDistributed();
    }

    // =========================================================================
    // PHASE 1: RESOLUTION
    // =========================================================================

    /**
     * Default resolution: list files via storage, infer schema from first file via format reader.
     *
     * <p>Subclasses with table catalogs (Iceberg, Delta Lake) should override this method
     * to resolve schema from catalog metadata instead.
     */
    @Override
    public ConnectorPlan resolve(ConnectorSourceDescriptor source, ResolutionContext context) {
        String expression = source.expression();
        StorageProvider storage = getStorageProvider();
        FormatReader format = getFormatReader();

        List<StorageObject> objects = storage.listObjects(expression);
        if (objects.isEmpty()) {
            throw new IllegalArgumentException("No files found matching: " + expression);
        }

        List<Attribute> schema = format.inferSchema(storage, objects.get(0));
        return createPlan(source.describe(), schema, expression, null, null);
    }

    // =========================================================================
    // PHASE 2: LOGICAL OPTIMIZATION
    // =========================================================================

    /**
     * Create a rule that pushes Filter into a DataLakePlan leaf.
     *
     * <p>Subclasses that support filter pushdown should include this in their
     * {@link #optimizationRules()} override and also implement {@link #translateFilter}
     * and {@link #applyFilter}.
     *
     * <p>The rule delegates to {@link #translateFilter} and handles three outcomes:
     * <ul>
     *   <li>Fully translatable: removes Filter node, applies native filter to plan</li>
     *   <li>Partially translatable: keeps Filter with remainder, applies translatable part</li>
     *   <li>Not translatable: leaves plan unchanged (ES|QL evaluates the filter)</li>
     * </ul>
     */
    protected Rule<?, LogicalPlan> pushFilterRule() {
        return new PushFilterToDataLake();
    }

    /**
     * Create a rule that pushes Limit into a DataLakePlan leaf.
     *
     * <p>Subclasses that support limit pushdown should include this in their
     * {@link #optimizationRules()} override and also implement {@link #applyLimit}.
     *
     * <p>The rule absorbs the limit into the connector plan and removes the Limit node.
     */
    protected Rule<?, LogicalPlan> pushLimitRule() {
        return new PushLimitToDataLake();
    }

    private class PushFilterToDataLake extends OptimizerRules.OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.child() instanceof DataLakePlan == false) {
                return filter;
            }
            DataLakePlan lakePlan = (DataLakePlan) filter.child();
            if (lakePlan.connector() != DataLakeConnector.this) {
                return filter;
            }

            FilterTranslation result = translateFilter(filter.condition());

            if (result.isFullyTranslated()) {
                return (LogicalPlan) applyFilter(lakePlan, result.translated());
            } else if (result.isPartiallyTranslated()) {
                DataLakePlan updated = applyFilter(lakePlan, result.translated());
                return new Filter(filter.source(), (LogicalPlan) updated, result.remainder());
            }
            // Not translatable — leave unchanged, ES|QL will evaluate
            return filter;
        }
    }

    private class PushLimitToDataLake extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child() instanceof DataLakePlan == false) {
                return limit;
            }
            DataLakePlan lakePlan = (DataLakePlan) limit.child();
            if (lakePlan.connector() != DataLakeConnector.this) {
                return limit;
            }
            if (limit.limit().foldable() == false) {
                return limit;
            }
            int limitValue = ((Number) limit.limit().fold(org.elasticsearch.xpack.esql.core.expression.FoldContext.small())).intValue();
            DataLakePlan updated = applyLimit(lakePlan, limitValue);
            return (LogicalPlan) updated;
        }
    }

    // =========================================================================
    // PHASE 3: PHYSICAL PLANNING
    // =========================================================================

    // createPhysicalPlan() is inherited from Connector — creates a ConnectorExec
    // wrapping the DataLakePlan with all pushed-down operations (filters, limits).

    // =========================================================================
    // PHASE 3.5: EXECUTION
    // =========================================================================

    /**
     * Create a source operator that reads files via StorageProvider + FormatReader.
     *
     * <p>Default implementation uses the composable storage/format architecture:
     * <ol>
     *   <li>Gets the file list from the partition</li>
     *   <li>For each file, opens it via {@link StorageProvider#open}</li>
     *   <li>Reads data via {@link FormatReader} (which handles format-specific reading)</li>
     *   <li>Produces Pages of columnar data</li>
     * </ol>
     *
     * <p>This is where the storage/format separation pays off: the same operator
     * works for any combination (S3+Parquet, GCS+ORC, local+CSV, etc.).
     *
     * <p>Subclasses must override {@link #createSourceOperator} or provide
     * a concrete {@link SourceOperator} implementation that reads from the
     * storage/format components.
     *
     * <p>A typical implementation would:
     * <ol>
     *   <li>Get the file list from the partition</li>
     *   <li>Open each file via {@code storage.open(storageObject)}</li>
     *   <li>Create a {@link FormatReader.BatchReader} for the file</li>
     *   <li>Read batches from the format reader</li>
     *   <li>Convert format-native data to ES|QL Blocks</li>
     *   <li>Assemble Blocks into Pages</li>
     *   <li>Move to next file when current is exhausted</li>
     * </ol>
     */
    @Override
    public SourceOperator.SourceOperatorFactory createSourceOperator(ConnectorPartition partition, ExecutionContext context) {
        // Subclasses provide their own SourceOperator that reads from storage + format.
        // The operator extends SourceOperator directly — no intermediate abstraction needed.
        //
        // Example:
        // DataLakePlan plan = (DataLakePlan) partition.plan();
        // StorageProvider storage = getStorageProvider();
        // FormatReader format = getFormatReader();
        // String description = "DataLakeSourceOperator[" + type() + ":" + plan.location() + "]";
        //
        // return new SourceOperator.SourceOperatorFactory() {
        // @Override
        // public SourceOperator get(DriverContext driverContext) {
        // return new FileSourceOperator(plan, storage, format, driverContext.blockFactory());
        // }
        // @Override
        // public String describe() { return description; }
        // };

        throw new UnsupportedOperationException("Subclass must override createSourceOperator");
    }

    // =========================================================================
    // PHASE 4: WORK DISTRIBUTION
    // =========================================================================

    /**
     * Called by the physical planner on the coordinator to divide work across data nodes.
     *
     * <p>Default implementation:
     * <ol>
     *   <li>Lists files via {@link #getFileTasks} (which defaults to {@link StorageProvider#listObjects})</li>
     *   <li>Groups files based on {@link DistributionHints#targetPartitions()}</li>
     *   <li>Creates a {@link ConnectorPartition} for each group</li>
     * </ol>
     */
    @Override
    public List<ConnectorPartition> planPartitions(ConnectorPlan plan, DistributionHints hints) {
        if (plan instanceof DataLakePlan == false) {
            return List.of();
        }

        DataLakePlan dataLakePlan = (DataLakePlan) plan;
        List<FileTask> tasks = getFileTasks(dataLakePlan);

        if (tasks.isEmpty()) {
            return List.of();
        }

        // Partition tasks based on target parallelism
        List<List<FileTask>> groups = partitionTasks(tasks, hints.targetPartitions());

        return groups.stream().map(taskGroup -> createPartition(dataLakePlan, taskGroup)).toList();
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
     * schema inference and filter pushdown capabilities.
     */
    protected abstract FormatReader getFormatReader();

    // =========================================================================
    // ABSTRACT METHODS - Plan
    // =========================================================================

    /**
     * Create the connector-specific plan node.
     *
     * <p>Called by {@link #resolve} after schema inference. Subclasses return
     * their specific {@link DataLakePlan} implementation.
     *
     * @param location Source location for error messages
     * @param schema Inferred schema as ES|QL attributes
     * @param expression The original expression from the query
     * @param filter Initial filter, or null
     * @param limit Initial limit, or null
     * @return Connector-specific plan node
     */
    protected abstract DataLakePlan createPlan(
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
     * @param translatedFilter The format-native filter (from {@link #translateFilter})
     * @return Updated plan with filter applied
     */
    protected DataLakePlan applyFilter(DataLakePlan plan, Object translatedFilter) {
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
    protected DataLakePlan applyLimit(DataLakePlan plan, int limit) {
        throw new UnsupportedOperationException("Override applyLimit when using pushLimitRule()");
    }

    /**
     * Get the list of file tasks to read for this plan.
     *
     * <p>Default implementation calls {@link StorageProvider#listObjects} with the
     * plan's {@link DataLakePlan#location() location} and wraps each
     * {@link StorageObject} as a {@link FileTask}.
     *
     * <p>Subclasses can override for custom file discovery (e.g., using an Iceberg
     * catalog to get data files with partition pruning).
     *
     * @param plan The plan (with any filters applied)
     * @return List of file tasks
     */
    protected List<FileTask> getFileTasks(DataLakePlan plan) {
        List<StorageObject> objects = getStorageProvider().listObjects(plan.location());
        return objects.stream().<FileTask>map(StorageFileTask::new).toList();
    }

    /**
     * Create a partition from a group of file tasks.
     *
     * <p>Subclasses return their own {@link ConnectorPartition} implementation carrying
     * the file tasks and any other partition-specific state needed for execution.
     *
     * @param plan The original plan
     * @param tasks The file tasks for this partition
     * @return A ConnectorPartition ready for execution
     */
    protected abstract ConnectorPartition createPartition(DataLakePlan plan, List<FileTask> tasks);

    // =========================================================================
    // FILTER TRANSLATION
    // =========================================================================

    /**
     * Translate an ES|QL filter expression to the source-native filter format.
     *
     * <p>Default implementation delegates to {@link FormatReader#translateFilter}.
     * Subclasses can override for additional logic (e.g., Iceberg partition pruning
     * on top of Parquet row-group filtering).
     *
     * @param filter The ES|QL filter expression
     * @return Translation result indicating success/partial/failure
     */
    protected FilterTranslation translateFilter(Expression filter) {
        return getFormatReader().translateFilter(filter);
    }

    // =========================================================================
    // HELPER METHODS
    // =========================================================================

    /**
     * Partition tasks into groups for parallel execution.
     */
    protected List<List<FileTask>> partitionTasks(List<FileTask> tasks, int targetPartitions) {
        if (tasks.size() <= targetPartitions) {
            // One task per partition
            return tasks.stream().map(List::of).toList();
        }

        // Distribute tasks across partitions
        int partitionCount = Math.min(targetPartitions, tasks.size());
        int tasksPerPartition = (tasks.size() + partitionCount - 1) / partitionCount;

        return java.util.stream.IntStream.range(0, partitionCount).mapToObj(i -> {
            int start = i * tasksPerPartition;
            int end = Math.min(start + tasksPerPartition, tasks.size());
            return tasks.subList(start, end);
        }).filter(list -> list.isEmpty() == false).toList();
    }

    // =========================================================================
    // HELPER TYPES
    // =========================================================================

    /**
     * Result of translating an ES|QL filter to source-native format.
     *
     * @param translated The source-native filter (null if nothing could be translated)
     * @param remainder ES|QL expressions that couldn't be translated (null if all translated)
     */
    public record FilterTranslation(Object translated, Expression remainder) {
        /**
         * All filter conditions were successfully translated.
         */
        public boolean isFullyTranslated() {
            return translated != null && remainder == null;
        }

        /**
         * Some filter conditions were translated, but some remain.
         */
        public boolean isPartiallyTranslated() {
            return translated != null && remainder != null;
        }

        /**
         * No filter conditions could be translated.
         */
        public boolean isNotTranslated() {
            return translated == null;
        }

        /**
         * Create a result for fully translated filter.
         */
        public static FilterTranslation full(Object translated) {
            return new FilterTranslation(translated, null);
        }

        /**
         * Create a result for partially translated filter.
         */
        public static FilterTranslation partial(Object translated, Expression remainder) {
            return new FilterTranslation(translated, remainder);
        }

        /**
         * Create a result for untranslatable filter.
         */
        public static FilterTranslation none() {
            return new FilterTranslation(null, null);
        }
    }

    /**
     * Adapts a {@link StorageObject} to the {@link FileTask} interface.
     * Used by the default {@link #getFileTasks} implementation.
     */
    private record StorageFileTask(StorageObject object) implements FileTask {
        @Override
        public String path() {
            return object.path();
        }

        @Override
        public OptionalLong estimatedBytes() {
            return object.size();
        }

        @Override
        public OptionalLong estimatedRows() {
            return OptionalLong.empty();
        }
    }

    /**
     * Represents a unit of work (typically a file or file segment) to be read.
     */
    public interface FileTask {
        /**
         * Path to the file/resource.
         */
        String path();

        /**
         * Estimated size in bytes.
         */
        OptionalLong estimatedBytes();

        /**
         * Estimated number of rows.
         */
        OptionalLong estimatedRows();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasource.lakehouse.ExternalSourceOperatorFactory;
import org.elasticsearch.xpack.esql.datasource.lakehouse.LakehouseRegistry;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourceDescriptor;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePushdownRule;
import org.elasticsearch.xpack.esql.datasource.spi.partitioning.DataSourceSplit;
import org.elasticsearch.xpack.esql.datasource.spi.partitioning.DistributionHints;
import org.elasticsearch.xpack.esql.datasource.spi.partitioning.SplitPartitioner;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Concrete data source for lakehouse (file-based) storage.
 *
 * <h2>Architecture: Registry-Driven Composition</h2>
 *
 * <p>This class composes its behavior from pluggable components discovered at startup
 * and held in a {@link LakehouseRegistry}:
 * <ul>
 *   <li>{@link StorageProvider} — accessed via
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProviderRegistry}, keyed by URI scheme (s3, gcs, file)</li>
 *   <li>{@link FormatReader} — accessed via
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.FormatReaderRegistry}, keyed by format name or file extension</li>
 *   <li>{@link TableCatalog} — (future) for catalog-managed sources (Iceberg, Delta Lake, Hudi)</li>
 * </ul>
 *
 * <p>No subclassing is needed. All variation is handled by the registered plugins:
 * <ul>
 *   <li>S3 + Parquet — StoragePlugin for "s3" + FormatPlugin for "parquet"</li>
 *   <li>GCS + ORC — StoragePlugin for "gcs" + FormatPlugin for "orc"</li>
 *   <li>Local FS + CSV — StoragePlugin for "file" + FormatPlugin for "csv"</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 *
 * <p>Per-data-source configuration is provided via {@link DataSourceDescriptor#configuration()}:
 * <ul>
 *   <li>{@code "format"} — explicit format name (optional; inferred from file extension if absent)</li>
 *   <li>Storage-specific keys (bucket, credentials, endpoint) — passed to
 *       {@link org.elasticsearch.xpack.esql.datasource.lakehouse.StorageProviderRegistry#createProvider}</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Instances are created per data source registration by {@link org.elasticsearch.xpack.esql.datasource.spi.DataSourceFactory}.
 * The {@link LakehouseRegistry} (shared across all instances) provides access to the
 * storage/format/catalog registries.
 *
 * @see LakehouseRegistry
 * @see LakehousePlan
 * @see StorageProvider
 * @see FormatReader
 */
public final class LakehouseDataSource implements DataSource {

    private static final Logger logger = LogManager.getLogger(LakehouseDataSource.class);

    private final String type;
    private final LakehouseRegistry registry;
    private final Map<String, Object> configuration;
    private final SplitPartitioner<FileTask> partitioner;

    /**
     * Create a lakehouse data source.
     *
     * @param type the data source type identifier (e.g., "lakehouse")
     * @param registry shared lakehouse registry with storage/format/catalog plugins
     * @param configuration per-data-source configuration (credentials, format, etc.)
     */
    public LakehouseDataSource(String type, LakehouseRegistry registry, Map<String, Object> configuration) {
        this.type = Objects.requireNonNull(type, "type");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.configuration = configuration != null ? configuration : Map.of();
        this.partitioner = new SplitPartitioner<>(this::discoverFileTasks, this::wrapPartition);
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public DataSourceCapabilities capabilities() {
        return DataSourceCapabilities.forDistributed();
    }

    // =========================================================================
    // PHASE 1: RESOLUTION
    // =========================================================================

    @Override
    public void resolve(DataSourceDescriptor source, ResolutionContext context, ActionListener<DataSourcePlan> listener) {
        context.executor().execute(() -> {
            try {
                DataSourcePlan plan = resolveSync(source);
                listener.onResponse(plan);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private DataSourcePlan resolveSync(DataSourceDescriptor source) {
        String expression = source.expression();

        // Look up storage provider by URI scheme
        StoragePath path = StoragePath.of(expression);
        StorageProvider storage = resolveStorageProvider(path);

        // Look up format reader (explicit from config, or inferred from extension)
        FormatReader format = resolveFormatReader(source, expression);
        logger.debug("Resolving [{}] via [{}] format reader on [{}] storage", expression, format.formatName(), path.scheme());

        try {
            StorageObject object = storage.newObject(path);
            SourceMetadata metadata = format.metadata(object);
            logger.debug("Inferred schema with [{}] columns from [{}]", metadata.schema().size(), path);
            return new LakehousePlan(Source.EMPTY, this, source.describe(), metadata.schema(), expression, format.formatName(), null, null);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read metadata for: " + expression, e);
        }
    }

    // =========================================================================
    // PHASE 2: LOGICAL OPTIMIZATION
    // =========================================================================

    @Override
    public List<Rule<?, LogicalPlan>> optimizationRules() {
        return List.of(new PushLimitToLakehouse());
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
            int limitValue = ((Number) limit.limit().fold(FoldContext.small())).intValue();
            return lakePlan.withLimit(limitValue);
        }
    }

    // =========================================================================
    // PHASE 3: PHYSICAL PLANNING
    // =========================================================================

    // Uses default from DataSource — creates DataSourceExec wrapping the LakehousePlan.

    // =========================================================================
    // PHASE 4: WORK DISTRIBUTION
    // =========================================================================

    @Override
    public List<DataSourcePartition> planPartitions(DataSourcePlan plan, DistributionHints hints) {
        return partitioner.planPartitions(plan, hints);
    }

    private List<FileTask> discoverFileTasks(DataSourcePlan plan) {
        if (plan instanceof LakehousePlan == false) {
            return List.of();
        }
        LakehousePlan lakePlan = (LakehousePlan) plan;

        StoragePath path = StoragePath.of(lakePlan.expression());
        StorageProvider storage = resolveStorageProvider(path);
        List<FileTask> tasks = new ArrayList<>();
        try (StorageIterator iterator = storage.listObjects(path, true)) {
            while (iterator.hasNext()) {
                tasks.add(new StorageFileTask(iterator.next()));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list files for: " + lakePlan.location(), e);
        }

        if (tasks.isEmpty()) {
            logger.debug("No file tasks for [{}], returning empty partitions", lakePlan.location());
        }
        return tasks;
    }

    private DataSourcePartition wrapPartition(DataSourcePlan plan, List<FileTask> splits) {
        return new LakehousePartition((LakehousePlan) plan, splits);
    }

    // =========================================================================
    // PHASE 5: EXECUTION
    // =========================================================================

    @Override
    public SourceOperator.SourceOperatorFactory createSourceOperator(DataSourcePartition partition, ExecutionContext context) {
        LakehousePartition lakePart = (LakehousePartition) partition;
        LakehousePlan plan = lakePart.lakehousePlan();

        StoragePath path = StoragePath.of(plan.expression());
        StorageProvider storage = resolveStorageProvider(path);
        FormatReader format = registry.formatReaderRegistry().byName(plan.formatName());

        return new ExternalSourceOperatorFactory(storage, format, path, plan.output(), 1000);
    }

    // =========================================================================
    // REGISTRY LOOKUPS
    // =========================================================================

    private StorageProvider resolveStorageProvider(StoragePath path) {
        return registry.storageProviderRegistry().createProvider(path.scheme(), registry.settings(), configuration);
    }

    private FormatReader resolveFormatReader(DataSourceDescriptor source, String expression) {
        // Explicit format from configuration takes priority
        Object formatConfig = source.config("format", null);
        if (formatConfig == null) {
            formatConfig = configuration.get("format");
        }
        if (formatConfig instanceof String formatName) {
            return registry.formatReaderRegistry().byName(formatName);
        }
        // Infer from file extension
        return registry.formatReaderRegistry().byExtension(expression);
    }

    // =========================================================================
    // INNER TYPES
    // =========================================================================

    /**
     * A partition carrying a list of file tasks for parallel execution.
     */
    public static final class LakehousePartition implements DataSourcePartition {
        private final LakehousePlan plan;
        private final List<FileTask> tasks;

        public LakehousePartition(LakehousePlan plan, List<FileTask> tasks) {
            this.plan = plan;
            this.tasks = tasks;
        }

        @Override
        public DataSourcePlan plan() {
            return plan;
        }

        /** Typed access to the lakehouse plan. */
        public LakehousePlan lakehousePlan() {
            return plan;
        }

        public List<FileTask> tasks() {
            return tasks;
        }

        @Override
        public String getWriteableName() {
            return "esql.partition.lakehouse";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("Serialization not yet implemented for lakehouse partitions");
        }
    }

    /**
     * A unit of work representing a single file to read.
     */
    public interface FileTask extends DataSourceSplit {
        /**
         * Path to the file/resource.
         */
        String path();
    }

    /**
     * Adapts a {@link StorageEntry} to the {@link FileTask} interface.
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
}

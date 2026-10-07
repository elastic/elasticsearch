/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalLimitSplits;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Context passed to {@link SplitProvider#discoverSplits} containing all information
 * needed to enumerate and optionally prune splits for an external source.
 *
 * @param querySchema the post-prune Query schema (data attributes only, metadata stripped) the
 *        query actually materializes. Empty means either all columns are needed or the projection
 *        is unknown. Split providers use {@link ExternalSchema#names()} for membership tests when pruning
 *        per-file mappings.
 * @param unifiedSchema the pre-prune Unified schema, or {@code null} when not available. Together
 *        with {@code querySchema} and each file's schema this lets split providers narrow per-file
 *        {@link org.elasticsearch.xpack.esql.datasources.ColumnMapping}s on the coordinator.
 * @param isCancelled polled during split discovery so a long-running enumeration (e.g. thousands of
 *        Parquet footer reads) aborts promptly when the originating query is cancelled. Defaults to
 *        {@code () -> false} ("never cancelled") for callers and SPI impls that do not carry a
 *        {@code CancellableTask}.
 * @param metadataColumnNames names bound to engine-generated metadata in the resolved output,
 *        not data columns that happen to share a metadata name. This binding is relation-wide
 *        and must not be reinterpreted based on each file's physical schema.
 * @param retainedPartitionKeys keys to keep on each survivor's partition map after filter
 *        evaluation. {@code null} means the projection is unknown, so hive values plus
 *        {@code _file.size} and {@code _file.modified} are kept. {@code _file.path},
 *        {@code _file.name}, and {@code _file.directory} are never stored. A non-null set,
 *        including empty, is authoritative, except those three location keys are still dropped.
 *        {@link org.elasticsearch.xpack.esql.datasources.ExternalSchema#EMPTY} does not imply an
 *        empty set: an empty schema means "do not narrow the file read", not "keep nothing".
 */
public record SplitDiscoveryContext(
    SourceMetadata metadata,
    FileList fileList,
    Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
    Map<String, Object> config,
    PartitionMetadata partitionInfo,
    List<Expression> filterHints,
    ExternalSchema querySchema,
    @Nullable ExternalSchema unifiedSchema,
    int maxRecordBytes,
    BooleanSupplier isCancelled,
    // The declared read-instructions (renames / declared-type columns / date formats). Lets split discovery make the
    // declared overlay a stats boundary — rekey physical->logical + poison retyped columns' footer stats. NONE when the
    // dataset carries no declared mapping (every current SplitProvider but FileSplitProvider ignores it).
    DeclaredReadSpec declaredReadSpec,
    Set<String> metadataColumnNames,
    @Nullable Set<String> retainedPartitionKeys,
    // How many rows the query needs from this relation, or FormatReader.NO_LIMIT when the commands above it make no
    // promise about that count. A provider may use it to stop producing splits once the demand is covered; one that
    // ignores it produces them all, which every provider but FileSplitProvider does.
    int rowLimit,
    // Reserves heap for the listing this query performs when the schema's listing was a prefix of the dataset.
    // Null when nothing is accounting for the query (tests, and providers reached outside a query). Live like
    // isCancelled rather than data: it draws on the query's own reservation and must not outlive it.
    @Nullable PlanningMemory listingMemory,
    // Drivers the planner will start for this query ({@code task_concurrency}). Discovery sizes LIMIT cuts
    // from this so a single-driver LIMIT does not probe. Zero or negative means the search-pool default.
    int taskConcurrency
) {
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints
    ) {
        this(
            metadata,
            fileList,
            Map.of(),
            config,
            partitionInfo,
            filterHints,
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE
        );
    }

    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema
    ) {
        this(
            metadata,
            fileList,
            Map.of(),
            config,
            partitionInfo,
            filterHints,
            querySchema,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE
        );
    }

    /**
     * The same context over the query's own file set: what a provider resolved the files to be, when the listing
     * it was handed answered the schema rather than the scan. Every reader downstream takes the file set from the
     * context, so replacing it once here is what keeps them all on the same answer.
     * <p>
     * Two things were derived per file from the listing this replaces, and both move with it. Partition values
     * key off the path, so a file the old listing never saw had no value for any partition column and read as
     * null on all of them; the columns themselves stay as resolution decided them, because the plan's attributes
     * carry that answer already ({@link PartitionMetadata#valuedOver}). Per-file schema info keys off the path
     * too, and a file with no entry is read under its own schema rather than the dataset's
     * ({@link SchemaReconciliation#pinnedOver}).
     */
    public SplitDiscoveryContext withScanFileSet(FileList resolved) {
        return new SplitDiscoveryContext(
            metadata,
            resolved,
            SchemaReconciliation.pinnedOver(schemaMap, resolved),
            config,
            partitionInfo == null ? null : partitionInfo.valuedOver(resolved, PartitionConfig.fromConfig(config)),
            filterHints,
            querySchema,
            unifiedSchema,
            maxRecordBytes,
            isCancelled,
            declaredReadSpec,
            metadataColumnNames,
            retainedPartitionKeys,
            rowLimit,
            listingMemory,
            taskConcurrency
        );
    }

    /** Without a row demand: the shape every caller had before a limit could reach split discovery. */
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema,
        @Nullable ExternalSchema unifiedSchema,
        int maxRecordBytes,
        BooleanSupplier isCancelled,
        DeclaredReadSpec declaredReadSpec,
        Set<String> metadataColumnNames,
        @Nullable Set<String> retainedPartitionKeys
    ) {
        this(
            metadata,
            fileList,
            schemaMap,
            config,
            partitionInfo,
            filterHints,
            querySchema,
            unifiedSchema,
            maxRecordBytes,
            isCancelled,
            declaredReadSpec,
            metadataColumnNames,
            retainedPartitionKeys,
            FormatReader.NO_LIMIT,
            null,
            0
        );
    }

    /** Row demand without a query-pragma driver cap; discovery uses the search-pool default. */
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema,
        @Nullable ExternalSchema unifiedSchema,
        int maxRecordBytes,
        BooleanSupplier isCancelled,
        DeclaredReadSpec declaredReadSpec,
        Set<String> metadataColumnNames,
        @Nullable Set<String> retainedPartitionKeys,
        int rowLimit,
        @Nullable PlanningMemory listingMemory
    ) {
        this(
            metadata,
            fileList,
            schemaMap,
            config,
            partitionInfo,
            filterHints,
            querySchema,
            unifiedSchema,
            maxRecordBytes,
            isCancelled,
            declaredReadSpec,
            metadataColumnNames,
            retainedPartitionKeys,
            rowLimit,
            listingMemory,
            0
        );
    }

    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema
    ) {
        this(metadata, fileList, schemaMap, config, partitionInfo, filterHints, querySchema, Set.of());
    }

    /**
     * Carries resolved metadata bindings without requiring file-splitting or schema-reconciliation options.
     */
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema,
        Set<String> metadataColumnNames
    ) {
        this(
            metadata,
            fileList,
            schemaMap,
            config,
            partitionInfo,
            filterHints,
            querySchema,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE,
            metadataColumnNames,
            null
        );
    }

    /**
     * Builds a context for a relation with no engine-generated metadata columns.
     */
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        ExternalSchema querySchema,
        @Nullable ExternalSchema unifiedSchema,
        int maxRecordBytes,
        BooleanSupplier isCancelled,
        DeclaredReadSpec declaredReadSpec
    ) {
        this(
            metadata,
            fileList,
            schemaMap,
            config,
            partitionInfo,
            filterHints,
            querySchema,
            unifiedSchema,
            maxRecordBytes,
            isCancelled,
            declaredReadSpec,
            Set.of(),
            null
        );
    }

    public SplitDiscoveryContext {
        if (fileList == null) {
            throw new IllegalArgumentException("fileList cannot be null");
        }
        schemaMap = schemaMap != null ? schemaMap : Map.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        filterHints = filterHints != null ? List.copyOf(filterHints) : List.of();
        querySchema = querySchema != null ? querySchema : ExternalSchema.EMPTY;
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive, got: " + maxRecordBytes);
        }
        isCancelled = isCancelled != null ? isCancelled : () -> false;
        declaredReadSpec = declaredReadSpec != null ? declaredReadSpec : DeclaredReadSpec.NONE;
        metadataColumnNames = Set.copyOf(metadataColumnNames);
        // null stays null: unknown projection keeps hive, size, and modified. Location keys are dropped
        // when the survivor map is frozen. A provided set is authoritative aside from those three keys.
        retainedPartitionKeys = retainedPartitionKeys == null ? null : Set.copyOf(retainedPartitionKeys);
        if (taskConcurrency <= 0) {
            taskConcurrency = ExternalLimitSplits.DEFAULT_TASK_CONCURRENCY;
        }
    }
}

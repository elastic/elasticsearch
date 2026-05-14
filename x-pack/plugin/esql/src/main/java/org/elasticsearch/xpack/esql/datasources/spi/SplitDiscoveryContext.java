/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.Schema;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;

import java.util.List;
import java.util.Map;

/**
 * Context passed to {@link SplitProvider#discoverSplits} containing all information
 * needed to enumerate and optionally prune splits for an external source.
 *
 * @param querySchema the post-prune Query schema (data attributes only, metadata stripped) the
 *        query actually materializes. Empty means either all columns are needed or the projection
 *        is unknown. Split providers use {@link Schema#names()} for membership tests when pruning
 *        per-file mappings or skipping files with no column overlap.
 * @param unifiedSchema the pre-prune Unified schema, or {@code null} when not available. Together
 *        with {@code querySchema} and each file's schema this lets split providers narrow per-file
 *        {@link org.elasticsearch.xpack.esql.datasources.ColumnMapping}s on the coordinator.
 */
public record SplitDiscoveryContext(
    SourceMetadata metadata,
    FileList fileList,
    Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
    Map<String, Object> config,
    PartitionMetadata partitionInfo,
    List<Expression> filterHints,
    Schema querySchema,
    @Nullable Schema unifiedSchema
) {
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints
    ) {
        this(metadata, fileList, Map.of(), config, partitionInfo, filterHints, Schema.EMPTY, null);
    }

    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        Schema querySchema
    ) {
        this(metadata, fileList, Map.of(), config, partitionInfo, filterHints, querySchema, null);
    }

    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        Schema querySchema
    ) {
        this(metadata, fileList, schemaMap, config, partitionInfo, filterHints, querySchema, null);
    }

    public SplitDiscoveryContext {
        if (fileList == null) {
            throw new IllegalArgumentException("fileList cannot be null");
        }
        schemaMap = schemaMap != null ? schemaMap : Map.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        filterHints = filterHints != null ? List.copyOf(filterHints) : List.of();
        querySchema = querySchema != null ? querySchema : Schema.EMPTY;
    }
}

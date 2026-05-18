/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context passed to {@link SplitProvider#discoverSplits} containing all information
 * needed to enumerate and optionally prune splits for an external source.
 *
 * @param projectedDataColumns names of data columns (excluding metadata attributes) that
 *        the query actually needs. When non-empty, split providers can skip files whose
 *        schema has zero overlap with this set (UNION_BY_NAME optimisation).
 *        Empty means either all columns are needed or the set is unknown.
 */
public record SplitDiscoveryContext(
    SourceMetadata metadata,
    FileList fileList,
    Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
    Map<String, Object> config,
    PartitionMetadata partitionInfo,
    List<Expression> filterHints,
    Set<String> projectedDataColumns
) {
    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints
    ) {
        this(metadata, fileList, Map.of(), config, partitionInfo, filterHints, Set.of());
    }

    public SplitDiscoveryContext(
        SourceMetadata metadata,
        FileList fileList,
        Map<String, Object> config,
        PartitionMetadata partitionInfo,
        List<Expression> filterHints,
        Set<String> projectedDataColumns
    ) {
        this(metadata, fileList, Map.of(), config, partitionInfo, filterHints, projectedDataColumns);
    }

    public SplitDiscoveryContext {
        if (fileList == null) {
            throw new IllegalArgumentException("fileList cannot be null");
        }
        schemaMap = schemaMap != null ? schemaMap : Map.of();
        config = config != null ? Map.copyOf(config) : Map.of();
        filterHints = filterHints != null ? List.copyOf(filterHints) : List.of();
        projectedDataColumns = projectedDataColumns != null ? Set.copyOf(projectedDataColumns) : Set.of();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;

import java.util.List;
import java.util.Map;

/**
 * Context passed to {@link SplitProvider#discoverSplits} containing all information
 * needed to enumerate and optionally prune splits for an external source.
 */
public record SplitDiscoveryContext(
    SourceMetadata metadata,
    FileSet fileSet,
    Map<String, Object> config,
    PartitionMetadata partitionInfo,
    List<Expression> filterHints
) {
    public SplitDiscoveryContext {
        if (fileSet == null) {
            throw new IllegalArgumentException("fileSet cannot be null");
        }
        config = config != null ? Map.copyOf(config) : Map.of();
        filterHints = filterHints != null ? List.copyOf(filterHints) : List.of();
    }
}

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
 *
 * <p>Constructed in PR 2 (Split Discovery Phase) by {@code SplitDiscoveryPhase}.
 *
 * <p>{@code filterHints} is typed as {@code List<Expression>} to keep the SPI decoupled from
 * the extractor. PR 2 will populate it with resolved expressions for L1 partition pruning
 * inside {@code FileSplitProvider}; the current placeholder type will be reconciled then.
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

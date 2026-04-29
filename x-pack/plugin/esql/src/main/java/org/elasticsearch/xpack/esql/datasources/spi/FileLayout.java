/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;

/**
 * Aggregate result of a single-pass file layout resolution: the file's
 * {@link SourceMetadata} (schema + statistics) plus its
 * {@link RangeAwareFormatReader.SplitRange split ranges} (e.g. Parquet row groups
 * or ORC stripes), captured in one pass over the file's footer/metadata.
 * <p>
 * For columnar formats this avoids reading the footer twice (once for metadata
 * resolution, once for split discovery). For non-columnar or non-range-aware
 * sources, {@code splitRanges} is an empty list.
 */
public record FileLayout(SourceMetadata metadata, List<RangeAwareFormatReader.SplitRange> splitRanges) {

    public FileLayout {
        if (metadata == null) {
            throw new IllegalArgumentException("metadata must not be null");
        }
        splitRanges = splitRanges != null ? List.copyOf(splitRanges) : List.of();
    }

    /**
     * Convenience factory for callers that have only metadata (no pre-resolved ranges).
     */
    public static FileLayout ofMetadata(SourceMetadata metadata) {
        return new FileLayout(metadata, List.of());
    }
}

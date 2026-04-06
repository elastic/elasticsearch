/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;

import java.util.Map;

/**
 * Registry for filter pushdown support implementations.
 *
 * <p>This registry provides a single entry point for looking up filter pushdown
 * support implementations by source type. It is populated by {@link DataSourceModule}
 * from all registered {@link org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin}s.
 *
 * <p>The registry is used by the optimizer's {@code PushFiltersToSource} rule to
 * determine if and how filters can be pushed down to external data sources.
 */
public class FilterPushdownRegistry {

    private final Map<String, FilterPushdownSupport> pushdownSupport;

    public FilterPushdownRegistry(Map<String, FilterPushdownSupport> pushdownSupport) {
        this.pushdownSupport = pushdownSupport != null ? Map.copyOf(pushdownSupport) : Map.of();
    }

    public FilterPushdownSupport get(String sourceType) {
        return sourceType != null ? pushdownSupport.get(sourceType) : null;
    }

    /**
     * Look up pushdown support by source type with format-based fallback.
     * <p>
     * File-based sources all share source type "file" (from FileSourceFactory). To enable
     * per-format pushdown (e.g., ORC, Parquet), this method first checks the source type
     * and falls back to the format name if no direct match is found.
     *
     * @param sourceType the source type (e.g., "file", "iceberg", "flight")
     * @param formatName the format name (e.g., "orc", "parquet"), may be null
     * @return the pushdown support, or null if none registered
     */
    public FilterPushdownSupport get(String sourceType, String formatName) {
        FilterPushdownSupport support = get(sourceType);
        if (support == null && formatName != null) {
            support = pushdownSupport.get(formatName);
        }
        return support;
    }

    public boolean hasSupport(String sourceType) {
        return sourceType != null && pushdownSupport.containsKey(sourceType);
    }

    public static FilterPushdownRegistry empty() {
        return new FilterPushdownRegistry(Map.of());
    }
}

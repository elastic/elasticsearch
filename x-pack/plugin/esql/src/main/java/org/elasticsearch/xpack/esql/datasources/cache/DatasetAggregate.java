/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.Map;

/**
 * The memoized fold for one resolved file set: a row count, and nothing else.
 * <p>
 * Row-count-only is a contract rather than a current limitation. Serving a per-column extremum from a
 * dataset-wide fold would require every file's statistics to have been normalised to one resolution, which is
 * exactly what the per-file rail exists to decide; a count needs no such agreement. So this type cannot carry a
 * column statistic, which the shape it replaces could - it was a schema-record value with four empty arrays and
 * a metadata map, and nothing stopped a per-file enrichment landing in that map.
 * <p>
 * The weight is a constant: one object header and one {@code long}. The shape it replaces also stored a source
 * type and a location that no reader ever retrieved.
 */
public record DatasetAggregate(long rowCount) {

    /** The serve-path form: the flat statistics map a plan consumes, carrying only the row count. */
    public Map<String, Object> asStatistics() {
        return Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
    }
}

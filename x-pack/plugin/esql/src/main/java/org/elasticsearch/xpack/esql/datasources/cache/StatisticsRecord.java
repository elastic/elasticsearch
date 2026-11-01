/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;

import java.util.Map;

/**
 * What one read measured about one file: a flat map of {@code _stats.*} keys, and nothing about the file's
 * shape.
 * <p>
 * <b>It holds no column names and no column types.</b> That is the whole point of the type. A statistics record
 * used to be a {@link SchemaCacheEntry} built as {@code schemaRecord.withSafeMetadata(...)}, so it carried the
 * schema record's four parallel arrays — a resolution that belonged to a DIFFERENT read. Every coercion that
 * touched it normalised this read's extrema against that other read's types, and because one store held both
 * kinds of fact under one key type, each consumer had to re-discriminate which it was holding. Two of them
 * disagreed, which was a shipped defect. There is nothing to discriminate now: a consumer handed one of these
 * cannot ask it for a type, because it has none.
 * <p>
 * The flat map stays the storage form rather than becoming typed fields, because
 * {@code SourceStatisticsSerializer}, {@code SplitStats} and the whole-file fold all speak it, and re-typing
 * the column map is a separate change from separating the stores. The map also carries the identity keys
 * contribution matching compares on ({@code _stats.file_mtime_millis}, {@code _stats.config_fingerprint}) and
 * the read stamp, exactly as it did before, so what rides the wire to the plan is unchanged.
 * <p>
 * Weight is precomputed at construction. The shared {@code Cache} calls a weigher twice for every hit that is
 * not already at the LRU head — {@code promote} relinks through {@code unlink} and {@code linkAtHead} — so a
 * weigher that walked this map would walk it twice per warm hit.
 */
public final class StatisticsRecord {

    private final Map<String, Object> measurements;
    private final long estimatedBytes;

    public StatisticsRecord(Map<String, Object> measurements) {
        this.measurements = measurements == null ? Map.of() : Map.copyOf(measurements);
        // 64B shell, the figure SchemaCacheEntry uses for an object header plus reference fields, plus the
        // map itself. Nothing else is retained: no arrays, no warnings, no connector config.
        this.estimatedBytes = 64L + HeapEstimates.mapBytes(this.measurements);
    }

    /** The measurements, as the flat {@code _stats.*} map every consumer of statistics already speaks. */
    public Map<String, Object> measurements() {
        return measurements;
    }

    public long estimatedBytes() {
        return estimatedBytes;
    }

    @Override
    public String toString() {
        return "StatisticsRecord[" + measurements.size() + " keys, " + estimatedBytes + "B]";
    }
}

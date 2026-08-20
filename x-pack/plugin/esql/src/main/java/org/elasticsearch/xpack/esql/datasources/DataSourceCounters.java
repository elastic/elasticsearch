/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceUsageAccumulator;

/**
 * Converts the values accumulated in a {@link DataSourceUsageAccumulator} into flat
 * {@link Counters} keys under the {@code datasources.} subtree of the ES|QL XPack usage payload.
 * <p>
 * Key naming follows the instrument names in {@code ExternalSourceMetrics}, with attribute values
 * (scheme, outcome) flattened into the key path. The resulting nested map (via
 * {@link Counters#toNestedMap()}) becomes the {@code esql.datasources} section of
 * {@code GET /_xpack/usage}.
 */
public final class DataSourceCounters {

    private DataSourceCounters() {}

    /**
     * Reads all accumulated values from {@code acc} and writes them into {@code counters} under
     * the {@code datasources.} prefix. Intended to be called once per node from
     * {@code TransportEsqlStatsAction.nodeOperation}.
     */
    public static void populate(DataSourceUsageAccumulator acc, Counters counters) {
        // ---- per-scheme counters ----
        for (int i = 0; i < DataSourceUsageAccumulator.SCHEME_COUNT; i++) {
            String s = DataSourceUsageAccumulator.SCHEME_NAMES[i];
            counters.inc("datasources.storage.requests.total." + s, acc.storageRequests(i));
            counters.inc("datasources.storage.bytes_read.total." + s, acc.storageBytesRead(i));
            counters.inc("datasources.storage.errors.total." + s, acc.storageErrors(i));
            counters.inc("datasources.storage.throttled.total." + s, acc.storageThrottled(i));
        }

        // ---- unattributed counters ----
        counters.inc("datasources.storage.retries.total", acc.storageRetries());
        // datasources.queries.cancelled.total mirrors the APM QUERIES_CANCELLED_TOTAL instrument (a dedicated
        // cancelled counter). datasources.queries.total.cancelled (populated below) mirrors QUERIES_TOTAL
        // attributed to the cancelled outcome. The two are intentionally separate keys — do not sum them.
        counters.inc("datasources.queries.cancelled.total", acc.queriesCancelled());
        counters.inc("datasources.queries.partial.total", acc.queriesPartial());
        counters.inc("datasources.discovery.failures.total", acc.discoveryFailures());
        counters.inc("datasources.parse.rows.total", acc.parseRows());
        counters.inc("datasources.reader.pool.rejected.total", acc.readerPoolRejected());
        counters.inc("datasources.breaker.tripped.total", acc.breakerTripped());

        // ---- per-outcome query counters ----
        for (int i = 0; i < DataSourceUsageAccumulator.OUTCOME_COUNT; i++) {
            counters.inc("datasources.queries.total." + DataSourceUsageAccumulator.OUTCOME_NAMES[i], acc.queries(i));
        }

        // ---- time histograms (no scheme attribute) ----
        String[] ts = DataSourceUsageAccumulator.TIME_SUFFIXES;
        for (int b = 0; b < DataSourceUsageAccumulator.BUCKET_COUNT; b++) {
            counters.inc("datasources.storage.requests.duration." + ts[b], acc.storageRequestDuration(b));
            counters.inc("datasources.storage.read_stall.duration." + ts[b], acc.storageReadStallDuration(b));
            counters.inc("datasources.queries.duration." + ts[b], acc.queryDuration(b));
            counters.inc("datasources.queries.time_to_first_row." + ts[b], acc.queryTimeToFirstRow(b));
            counters.inc("datasources.discovery.duration." + ts[b], acc.discoveryDuration(b));
            counters.inc("datasources.parse.duration." + ts[b], acc.parseDuration(b));
        }

        // ---- count histograms ----
        String[] cs = DataSourceUsageAccumulator.COUNT_SUFFIXES;
        for (int b = 0; b < DataSourceUsageAccumulator.BUCKET_COUNT; b++) {
            counters.inc("datasources.discovery.files_scanned." + cs[b], acc.discoveryFilesScanned(b));
            counters.inc("datasources.parse.splits_scanned." + cs[b], acc.parseSplitsScanned(b));
        }

        // ---- bytes histogram ----
        String[] bs = DataSourceUsageAccumulator.BYTES_SUFFIXES;
        for (int b = 0; b < DataSourceUsageAccumulator.BUCKET_COUNT; b++) {
            counters.inc("datasources.discovery.bytes_scanned." + bs[b], acc.discoveryBytesScanned(b));
        }
    }
}

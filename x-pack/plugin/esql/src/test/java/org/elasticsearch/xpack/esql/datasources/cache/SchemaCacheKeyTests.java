/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

/**
 * Locks the identity contract of {@link SchemaCacheKey#forDatasetAggregate}: the key must change with
 * the listing content token, the format-affecting config, and the source type — and must be
 * structurally distinct from every per-file key so the per-file reconcile/lookup paths can never
 * touch a dataset-aggregate entry.
 */
public class SchemaCacheKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";

    public void testDatasetAggregateKeyStableForSameInputs() {
        SchemaCacheKey a = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("format", "ndjson"));
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("format", "ndjson"));
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithEitherTokenLane() {
        SchemaCacheKey base = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of());
        assertNotEquals(base, SchemaCacheKey.forDatasetAggregate(PATTERN, 12L, 22L, "ndjson", Map.of()));
        assertNotEquals(base, SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 23L, "ndjson", Map.of()));
    }

    public void testDatasetAggregateKeyChangesWithFormatAffectingConfig() {
        // error_mode changes which rows survive a scan, so a count harvested under one policy must
        // never serve a query running under another — the config fingerprint is part of the key.
        SchemaCacheKey strict = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("error_mode", "fail_fast"));
        SchemaCacheKey lenient = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("error_mode", "skip_row"));
        assertNotEquals(strict, lenient);
    }

    public void testDatasetAggregateKeyIgnoresCredentials() {
        // Mirrors buildFormatConfig: credentials are not row-interpretation-affecting, so two users
        // over the same files share the aggregate (the schema cache is shared by design).
        SchemaCacheKey a = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("access_key", "userA"));
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of("access_key", "userB"));
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithSourceType() {
        SchemaCacheKey ndjson = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of());
        SchemaCacheKey csv = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "csv", Map.of());
        assertNotEquals(ndjson, csv);
    }

    public void testDatasetAggregateKeyDistinctFromPerFileKeys() {
        // Even a per-file key crafted over the same strings cannot equal a dataset key: the dataset
        // formatType carries the reserved '#' marker suffix (extension detection derives formatType
        // from a file name's last dot and so never emits '#'), and the content token rides the two
        // dedicated datasetTokenH1/H2 lanes, which every per-file key zeroes. canonicalPath stays the
        // plain glob pattern (diagnostics-friendly, no smuggled separators).
        SchemaCacheKey dataset = SchemaCacheKey.forDatasetAggregate(PATTERN, 11L, 22L, "ndjson", Map.of());
        SchemaCacheKey perFile = SchemaCacheKey.build(PATTERN, 11L, "ndjson", Map.of());
        assertNotEquals(dataset, perFile);
        assertTrue(dataset.formatType().endsWith(SchemaCacheKey.DATASET_AGGREGATE_MARKER));
        assertEquals(PATTERN, dataset.canonicalPath());
        assertEquals(11L, dataset.datasetTokenH1());
        assertEquals(22L, dataset.datasetTokenH2());
        assertEquals(0L, perFile.datasetTokenH1());
        assertEquals(0L, perFile.datasetTokenH2());
    }
}

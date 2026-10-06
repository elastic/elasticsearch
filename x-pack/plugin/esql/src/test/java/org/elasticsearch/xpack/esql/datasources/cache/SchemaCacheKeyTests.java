/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.util.Map;
import java.util.Set;

/**
 * Locks the identity contract of {@link SchemaCacheKey#forDatasetAggregate}: the key must change with
 * the listing's file-set fingerprint, the format-affecting config, and the source type — and must be
 * structurally distinct from every per-file key so the per-file reconcile/lookup paths can never
 * touch a dataset-aggregate entry.
 */
public class SchemaCacheKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";

    public void testDatasetAggregateKeyStableForSameInputs() {
        SchemaCacheKey a = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            "",
            Map.of("format", "ndjson")
        );
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            "",
            Map.of("format", "ndjson")
        );
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithEitherFingerprintLane() {
        SchemaCacheKey base = SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 22), "ndjson", "", Map.of());
        assertNotEquals(base, SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(12, 22), "ndjson", "", Map.of()));
        assertNotEquals(base, SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 23), "ndjson", "", Map.of()));
    }

    public void testDatasetAggregateKeyIgnoresCredentials() {
        // A storage identity names only non-secret fields: credentials are not row-interpretation-affecting, so two users
        // over the same files share the aggregate (the schema cache is shared by design).
        SchemaCacheKey a = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            "",
            Map.of("access_key", "userA")
        );
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            "",
            Map.of("access_key", "userB")
        );
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithSourceType() {
        SchemaCacheKey ndjson = SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 22), "ndjson", "", Map.of());
        SchemaCacheKey csv = SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 22), "csv", "", Map.of());
        assertNotEquals(ndjson, csv);
    }

    public void testDatasetAggregateKeyChangesWithRegion() {
        // region is a dataset-level key; two identical file sets accessed with different regions
        // must not share the same aggregate cache entry.
        SchemaCacheKey usEast = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            Configured.identityOf(Map.of("region", "us-east-1"), Set.of("region")),
            Map.of()
        );
        SchemaCacheKey euWest = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            "ndjson",
            Configured.identityOf(Map.of("region", "eu-west-1"), Set.of("region")),
            Map.of()
        );
        assertNotEquals(usEast, euWest);
    }

    public void testPerFileKeyChangesWithRegion() {
        // region is a dataset-level key; the same file at the same mtime on different regions
        // must not share a per-file schema cache entry.
        String usEastIdentity = Configured.identityOf(Map.of("region", "us-east-1"), Set.of("region"));
        String euWestIdentity = Configured.identityOf(Map.of("region", "eu-west-1"), Set.of("region"));
        SchemaCacheKey usEast = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, "parquet", usEastIdentity, Map.of());
        SchemaCacheKey euWest = SchemaCacheKey.build("s3://bucket/file.parquet", 1000L, "parquet", euWestIdentity, Map.of());
        assertNotEquals(usEast, euWest);
    }

    public void testDatasetAggregateKeyDistinctFromPerFileKeys() {
        // Even a per-file key crafted over the same strings cannot equal a dataset key: the file-set
        // fingerprint rides the dedicated fileSetFingerprint component, which every per-file key leaves
        // null (so a pathological '#dataset-agg'-bearing object name at most loses warm enrichment, never
        // collides). canonicalPath stays the plain glob pattern (diagnostics-friendly, no smuggled
        // separators).
        SchemaCacheKey dataset = SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 22), "ndjson", "", Map.of());
        SchemaCacheKey perFile = SchemaCacheKey.build(PATTERN, 11L, "ndjson", "", Map.of());
        assertNotEquals(dataset, perFile);
        assertTrue(dataset.isDatasetAggregate());
        assertFalse(perFile.isDatasetAggregate());
        assertEquals(PATTERN, dataset.canonicalPath());
        assertEquals(new FileSetFingerprint(11, 22), dataset.fileSetFingerprint());
        assertNull(perFile.fileSetFingerprint());
    }

    /**
     * A statistics record is addressed by the read that produced it, so two reads of one file that resolved
     * different schemas must not share an address — and must not collide with the schema record beside them.
     * This is the discrimination the refusal in {@code applicableStats} exists to do today; once the address
     * carries the read, there is nothing left to compare.
     */
    public void testStatisticsAddressDiscriminatesTheReadThatProducedIt() {
        SchemaCacheKey schema = SchemaCacheKey.build(PATTERN, 11L, "ndjson", "", Map.of());

        SchemaCacheKey readAsFileOwn = schema.withReadConfig("aaaa1111");
        SchemaCacheKey readAsAnchor = schema.withReadConfig("bbbb2222");

        // Two reads, two addresses. On main both harvests contend for the schema key and the second is refused.
        assertNotEquals(readAsFileOwn, readAsAnchor);
        // Neither collides with the schema record it sits beside.
        assertNotEquals(schema, readAsFileOwn);
        assertNotEquals(schema, readAsAnchor);

        assertTrue(readAsFileOwn.isStatisticsRecord());
        assertFalse(schema.isStatisticsRecord());
        assertEquals("aaaa1111", readAsFileOwn.readConfig());
        assertNull(schema.readConfig());
    }

    /**
     * Everything but the read is carried across, so a statistics record can never drift from the schema record
     * it belongs to — same file, same version, same format, same participants.
     */
    public void testStatisticsAddressCarriesEveryOtherComponent() {
        SchemaCacheKey dataset = SchemaCacheKey.forDatasetAggregate(PATTERN, new FileSetFingerprint(11, 22), "ndjson", "id", Map.of());
        SchemaCacheKey stats = dataset.withReadConfig("cccc3333");

        // Pin that a key was actually derived first. Without these two the carrying assertions below hold
        // trivially when withReadConfig returns its receiver, and the test passes whether or not it works.
        assertNotSame(dataset, stats);
        assertEquals("cccc3333", stats.readConfig());

        assertEquals(dataset.canonicalPath(), stats.canonicalPath());
        assertEquals(dataset.lastModifiedEpochMillis(), stats.lastModifiedEpochMillis());
        assertEquals(dataset.formatType(), stats.formatType());
        assertEquals(dataset.identity(), stats.identity());
        assertEquals(dataset.fileSetFingerprint(), stats.fileSetFingerprint());
        assertEquals(dataset.definitionVersion(), stats.definitionVersion());
        // The record kind survives the derivation, so isDatasetAggregate() keeps answering for the aggregate's
        // own statistics record rather than silently becoming a per-file one.
        assertTrue(stats.isDatasetAggregate());
    }

    /**
     * A rail that stamps no read configuration gets the address it has. An address asserting a read nobody
     * recorded would claim more than the harvest does, which is the mistake this whole area is about.
     */
    public void testAnUnstampedReadKeepsTheAddressItHas() {
        SchemaCacheKey schema = SchemaCacheKey.build(PATTERN, 11L, "ndjson", "", Map.of());
        assertSame(schema, schema.withReadConfig(null));
        assertSame(schema, schema.withReadConfig(""));
        assertFalse(schema.withReadConfig(null).isStatisticsRecord());
    }
}

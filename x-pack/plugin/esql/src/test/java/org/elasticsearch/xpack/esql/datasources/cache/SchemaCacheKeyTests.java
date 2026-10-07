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
            TestDatasetIdentities.identity("ndjson", "", Map.of("format", "ndjson"))
        );
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of("format", "ndjson"))
        );
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithEitherFingerprintLane() {
        SchemaCacheKey base = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        assertNotEquals(
            base,
            SchemaCacheKey.forDatasetAggregate(
                PATTERN,
                new FileSetFingerprint(12, 22),
                TestDatasetIdentities.identity("ndjson", "", Map.of())
            )
        );
        assertNotEquals(
            base,
            SchemaCacheKey.forDatasetAggregate(
                PATTERN,
                new FileSetFingerprint(11, 23),
                TestDatasetIdentities.identity("ndjson", "", Map.of())
            )
        );
    }

    /**
     * Two data sources differing only in their credentials must not share an address. This reverses what this
     * suite previously pinned - that they DO share one, on the reasoning that credentials are not
     * row-interpretation-affecting and so two users over the same files may share the aggregate. That reasoning
     * is sound about interpretation and answers a different question than the one that matters: a row count and a
     * column extremum are facts about the data, not interpretations of it, and the listing and footer-byte stores
     * already separate principals for exactly that reason.
     * <p>
     * It is a second layer of defence and not the authorization control. It cannot see a principal who may list
     * but not read within ONE data source, it cannot see a revoked credential, which digests to the value it had
     * while it was valid, and it cannot see a federated token, which arrives at read time and belongs to no
     * definition. An authorization check on the resolve path is the control.
     * <p>
     * It also costs sharing that is legitimately correct, since S3 authorizes per object and two data sources
     * over the same files hold facts equally true for both. That trade is deliberate.
     */
    public void testDatasetAggregateKeySeparatesPrincipals() {
        SchemaCacheKey a = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userA-secret", Map.of())
        );
        SchemaCacheKey b = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userB-secret", Map.of())
        );
        assertNotEquals("two data sources differing only in their credentials must not share an aggregate", a, b);
        assertEquals(
            "and two resolves under the same credentials must still share it",
            a,
            SchemaCacheKey.forDatasetAggregate(
                PATTERN,
                new FileSetFingerprint(11, 22),
                TestDatasetIdentities.identity("ndjson", "", "digest-of-userA-secret", Map.of())
            )
        );
    }

    /**
     * The strict-declared rail stores a different answer about the same bytes than the inferred rail does: its
     * record holds the DECLARED schema, where the inferred record holds what inference produced. If the two share
     * one address the loser is served the other's schema, which is a wrong answer and not a miss.
     * <p>
     * This separation used to be carried by a marker suffixed onto the format name and tested with
     * {@code endsWith}; it is now a compared component. Neither form had a test in this suite that could fail,
     * which is what this is.
     */
    public void testStrictDeclaredRecordDoesNotShareTheInferredAddress() {
        DatasetIdentity identity = TestDatasetIdentities.identity("csv", "endpoint=a", Map.of());
        SchemaCacheKey inferred = SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, false);
        SchemaCacheKey strict = SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, true);
        assertNotEquals(
            "the strict-declared record holds the declared schema and the inferred record holds the inferred one; "
                + "sharing one address serves one of them the other's schema",
            inferred,
            strict
        );
        assertEquals("and the rail is the only thing that differs", inferred, SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, false));
        assertEquals(strict, SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, true));
    }

    /** The rail must survive the derivation to a statistics address, or a strict harvest lands on the inferred record. */
    public void testTheRailSurvivesTheStatisticsDerivation() {
        DatasetIdentity identity = TestDatasetIdentities.identity("csv", "endpoint=a", Map.of());
        SchemaCacheKey strictStats = SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, true).withReadConfig("aaaa1111");
        SchemaCacheKey inferredStats = SchemaCacheKey.build("s3://b/f.csv", 1000L, identity, false).withReadConfig("aaaa1111");
        assertTrue(strictStats.declaredStrict());
        assertFalse(inferredStats.declaredStrict());
        assertNotEquals(strictStats, inferredStats);
    }

    /** The same separation on the per-file rail, where the schema and the per-column extrema live. */
    public void testPerFileKeySeparatesPrincipals() {
        SchemaCacheKey a = SchemaCacheKey.build(
            "s3://bucket/data/a.ndjson",
            1000L,
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userA-secret", Map.of()),
            false
        );
        SchemaCacheKey b = SchemaCacheKey.build(
            "s3://bucket/data/a.ndjson",
            1000L,
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userB-secret", Map.of()),
            false
        );
        assertNotEquals("one file read under two credential sets must not share one schema address", a, b);
    }

    /**
     * An auth mode with no stored secret yields an empty digest. That must behave as a value and not as a
     * wildcard, or every such data source would collapse onto one address with every other.
     */
    public void testAnAbsentSecretIsStillAnIdentity() {
        SchemaCacheKey none = SchemaCacheKey.build(
            "s3://b/f.ndjson",
            1L,
            TestDatasetIdentities.identity("ndjson", "", "", Map.of()),
            false
        );
        SchemaCacheKey some = SchemaCacheKey.build(
            "s3://b/f.ndjson",
            1L,
            TestDatasetIdentities.identity("ndjson", "", "digest", Map.of()),
            false
        );
        assertNotEquals(none, some);
        assertEquals(none, SchemaCacheKey.build("s3://b/f.ndjson", 1L, TestDatasetIdentities.identity("ndjson", "", "", Map.of()), false));
    }

    public void testDatasetAggregateKeyChangesWithTheReaderIdentity() {
        SchemaCacheKey ndjson = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        SchemaCacheKey csv = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("csv", "", Map.of())
        );
        assertNotEquals(ndjson, csv);
    }

    public void testDatasetAggregateKeyChangesWithRegion() {
        // region is a dataset-level key; two identical file sets accessed with different regions
        // must not share the same aggregate cache entry.
        SchemaCacheKey usEast = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", Configured.identityOf(Map.of("region", "us-east-1"), Set.of("region")), Map.of())
        );
        SchemaCacheKey euWest = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", Configured.identityOf(Map.of("region", "eu-west-1"), Set.of("region")), Map.of())
        );
        assertNotEquals(usEast, euWest);
    }

    public void testPerFileKeyChangesWithRegion() {
        // region is a dataset-level key; the same file at the same mtime on different regions
        // must not share a per-file schema cache entry.
        String usEastIdentity = Configured.identityOf(Map.of("region", "us-east-1"), Set.of("region"));
        String euWestIdentity = Configured.identityOf(Map.of("region", "eu-west-1"), Set.of("region"));
        SchemaCacheKey usEast = SchemaCacheKey.build(
            "s3://bucket/file.parquet",
            1000L,
            TestDatasetIdentities.identity("parquet", usEastIdentity, Map.of()),
            false
        );
        SchemaCacheKey euWest = SchemaCacheKey.build(
            "s3://bucket/file.parquet",
            1000L,
            TestDatasetIdentities.identity("parquet", euWestIdentity, Map.of()),
            false
        );
        assertNotEquals(usEast, euWest);
    }

    public void testDatasetAggregateKeyDistinctFromPerFileKeys() {
        // A per-file key cannot equal a dataset key: the file-set fingerprint rides its own component, which
        // every per-file key leaves null. canonicalPath stays the plain glob pattern, for diagnostics.
        SchemaCacheKey dataset = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        SchemaCacheKey perFile = SchemaCacheKey.build(PATTERN, 11L, TestDatasetIdentities.identity("ndjson", "", Map.of()), false);
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
        SchemaCacheKey schema = SchemaCacheKey.build(PATTERN, 11L, TestDatasetIdentities.identity("ndjson", "", Map.of()), false);

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
        SchemaCacheKey dataset = SchemaCacheKey.forDatasetAggregate(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "id", Map.of())
        );
        SchemaCacheKey stats = dataset.withReadConfig("cccc3333");

        // Pin that a key was actually derived first. Without these two the carrying assertions below hold
        // trivially when withReadConfig returns its receiver, and the test passes whether or not it works.
        assertNotSame(dataset, stats);
        assertEquals("cccc3333", stats.readConfig());

        assertEquals(dataset.canonicalPath(), stats.canonicalPath());
        assertEquals(dataset.lastModifiedEpochMillis(), stats.lastModifiedEpochMillis());
        assertEquals(dataset.dataset(), stats.dataset());
        assertEquals(dataset.fileSetFingerprint(), stats.fileSetFingerprint());
        assertEquals(dataset.declaredStrict(), stats.declaredStrict());
        // The record kind survives the derivation, so isDatasetAggregate() keeps answering for the aggregate's
        // own statistics record rather than silently becoming a per-file one.
        assertTrue(stats.isDatasetAggregate());
    }

    /**
     * A rail that stamps no read configuration gets the address it has. An address asserting a read nobody
     * recorded would claim more than the harvest does, which is the mistake this whole area is about.
     */
    public void testAnUnstampedReadKeepsTheAddressItHas() {
        SchemaCacheKey schema = SchemaCacheKey.build(PATTERN, 11L, TestDatasetIdentities.identity("ndjson", "", Map.of()), false);
        assertSame(schema, schema.withReadConfig(null));
        assertSame(schema, schema.withReadConfig(""));
        assertFalse(schema.withReadConfig(null).isStatisticsRecord());
    }
}

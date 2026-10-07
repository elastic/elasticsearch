/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;

import java.util.Map;
import java.util.Set;

/**
 * Locks the identity contract of {@link SchemaCacheKey}, the per-file address: it must change with the
 * file's path and mtime, with the dataset identity behind it, with the declared-strict rail, and with the
 * read a statistics record was harvested under. The dataset-level fold is a different type in a different
 * store — see {@link DatasetAggregateKeyTests}.
 */
public class SchemaCacheKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";

    /** A realistic per-file identity: a folded participant string, a definition version and a secret digest. */
    private static DatasetIdentity identity() {
        return DatasetIdentity.of(
            "9f86d081884c7d65",
            "2c26b46b68ffc68f",
            "s3|eu-west-1|bucket",
            "csv|sep=,|header=true",
            "fail_fast:0:0.0|first_file_wins"
        );
    }

    /**
     * The key's footprint, measured and pinned EXACTLY. Shrinking the key is the point of this shape: a schema
     * record's key holds a dataset identity, a path reference, an mtime and a flag, and the identity is six
     * {@code long}s and an int behind one reference rather than a set of strings rebuilt per key.
     * <p>
     * Exact equality, not a ceiling, and that is deliberate. A ceiling does not discriminate: with compressed
     * references a component added back to the key costs 4 bytes, so a 6-component key at 40B becomes 44B and
     * pads to 48B — under any ceiling loose enough to be safe. Exact equality fails on any added field, which is
     * the regression this exists to catch.
     * <p>
     * The figures are layout-specific, so the layout is asserted rather than assumed: the case is skipped unless
     * compressed references are on, which is the default for the test JVM. {@code shallowSizeOf} reads the class
     * layout, so these are real figures, unlike {@code sizeOfObject}, which cannot introspect a plain object and
     * returns {@code UNKNOWN_DEFAULT_RAM_BYTES_USED} — a 256-byte constant that reads exactly like a measurement.
     * <p>
     * The path string is deliberately excluded. It dominates the retained graph and is not this change's to
     * shrink; what this pins is the part that is.
     */
    public void testThePerFileKeyFootprintStaysSmall() {
        assumeTrue("footprint figures are pinned for the compressed-reference layout", RamUsageEstimator.COMPRESSED_REFS_ENABLED);

        DatasetIdentity identity = identity();
        SchemaCacheKey key = SchemaCacheKey.build("s3://bucket/data/part-00000.csv", 1730000000000L, identity, false);

        long identityShallow = RamUsageEstimator.shallowSizeOf(identity);
        long keyShallow = RamUsageEstimator.shallowSizeOf(key);

        // Header + six longs + the cached hash. A nested object or a retained string makes this 72.
        assertEquals("DatasetIdentity must stay six longs and an int behind one reference", 64L, identityShallow);
        // Header + one identity reference + a long + a boolean + two nullable references. A seventh component
        // makes this 48.
        assertEquals("the per-file key must stay at six components", 40L, keyShallow);
        assertEquals("and the pair is what replaced three retained strings", 104L, keyShallow + identityShallow);
    }

    /**
     * A schema record's key retains no read-configuration string. Only a statistics record is addressed by its
     * read, so that is the one kind of key that pays for the extra component, and it pays for it in the
     * reference the key already has rather than in a wider object.
     */
    public void testASchemaRecordKeyRetainsNoReadConfiguration() {
        SchemaCacheKey schema = SchemaCacheKey.build("s3://bucket/data/a.csv", 1L, identity(), false);
        assertNull("a schema record is not addressed by a read", schema.readConfig());

        SchemaCacheKey stats = schema.withReadConfig("0123456789abcdef0123456789abcdef");
        assertNotNull("a statistics record is", stats.readConfig());
        assertEquals(
            "and it costs no extra field to say so",
            RamUsageEstimator.shallowSizeOf(schema),
            RamUsageEstimator.shallowSizeOf(stats)
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
     * it belongs to — same file, same mtime, same dataset identity, same rail.
     * <p>
     * It is derived from a PER-FILE key, and that is now the only thing it can be derived from. This case used
     * to build a dataset-aggregate key and derive a statistics address from it, asserting that the aggregate
     * kind "survived the derivation" — an address that was reachable, meaningless, and written by nothing. A
     * dataset fold is a {@link DatasetAggregateKey} now, which has no {@code withReadConfig}, so the state does
     * not exist to assert about.
     */
    public void testStatisticsAddressCarriesEveryOtherComponent() {
        SchemaCacheKey file = SchemaCacheKey.build(PATTERN, 11L, TestDatasetIdentities.identity("ndjson", "id", Map.of()), true);
        SchemaCacheKey stats = file.withReadConfig("cccc3333");

        // Pin that a key was actually derived first. Without these two the carrying assertions below hold
        // trivially when withReadConfig returns its receiver, and the test passes whether or not it works.
        assertNotSame(file, stats);
        assertEquals("cccc3333", stats.readConfig());

        assertEquals(file.canonicalPath(), stats.canonicalPath());
        assertEquals(file.lastModifiedEpochMillis(), stats.lastModifiedEpochMillis());
        assertEquals(file.dataset(), stats.dataset());
        // The rail survives the derivation: a strict-declared record's statistics stay on the strict rail
        // rather than silently becoming the inferred record's.
        assertTrue("the declared-strict rail must survive", stats.declaredStrict());
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

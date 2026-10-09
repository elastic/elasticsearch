/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;

/**
 * Locks the identity contract of {@link StatisticsKey}: the address of what ONE read measured about one file.
 * <p>
 * Composed from the file's own address rather than copying its components, so a measurement can never drift
 * from the file it is about. These cases used to live in {@code SchemaCacheKeyTests}, because the per-file key
 * carried a nullable read component and a {@code withReadConfig} derivation; it carries neither now, so a
 * schema address cannot express a measurement at all.
 */
public class StatisticsKeyTests extends ESTestCase {

    private static final String PATH = "s3://bucket/data/part-00000.csv";

    private static DatasetIdentity identity() {
        return DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1");
    }

    private static SchemaCacheKey file(boolean declaredStrict) {
        return SchemaCacheKey.build(PATH, 11L, identity(), declaredStrict);
    }

    /**
     * The read is part of the address. A statistic measures the rows one read produced, and which rows a read
     * produces depends on the schema it was handed, so two reads of one file that resolved different schemas
     * measured different things and must not share an address.
     */
    public void testTheReadDiscriminatesTheAddress() {
        StatisticsKey underOwn = StatisticsKey.of(file(false), "aaaa1111");
        StatisticsKey underAnchor = StatisticsKey.of(file(false), "bbbb2222");
        assertNotEquals(underOwn, underAnchor);
        assertEquals("aaaa1111", underOwn.readConfig());
        assertEquals(StatisticsKey.of(file(false), "aaaa1111"), underOwn);
    }

    /**
     * Everything but the read comes from the file's address, by composition rather than by copy, so the two
     * cannot drift: same path, same mtime, same dataset identity, same rail.
     */
    public void testTheFileAddressIsCarriedWhole() {
        SchemaCacheKey f = file(false);
        StatisticsKey key = StatisticsKey.of(f, "cccc3333");
        assertSame("composed, not copied", f, key.file());
    }

    /**
     * The declared-strict rail survives the derivation: a strict record's measurements are not the inferred
     * record's, over the same file and the same read. It comes for free, because the file address carries it.
     */
    public void testTheStrictRailSurvivesTheDerivation() {
        StatisticsKey strict = StatisticsKey.of(file(true), "aaaa1111");
        StatisticsKey inferred = StatisticsKey.of(file(false), "aaaa1111");
        assertNotEquals("two rails over one file and one read are two addresses", strict, inferred);
        assertTrue(strict.file().declaredStrict());
        assertFalse(inferred.file().declaredStrict());
    }

    /**
     * A read that recorded no configuration gets the one shared unstamped address rather than no address.
     * <p>
     * Sharing is correct for them: two reads that recorded nothing are indistinguishable, so there is nothing
     * an address could separate, and it is what they already did when every unstamped measurement landed on
     * the one schema record. Refusing them an address would drop a real measurement — columnar never stamps,
     * and a text read of a file with no schema to describe resolves to {@link ReadConfigFingerprint#UNKNOWN} —
     * and take those rails cold.
     */
    public void testAnUnrecordedReadSharesTheUnstampedAddress() {
        StatisticsKey fromNull = StatisticsKey.of(file(false), null);
        StatisticsKey fromEmpty = StatisticsKey.of(file(false), ReadConfigFingerprint.UNKNOWN);
        assertEquals("null and empty are the same absence", fromNull, fromEmpty);
        assertEquals(StatisticsKey.UNSTAMPED, fromNull.readConfig());
        // And it cannot collide with a real fingerprint: ReadConfigFingerprint renders 32 hex characters or
        // the empty string, so a sentinel of letters is unreachable as a derived value.
        assertNotEquals(32, StatisticsKey.UNSTAMPED.length());
        assertNotEquals(fromNull, StatisticsKey.of(file(false), "0123456789abcdef0123456789abcdef"));
    }

    /**
     * The address is small, measured rather than reasoned about. A header, the file reference and the read
     * reference: the file's components are reached through it rather than duplicated into it.
     * <p>
     * {@link RamUsageEstimator#shallowSizeOf} because {@code sizeOfObject} cannot introspect a plain object and
     * returns a 256-byte constant that reads exactly like a measurement.
     */
    public void testTheAddressIsSmall() {
        assumeTrue("figures are pinned for the compressed-reference layout", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        assertEquals("a header and two references", 24L, RamUsageEstimator.shallowSizeOf(StatisticsKey.of(file(false), "aaaa1111")));
    }

    /** A read is required: an address asserting a file but no read would claim more than any harvest does. */
    public void testTheFileIsRequired() {
        expectThrows(NullPointerException.class, () -> StatisticsKey.of(null, "aaaa1111"));
    }
}

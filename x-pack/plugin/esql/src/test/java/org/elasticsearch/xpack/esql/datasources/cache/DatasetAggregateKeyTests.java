/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

/**
 * Locks the identity contract of {@link DatasetAggregateKey}. The fingerprint makes the key correct-or-miss over
 * the bytes; the pattern separates two globs that resolve to one file set; the owner separates two datasets. What
 * a definition version itself folds is {@code DefinitionVersion.ofDataset}'s contract, pinned field by field in
 * {@code DefinitionVersionTests}.
 * <p>
 * It carries no read configuration: one owner over one file set performs one read, so there is nothing for one to
 * separate at this tier.
 */
public class DatasetAggregateKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";
    private static final String VERSION = "0123456789abcdef0123456789abcdef";

    private static DatasetIdentity identity() {
        return DatasetIdentity.of("defv1", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1");
    }

    public void testDatasetAggregateKeyStableForSameInputs() {
        assertEquals(
            DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION)
        );
    }

    public void testDatasetAggregateKeyChangesWithEitherFingerprintLane() {
        DatasetAggregateKey base = DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION);
        assertNotEquals(base, DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(12, 22), VERSION));
        assertNotEquals(base, DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 23), VERSION));
    }

    public void testDatasetAggregateKeyChangesWithThePattern() {
        assertNotEquals(
            "two globs that happen to resolve to one file set are two datasets",
            DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.ofDefinition("s3://bucket/data/2026-*.ndjson", new FileSetFingerprint(11, 22), VERSION)
        );
    }

    /**
     * The invalidation protocol, in one case: an edited definition derives a different version, so the fold
     * measured under the previous one is unreachable and ages out. Nothing has to notice the edit and tell the
     * cache about it, which is why every component of a definition has to reach the version - see
     * {@code DefinitionVersionTests}.
     */
    public void testDatasetAggregateKeyChangesWithTheDatasetVersion() {
        assertNotEquals(
            DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), "fedcba9876543210fedcba9876543210")
        );
    }

    /**
     * A query with no stored definition still gets this tier, owned by its resolved identity. That is the tier
     * which survives per-file eviction, so refusing it an address would make every warm {@code COUNT(*)} over a
     * glob re-scan once the per-file records aged out.
     */
    public void testAQueryWithNoDefinitionIsOwnedByItsIdentity() {
        FileSetFingerprint fileSet = new FileSetFingerprint(11, 22);
        assertEquals(
            DatasetAggregateKey.ofIdentity(PATTERN, fileSet, identity()),
            DatasetAggregateKey.ofIdentity(PATTERN, fileSet, identity())
        );
        assertNotEquals(
            "a different identity owns a different fold",
            DatasetAggregateKey.ofIdentity(PATTERN, fileSet, identity()),
            DatasetAggregateKey.ofIdentity(
                PATTERN,
                fileSet,
                DatasetIdentity.of("defv2", "secretdigest", "s3|eu-west-1", "csv|sep=,", "coord1")
            )
        );
    }

    /**
     * The two owner spaces must not meet. A definition version and an identity are both derived values, and an
     * entry measured under one must never be served to the other - which a single nullable field, or an owner
     * rendered to a bare string, could let happen.
     */
    public void testADefinitionOwnedFoldAndAnIdentityOwnedFoldNeverShareAnAddress() {
        FileSetFingerprint fileSet = new FileSetFingerprint(11, 22);
        assertNotEquals(
            DatasetAggregateKey.ofDefinition(PATTERN, fileSet, VERSION),
            DatasetAggregateKey.ofIdentity(PATTERN, fileSet, identity())
        );
    }

    /** Neither owner may be absent: an unowned entry is one nothing can invalidate. */
    public void testAnAddressAlwaysHasAnOwner() {
        FileSetFingerprint fileSet = new FileSetFingerprint(11, 22);
        expectThrows(IllegalArgumentException.class, () -> DatasetAggregateKey.ofDefinition(PATTERN, fileSet, null));
        expectThrows(IllegalArgumentException.class, () -> DatasetAggregateKey.ofDefinition(PATTERN, fileSet, ""));
        expectThrows(NullPointerException.class, () -> DatasetAggregateKey.ofIdentity(PATTERN, fileSet, null));
        expectThrows(NullPointerException.class, () -> DatasetAggregateKey.ofDefinition(PATTERN, null, VERSION));
    }

    public void testDatasetAggregateKeyKeepsWhatItWasBuiltFrom() {
        DatasetAggregateKey key = DatasetAggregateKey.ofDefinition(PATTERN, new FileSetFingerprint(11, 22), VERSION);
        assertEquals(PATTERN, key.pattern());
        assertEquals(new FileSetFingerprint(11, 22), key.fileSet());
        assertEquals(new DatasetAggregateKey.ByDefinition(VERSION), key.owner());
    }
}

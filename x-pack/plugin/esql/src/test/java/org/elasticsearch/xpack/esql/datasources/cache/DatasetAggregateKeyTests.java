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
 * Locks the identity contract of {@link DatasetAggregateKey}: the address of a memoized multi-file fold must
 * change with the listing's file-set fingerprint, with the pattern it was resolved from, and with the version of
 * the dataset definition it belongs to.
 * <p>
 * The three components are the whole address, and each is here for a different reason. The fingerprint makes the
 * key correct-or-miss over the bytes: any file added, removed or modified derives a different key and the stale
 * fold ages out with no invalidation protocol. The pattern separates two globs that happen to resolve to one
 * file set. The version separates two dataset definitions, and carries every edit to either definition - that is
 * {@code DefinitionVersion.ofDataset}'s contract, pinned field by field in {@code DefinitionVersionTests}
 * rather than restated here, because what the version folds is that method's business and not this address's.
 * <p>
 * What this type no longer carries is a read configuration. One dataset definition over one file set performs
 * one read, so there is nothing for a read configuration to separate at this tier - and the properties that
 * reasoning used to be needed for (two principals, two regions, two readers must not share a fold) hold now
 * because each of those is an edit to a definition, and so moves the version. They are pinned where they are
 * decided.
 * <p>
 * It also cannot be mistaken for a per-file address, and that is a property of the type rather than something
 * asserted here: {@link DatasetAggregateKey} and {@link SchemaCacheKey} are different types, so the per-file
 * reconcile and lookup paths cannot be handed one.
 */
public class DatasetAggregateKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";
    private static final String VERSION = "0123456789abcdef0123456789abcdef";

    public void testDatasetAggregateKeyStableForSameInputs() {
        assertEquals(
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION)
        );
    }

    public void testDatasetAggregateKeyChangesWithEitherFingerprintLane() {
        DatasetAggregateKey base = DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION);
        assertNotEquals(base, DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(12, 22), VERSION));
        assertNotEquals(base, DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 23), VERSION));
    }

    public void testDatasetAggregateKeyChangesWithThePattern() {
        assertNotEquals(
            "two globs that happen to resolve to one file set are two datasets",
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.of("s3://bucket/data/2026-*.ndjson", new FileSetFingerprint(11, 22), VERSION)
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
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION),
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), "fedcba9876543210fedcba9876543210")
        );
    }

    /**
     * A fold with no definition behind it has nothing to invalidate it, so it must not be addressable at all.
     * The resolver already refuses to mint one for a bare {@code FROM} over a URI; this makes the address itself
     * refuse, so a future caller cannot reintroduce a shared slot by passing an empty version.
     */
    public void testADatasetAggregateAddressRequiresADefinition() {
        FileSetFingerprint fileSet = new FileSetFingerprint(11, 22);
        expectThrows(IllegalArgumentException.class, () -> DatasetAggregateKey.of(PATTERN, fileSet, null));
        expectThrows(IllegalArgumentException.class, () -> DatasetAggregateKey.of(PATTERN, fileSet, ""));
        expectThrows(NullPointerException.class, () -> DatasetAggregateKey.of(PATTERN, null, VERSION));
    }

    public void testDatasetAggregateKeyKeepsWhatItWasBuiltFrom() {
        DatasetAggregateKey key = DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 22), VERSION);
        assertEquals(PATTERN, key.pattern());
        assertEquals(new FileSetFingerprint(11, 22), key.fileSet());
        assertEquals(VERSION, key.datasetVersion());
    }
}

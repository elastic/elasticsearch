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
 * Locks the identity contract of {@link DatasetAggregateKey}: the address of a memoized multi-file fold must
 * change with the listing's file-set fingerprint, with the dataset identity behind it, and with the pattern it
 * was resolved from.
 * <p>
 * It can no longer be mistaken for a per-file address, and that is a property of the type rather than something
 * asserted here: {@link DatasetAggregateKey} and {@link SchemaCacheKey} are different types, so the per-file
 * reconcile and lookup paths cannot be handed one. The case that used to assert {@code isDatasetAggregate()} on
 * one key and not the other has nothing left to check.
 */
public class DatasetAggregateKeyTests extends ESTestCase {

    private static final String PATTERN = "s3://bucket/data/*.ndjson";

    public void testDatasetAggregateKeyStableForSameInputs() {
        DatasetAggregateKey a = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of("format", "ndjson"))
        );
        DatasetAggregateKey b = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of("format", "ndjson"))
        );
        assertEquals(a, b);
    }

    public void testDatasetAggregateKeyChangesWithEitherFingerprintLane() {
        DatasetAggregateKey base = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        assertNotEquals(
            base,
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(12, 22), TestDatasetIdentities.identity("ndjson", "", Map.of()))
        );
        assertNotEquals(
            base,
            DatasetAggregateKey.of(PATTERN, new FileSetFingerprint(11, 23), TestDatasetIdentities.identity("ndjson", "", Map.of()))
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
        DatasetAggregateKey a = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userA-secret", Map.of())
        );
        DatasetAggregateKey b = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", "digest-of-userB-secret", Map.of())
        );
        assertNotEquals("two data sources differing only in their credentials must not share an aggregate", a, b);
        assertEquals(
            "and two resolves under the same credentials must still share it",
            a,
            DatasetAggregateKey.of(
                PATTERN,
                new FileSetFingerprint(11, 22),
                TestDatasetIdentities.identity("ndjson", "", "digest-of-userA-secret", Map.of())
            )
        );
    }

    public void testDatasetAggregateKeyChangesWithTheReaderIdentity() {
        DatasetAggregateKey ndjson = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        DatasetAggregateKey csv = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("csv", "", Map.of())
        );
        assertNotEquals(ndjson, csv);
    }

    public void testDatasetAggregateKeyChangesWithRegion() {
        // region is a dataset-level key; two identical file sets accessed with different regions
        // must not share the same aggregate cache entry.
        DatasetAggregateKey usEast = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", Configured.identityOf(Map.of("region", "us-east-1"), Set.of("region")), Map.of())
        );
        DatasetAggregateKey euWest = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", Configured.identityOf(Map.of("region", "eu-west-1"), Set.of("region")), Map.of())
        );
        assertNotEquals(usEast, euWest);
    }

    public void testDatasetAggregateKeyDistinctFromPerFileKeys() {
        // A per-file key cannot equal a dataset key: the file-set fingerprint rides its own component, which
        // every per-file key leaves null. location stays the plain glob pattern, for diagnostics.
        DatasetAggregateKey dataset = DatasetAggregateKey.of(
            PATTERN,
            new FileSetFingerprint(11, 22),
            TestDatasetIdentities.identity("ndjson", "", Map.of())
        );
        // Nothing left to assert about telling the two apart: a DatasetAggregateKey and a SchemaCacheKey are
        // different types, so no consumer can be handed the wrong one and no flag or nullable component encodes
        // the distinction. What remains worth pinning is that the address keeps what it was built from.
        assertEquals(PATTERN, dataset.pattern());
        assertEquals(new FileSetFingerprint(11, 22), dataset.fileSet());
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * Which dataset, read through which data source, a cached fact belongs to - the part of every address in
 * these stores that is a property of the dataset rather than of a file or of a read.
 * <p>
 * One instance per resolve, shared by reference across every key that resolve mints, so a glob over ten
 * thousand files holds one of these rather than ten thousand copies of the strings it was folded from.
 * Equality then starts with a reference check that succeeds for every key of the same dataset.
 *
 * <h2>Three pairs, kept apart on purpose</h2>
 * <ul>
 *   <li><b>dataset</b> - the dataset's own stored definition: its resource and its settings. Editing a
 *       data source does not move this, so a dataset's own identity is stable across changes it did not
 *       make, and which of the two moved is visible.</li>
 *   <li><b>source</b> - the data source's stored definition, folded with the digest of the declared-secret
 *       settings the provider consumed.</li>
 *   <li><b>participants</b> - what the resolved parties say identifies them: the storage provider's own
 *       identity, the format reader's identity for its configuration, and the coordinator's for its own.
 *       Separate from the two definitions because a query can reach these stores with no stored dataset
 *       behind it at all, where the definitions are absent and only these discriminate.</li>
 * </ul>
 *
 * <h2>Lanes, and why nothing reads them</h2>
 * Six {@code long}s behind private fields. Every one of these components is compared and never inspected,
 * so none of them is published: a record would hand out the hash representation as six public accessors
 * for no caller. Flat primitives rather than nested value types because a probe then dereferences one
 * shared object instead of walking a chain, and opaque rather than public because nothing needs the
 * value - the two goals do not conflict here.
 * <p>
 * 128 bits per pair, for the reason {@link org.elasticsearch.xpack.esql.datasources.FileSetFingerprint}
 * gives: a collision serves one dataset's record to another, which is a wrong answer and not a slow path.
 * Non-cryptographic (Murmur3) for the definition and participant folds, matching the file-set and
 * listing-cache precedents - these guard accidental collision, not an adversary. The secret digest folded
 * into the source pair arrives already hashed with SHA-256 by
 * {@code StorageIdentity.digestSecret}, so what is stored here is a fold of a digest and never a secret:
 * an address outlives the data source it came from, and records print their fields.
 *
 * <h2>What the secret digest is for, and what it is not</h2>
 * It is a second layer of defence. It stops two data sources that differ in their credentials from
 * sharing one address - the property the listing and footer-byte stores already had and these did not.
 * It is not the authorization control and must not be described as one: it cannot see a principal who may
 * list but not read within one data source, it cannot see a revoked credential, which digests to the
 * value it had while it was valid, and it cannot see {@code federated_identity}, whose token is presented
 * at read time and belongs to no definition. It also costs real sharing, since two data sources over the
 * same files under different credentials stop sharing warm facts that are equally true for both.
 */
public final class DatasetIdentity {

    private final long datasetHi;
    private final long datasetLo;
    private final long sourceHi;
    private final long sourceLo;
    private final long participantsHi;
    private final long participantsLo;
    private final int hash;

    private DatasetIdentity(long datasetHi, long datasetLo, long sourceHi, long sourceLo, long participantsHi, long participantsLo) {
        this.datasetHi = datasetHi;
        this.datasetLo = datasetLo;
        this.sourceHi = sourceHi;
        this.sourceLo = sourceLo;
        this.participantsHi = participantsHi;
        this.participantsLo = participantsLo;
        this.hash = computeHash();
    }

    /**
     * @param datasetVersion    the dataset's stored definition version, or {@code null}/empty for a query that
     *                          reaches these stores with no registered dataset behind it
     * @param dataSourceVersion the data source's stored definition version, under the same condition
     * @param secretIdentity    the SHA-256 digest of the declared-secret settings the provider consumed, as
     *                          {@code Configured.secretIdentityOf} computes it; empty when it consumed none,
     *                          which is a correct answer for an auth mode that has no stored secret
     * @param storageIdentity   what the storage provider says identifies the object
     * @param formatIdentity    what the format reader says identifies its own configuration
     * @param coordinatorIdentity what the coordinator says identifies its own
     */
    public static DatasetIdentity of(
        @Nullable String datasetVersion,
        @Nullable String dataSourceVersion,
        @Nullable String secretIdentity,
        @Nullable String storageIdentity,
        @Nullable String formatIdentity,
        @Nullable String coordinatorIdentity
    ) {
        MurmurHash3.Hash128 dataset = fold(datasetVersion);
        MurmurHash3.Hash128 source = fold(dataSourceVersion, secretIdentity);
        MurmurHash3.Hash128 participants = fold(storageIdentity, formatIdentity, coordinatorIdentity);
        return new DatasetIdentity(dataset.h1, dataset.h2, source.h1, source.h2, participants.h1, participants.h2);
    }

    /**
     * Length-prefixed so no component can forge a field boundary. These are open vocabulary - a resource
     * pattern, a provider identity and a digest all reach arbitrary text - so a plain join would let two
     * different identities encode the same way and share one address, which is the failure this guards.
     * A {@code null} encodes differently from an empty string, because absent and empty are different
     * states: a query with no stored dataset is not a query whose dataset has an empty resource.
     */
    private static MurmurHash3.Hash128 fold(@Nullable String... parts) {
        StringBuilder encoded = new StringBuilder();
        for (String part : parts) {
            if (part == null) {
                encoded.append("-1:");
            } else {
                encoded.append(part.length()).append(':').append(part);
            }
        }
        byte[] bytes = encoded.toString().getBytes(StandardCharsets.UTF_8);
        return MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
    }

    private int computeHash() {
        long mixed = datasetHi * 31 + datasetLo;
        mixed = mixed * 31 + sourceHi;
        mixed = mixed * 31 + sourceLo;
        mixed = mixed * 31 + participantsHi;
        mixed = mixed * 31 + participantsLo;
        return Long.hashCode(mixed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof DatasetIdentity other) {
            return datasetHi == other.datasetHi
                && datasetLo == other.datasetLo
                && sourceHi == other.sourceHi
                && sourceLo == other.sourceLo
                && participantsHi == other.participantsHi
                && participantsLo == other.participantsLo;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /**
     * Renders the three pairs as hex, for a log line or an assertion message. Deliberately not six
     * accessors: nothing needs a lane, and publishing them would make the representation part of the API.
     */
    @Override
    public String toString() {
        return "DatasetIdentity[dataset="
            + ReadConfigFingerprint.render(datasetHi, datasetLo)
            + ", source="
            + ReadConfigFingerprint.render(sourceHi, sourceLo)
            + ", participants="
            + ReadConfigFingerprint.render(participantsHi, participantsLo)
            + "]";
    }
}

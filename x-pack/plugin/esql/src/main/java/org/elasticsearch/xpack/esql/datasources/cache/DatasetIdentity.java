/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.DefinitionVersion;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Which dataset, read through which data source, a cached fact belongs to - the part of every address in
 * these stores that is a property of the dataset rather than of a file or of a read.
 * <p>
 * Six {@code long}s behind one reference. It is NOT shared one instance across the keys of a dataset: the
 * resolver derives it per mint site, because the participant fold includes
 * {@code formatConfigIdentity(objectName, config)} and that resolves a reader per object name. Hoisting it
 * to one instance per resolve is a separate change.
 *
 * <h2>Three pairs, and what currently feeds them</h2>
 * <ul>
 *   <li><b>dataset</b> - today this receives {@code DefinitionVersion.of(dataset, parent)}, which folds
 *       BOTH the dataset's definition and its data source's into one value. So the separation the next
 *       lane exists for is prepared and not yet real: editing a data source still moves this lane. The
 *       split needs {@code DefinitionVersion} to vend the two halves separately, which it does not.</li>
 *   <li><b>source</b> - the digest of the declared-secret settings the provider consumed, and only that. The
 *       data source's own definition version does not reach this lane: there is no parameter for it, because
 *       the dataset lane above already folds it. This lane separates from that one once the split exists.</li>
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
 * 128 bits per pair, for the reason {@link FileSetFingerprint}
 * gives: a collision serves one dataset's record to another, which is a wrong answer and not a slow path.
 * Non-cryptographic (Murmur3) for the definition and participant folds, matching the file-set
 * fingerprint, which guards accidental collision rather than an adversary. Note the listing cache is NOT
 * that precedent: {@code ListingCacheKey.sha256Truncated} uses SHA-256, because its pre-image carries
 * identities as plain user-influenced strings. Here the secret arrives already digested, so aiming a
 * collision would need the target's digest first. The secret digest folded
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
     * @param definitionVersion the stored definition version a query reads under, or {@code null}/empty for a query
     *                          that reaches these stores with no registered dataset behind it
     * @param secretIdentity    the SHA-256 digest of the declared-secret settings the provider consumed, as
     *                          {@code Configured.secretIdentityOf} computes it; empty when it consumed none,
     *                          which is a correct answer for an auth mode that has no stored secret
     * @param storageIdentity   what the storage provider says identifies the object
     * @param formatIdentity    what the format reader says identifies its own configuration
     * @param coordinatorIdentity what the coordinator says identifies its own
     */
    public static DatasetIdentity of(
        @Nullable String definitionVersion,
        @Nullable String secretIdentity,
        @Nullable String storageIdentity,
        @Nullable String formatIdentity,
        @Nullable String coordinatorIdentity
    ) {
        MurmurHash3.Hash128 dataset = fold(definitionVersion);
        MurmurHash3.Hash128 source = fold(secretIdentity);
        MurmurHash3.Hash128 participants = fold(storageIdentity, formatIdentity, coordinatorIdentity);
        return new DatasetIdentity(dataset.h1, dataset.h2, source.h1, source.h2, participants.h1, participants.h2);
    }

    /**
     * The version of the stored definitions a query reads under, as a named component rather than a format
     * setting: it is not an option a reader parses, so it has no place among the settings a participant reports
     * as its own identity.
     * <p>
     * Absent for a query that reaches the cache without a registered dataset behind it, where there is no
     * definition to version. Such entries share one version value and are discriminated by the participants
     * instead.
     */
    public static String definitionVersionOf(@Nullable Map<String, Object> config) {
        if (config == null) {
            return "";
        }
        Object version = config.get(DefinitionVersion.CONFIG_KEY);
        return version instanceof String s ? s : "";
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
     * The participant lanes alone, as an opaque token, for the one caller that must compare only those: the
     * reconcile refuses to enrich when the entries it matched for one path were derived under more than one
     * identity, because a contribution says which path, mtime and format config it came from and never which
     * entry it belongs to.
     * <p>
     * It must compare participants and NOT the whole identity. The components this leaves out - the definition
     * version and the secret digest - were outside the compared value before this type existed, so widening the
     * comparison to the whole identity makes two entries over one file that differ only in one of those refuse
     * each other, and then NEITHER is enriched while both live. The schema store has no clock, so that is not a
     * cold read each: it is a warm path that stays dead. Two datasets over one file differing only in their
     * definition version is an ordinary state, and so is one file reachable under two credential sets.
     * <p>
     * Whether a contribution should enrich an entry whose definition version it cannot confirm is a separate
     * question this does not answer; it preserves what the comparison did before.
     */
    Participants participants() {
        return new Participants(participantsHi, participantsLo);
    }

    /** An opaque equality token over the participant lanes; the lanes stay private to {@link DatasetIdentity}. */
    static final class Participants {

        private final long hi;
        private final long lo;

        private Participants(long hi, long lo) {
            this.hi = hi;
            this.lo = lo;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Participants other && hi == other.hi && lo == other.lo;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(hi * 31 + lo);
        }

        @Override
        public String toString() {
            return ReadConfigFingerprint.render(hi, lo);
        }
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

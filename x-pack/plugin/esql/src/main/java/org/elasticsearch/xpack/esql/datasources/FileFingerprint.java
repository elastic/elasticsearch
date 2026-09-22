/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.hash.MurmurHash3;

import java.nio.charset.StandardCharsets;

/**
 * A 128-bit identity of ONE file: its path, modification time and size. Two files share a fingerprint only when all
 * three agree — which is the same condition under which a cached result derived from the file is still valid.
 * <p>
 * This is the single definition of the per-file hash. {@link FileSetFingerprint} is the commutative fold of these over
 * a whole listing, and it is computed by delegating here, so the two cannot drift: a caller holding one file's
 * fingerprint and a caller holding the set's are hashing the same thing. That matters to anything keyed on part of a
 * listing — a cache keyed on the one file that decides a result must agree with a cache keyed on all of them about
 * what "the same file" means.
 * <p>
 * Kept distinct from {@link FileSetFingerprint} although both are two longs: one identifies a file, the other a set,
 * and a signature that accepted either would let a caller key a per-file cache on a whole listing by mistake.
 */
public record FileFingerprint(long high, long low) {

    /** Distinct odd multipliers so mtime and size perturb the two lanes independently. */
    private static final long MTIME_LANE_MULTIPLIER = 0x9E3779B97F4A7C15L;
    private static final long SIZE_LANE_MULTIPLIER = 0xC2B2AE3D27D4EB4FL;

    /**
     * The fingerprint of the file at {@code path}. {@code path} must be the path's canonical string form — the same
     * string for the same object however the listing that produced it was stored — or two listings of one file set
     * would disagree.
     */
    public static FileFingerprint of(String path, long lastModifiedMillis, long size) {
        byte[] pathBytes = path.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 pathHash = MurmurHash3.hash128(pathBytes, 0, pathBytes.length, 0, new MurmurHash3.Hash128());
        return new FileFingerprint(
            pathHash.h1 ^ MurmurHash3.fmix(lastModifiedMillis * MTIME_LANE_MULTIPLIER + size),
            pathHash.h2 ^ MurmurHash3.fmix(size * SIZE_LANE_MULTIPLIER + lastModifiedMillis)
        );
    }
}

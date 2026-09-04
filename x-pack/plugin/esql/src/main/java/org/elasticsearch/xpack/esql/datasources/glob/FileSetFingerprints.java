/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Computes the 128-bit {@linkplain FileList#fileSetFingerprint() file-set fingerprint} of a resolved
 * file set.
 * <p>
 * Each file contributes a 128-bit Murmur3 hash of its path (the same primitive
 * {@code ListingCacheKey.computeCredentialHash} already uses for identity hashing in the listing
 * cache), perturbed by its mtime and size with a different multiplier per lane so the two lanes stay
 * independent. Per-file contributions are folded <em>commutatively</em> (wrapping addition), so the
 * fingerprint is a pure function of the file SET — the same files listed in a different order produce
 * the same fingerprint, and any add/remove/mtime/size change produces a different one. The file count
 * is mixed into the final avalanche so that pathological cancellations across files still perturb the
 * result.
 * <p>
 * The fingerprint is an identity for cache keying (correct-or-miss dataset-level derived state), not a
 * cryptographic commitment: a 128-bit non-adversarial collision is negligible, matching the
 * listing-cache credential-hash precedent.
 */
final class FileSetFingerprints {

    private FileSetFingerprints() {}

    /** Distinct odd multipliers so mtime and size perturb the two lanes independently. */
    private static final long MTIME_LANE_MULTIPLIER = 0x9E3779B97F4A7C15L;
    private static final long SIZE_LANE_MULTIPLIER = 0xC2B2AE3D27D4EB4FL;

    /**
     * Computes the fingerprint over a raw {@link StorageEntry} list (the {@link GenericFileList}
     * storage). O(N), intended to run exactly once at listing build.
     */
    static FileSetFingerprint compute(List<StorageEntry> files) {
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        long sum1 = 0;
        long sum2 = 0;
        for (StorageEntry file : files) {
            byte[] pathBytes = file.path().toString().getBytes(StandardCharsets.UTF_8);
            MurmurHash3.hash128(pathBytes, 0, pathBytes.length, 0, scratch);
            long mtime = file.lastModified().toEpochMilli();
            long size = file.length();
            sum1 += scratch.h1 ^ MurmurHash3.fmix(mtime * MTIME_LANE_MULTIPLIER + size);
            sum2 += scratch.h2 ^ MurmurHash3.fmix(size * SIZE_LANE_MULTIPLIER + mtime);
        }
        return new FileSetFingerprint(MurmurHash3.fmix(sum1 + files.size()), MurmurHash3.fmix(sum2 ^ files.size()));
    }
}

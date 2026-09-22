/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.xpack.esql.datasources.FileFingerprint;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

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

    /**
     * Computes the fingerprint over a raw {@link StorageEntry} list (the {@link GenericFileList}
     * storage). O(N), intended to run exactly once at listing build.
     */
    static FileSetFingerprint compute(List<StorageEntry> files) {
        long sum1 = 0;
        long sum2 = 0;
        for (StorageEntry file : files) {
            FileFingerprint fingerprint = FileFingerprint.of(file.path().toString(), file.lastModified().toEpochMilli(), file.length());
            sum1 += fingerprint.high();
            sum2 += fingerprint.low();
        }
        return new FileSetFingerprint(MurmurHash3.fmix(sum1 + files.size()), MurmurHash3.fmix(sum2 ^ files.size()));
    }
}

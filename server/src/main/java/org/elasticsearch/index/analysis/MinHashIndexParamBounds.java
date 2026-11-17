/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.minhash.MinHashFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersions;

/**
 * Bounds on the parameters of a {@code min_hash} token filter defined in index settings. They are enforced for indices
 * created on or after {@link IndexVersions#MIN_HASH_INDEX_PARAM_BOUNDS}; older indices that exceed them keep working but
 * are reported through deprecation warnings, because the same configuration can't be used for a new index.
 * <p>
 * The filter is implemented in the {@code analysis-common} module; the bounds live here so that the deprecation info
 * checks can report them too.
 */
public final class MinHashIndexParamBounds {

    /**
     * Upper bound on {@code hash_count * bucket_count}. The filter allocates this many sets when it is constructed, and
     * keeps them for as long as the analyzer is cached on a thread.
     */
    public static final long MAX_HASH_SETS = 10_000;

    /**
     * Upper bound on {@code hash_count * bucket_count * hash_set_size}. The filter reads its whole input before emitting
     * tokens, and can hold up to this many hashes while doing so. Unlike {@code index.analyze.max_token_count}, this bound
     * doesn't depend on a dynamic setting, so it can't stop an existing index from opening.
     */
    public static final long MAX_HASH_ENTRIES = 100_000;

    private MinHashIndexParamBounds() {}

    /**
     * Like {@link #violation(int, int, int)}, reading the parameters from a filter's settings and applying the filter's
     * defaults for those that aren't set.
     */
    @Nullable
    public static String violation(Settings filterSettings) {
        return violation(
            filterSettings.getAsInt("hash_count", MinHashFilter.DEFAULT_HASH_COUNT),
            filterSettings.getAsInt("bucket_count", MinHashFilter.DEFAULT_BUCKET_COUNT),
            filterSettings.getAsInt("hash_set_size", MinHashFilter.DEFAULT_HASH_SET_SIZE)
        );
    }

    /**
     * Returns a description of the bound the given parameters exceed, or {@code null} if they are within the bounds.
     */
    @Nullable
    public static String violation(int hashCount, int bucketCount, int hashSetSize) {
        // Both factors are ints, so this can't overflow a long.
        long hashSets = (long) hashCount * bucketCount;
        if (hashSets > MAX_HASH_SETS) {
            return "The product of hash_count ["
                + hashCount
                + "] and bucket_count ["
                + bucketCount
                + "] is ["
                + hashSets
                + "], which exceeds the limit ["
                + MAX_HASH_SETS
                + "].";
        }
        // hashSets is at most MAX_HASH_SETS here, so this can't overflow a long either.
        long hashEntries = hashSets * hashSetSize;
        if (hashEntries > MAX_HASH_ENTRIES) {
            return "The product of hash_count ["
                + hashCount
                + "], bucket_count ["
                + bucketCount
                + "], and hash_set_size ["
                + hashSetSize
                + "] is ["
                + hashEntries
                + "], which exceeds the limit ["
                + MAX_HASH_ENTRIES
                + "].";
        }
        return null;
    }
}

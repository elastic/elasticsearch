/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.List;

/**
 * A glob listing as stored in the listing cache, plus the notices raised while producing it.
 * <p>
 * Expanding a glob can raise notices the user should see: objects dropped by {@code file_exclusions}, or a partition
 * column renamed because it clashed with a reserved name. The listing itself is cached. If the notices were emitted
 * only where they were raised, a cache hit would skip them, and the same query would warn on its first run and go
 * quiet on its second. Storing them with the listing lets a hit replay exactly what the miss did.
 */
public record CachedListing(FileList files, List<String> warnings) {
    public CachedListing {
        warnings = warnings != null ? List.copyOf(warnings) : List.of();
    }

    /** A listing that raised no notices. */
    public static CachedListing of(FileList files) {
        return new CachedListing(files, List.of());
    }

    /** Cache weight: the listing's own estimate plus each notice as a String (~40B header + UTF-16 chars). */
    public long estimatedBytes() {
        long bytes = files.estimatedBytes();
        for (String warning : warnings) {
            bytes += 40 + warning.length() * (long) Character.BYTES;
        }
        return bytes;
    }
}

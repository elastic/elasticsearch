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
 * A cached glob listing together with the client-facing notices raised while producing it (objects dropped by
 * {@code file_exclusions}, partition columns renamed off reserved names). The notices live with the listing so a
 * cache hit replays them exactly like the expansion that produced them did; emitted only where raised, the same query
 * would warn on its first run and go quiet on its second.
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

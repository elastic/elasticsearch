/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The dictionary of a {@link DocRefVector}: the origins its rows point at by ordinal. Immutable and free of duplicates,
 * so vectors filtered or sliced from one vector share it.
 */
public final class DocRefOrigins implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocRefOrigins.class);

    public static final DocRefOrigins EMPTY = new DocRefOrigins(new DocRefOrigin[0]);

    private final DocRefOrigin[] origins;
    private final long ramBytesUsed;

    private DocRefOrigins(DocRefOrigin[] origins) {
        this.origins = origins;
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(origins);
        for (DocRefOrigin origin : origins) {
            bytes += origin.ramBytesUsed();
        }
        this.ramBytesUsed = bytes;
    }

    /**
     * @throws IllegalArgumentException if an origin is listed twice
     */
    public static DocRefOrigins of(List<DocRefOrigin> origins) {
        if (origins.isEmpty()) {
            return EMPTY;
        }
        Set<DocRefOrigin> seen = new HashSet<>(origins.size());
        for (DocRefOrigin origin : origins) {
            if (seen.add(origin) == false) {
                throw new IllegalArgumentException("duplicate origin " + origin);
            }
        }
        return new DocRefOrigins(origins.toArray(DocRefOrigin[]::new));
    }

    public int size() {
        return origins.length;
    }

    public DocRefOrigin get(int ordinal) {
        return origins[ordinal];
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public String toString() {
        return Arrays.toString(origins);
    }
}

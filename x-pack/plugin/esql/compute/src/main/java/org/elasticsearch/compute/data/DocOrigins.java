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
 * The origins the rows of a vector point at by ordinal, once the rows left the node that read them. Immutable and free
 * of duplicates, so vectors filtered or sliced from one vector share it.
 */
public final class DocOrigins implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocOrigins.class);

    public static final DocOrigins EMPTY = new DocOrigins(new DocOrigin[0]);

    private final DocOrigin[] origins;
    private final long ramBytesUsed;

    private DocOrigins(DocOrigin[] origins) {
        this.origins = origins;
        long bytes = BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(origins);
        for (DocOrigin origin : origins) {
            bytes += origin.ramBytesUsed();
        }
        this.ramBytesUsed = bytes;
    }

    /**
     * @throws IllegalArgumentException if an origin is listed twice
     */
    public static DocOrigins of(List<DocOrigin> origins) {
        if (origins.isEmpty()) {
            return EMPTY;
        }
        Set<DocOrigin> seen = new HashSet<>(origins.size());
        for (DocOrigin origin : origins) {
            if (seen.add(origin) == false) {
                throw new IllegalArgumentException("duplicate origin " + origin);
            }
        }
        return new DocOrigins(origins.toArray(DocOrigin[]::new));
    }

    public int size() {
        return origins.length;
    }

    public DocOrigin get(int ordinal) {
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

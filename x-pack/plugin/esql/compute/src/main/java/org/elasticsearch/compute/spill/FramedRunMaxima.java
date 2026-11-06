/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * The bounds a reader validates before it allocates anything and a writer enforces as it goes, so that a corrupt
 * length or an unexpectedly large record fails with a clear message instead of an allocation the breaker has to catch.
 * The bounds also keep every size in the format inside an {@code int} where a Java array has to hold it.
 *
 * @param chunkBytes the largest chunk a header may announce and a writer may use
 * @param recordBytes the largest payload a record may have
 * @param indexEntries the most records a run may hold, which bounds the index a writer keeps in memory
 * @param partitions the most partitions a run may have, which bounds its directory
 */
public record FramedRunMaxima(int chunkBytes, long recordBytes, long indexEntries, int partitions) {

    /** A record must fit a byte array with room for a few bytes of framing. */
    public static final long MAX_RECORD_BYTES = Integer.MAX_VALUE - 64;
    /** The encoded index must fit a byte array. */
    public static final long MAX_INDEX_ENTRIES = (Integer.MAX_VALUE - 1024) / FramedRunLayout.INDEX_ENTRY_BYTES;
    public static final int MAX_PARTITIONS = 1 << 20;

    public static final FramedRunMaxima DEFAULT = new FramedRunMaxima(
        (int) ByteSizeValue.ofMb(16).getBytes(),
        ByteSizeValue.ofMb(512).getBytes(),
        64L * 1024 * 1024,
        1 << 16
    );

    public FramedRunMaxima(int chunkBytes, long recordBytes, long indexEntries) {
        this(chunkBytes, recordBytes, indexEntries, DEFAULT == null ? 1 << 16 : DEFAULT.partitions());
    }

    public FramedRunMaxima {
        if (chunkBytes <= 0 || recordBytes <= 0 || indexEntries <= 0 || partitions <= 0) {
            throw new IllegalArgumentException("maxima must be positive: " + this);
        }
        if (recordBytes > MAX_RECORD_BYTES || indexEntries > MAX_INDEX_ENTRIES || partitions > MAX_PARTITIONS) {
            throw new IllegalArgumentException("maxima exceed what the format can hold: " + this);
        }
    }
}

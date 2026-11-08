/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;

/**
 * Hands the {@link Resolver} to the doc values producer of a segment. Lucene creates the producer from a codec resolved by
 * name, so the producer cannot get the resolver through a constructor. The engine sets it for the thread that opens the segment,
 * and the producer keeps what it found at that moment.
 */
public final class ParquetReadContext {

    private static final ThreadLocal<Resolver> CURRENT = new ThreadLocal<>();

    private ParquetReadContext() {}

    /** The resolver of the segment that this thread opens now, or {@code null} if the thread opens a segment outside the engine. */
    @Nullable
    static Resolver current() {
        return CURRENT.get();
    }

    /** Runs the action with the resolver active, and restores the previous one afterwards. */
    public static <T> T with(Resolver resolver, CheckedSupplier<T, IOException> action) throws IOException {
        Resolver previous = CURRENT.get();
        CURRENT.set(resolver);
        try {
            return action.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /** Opens the file that a segment references. The engine that opens the segment supplies it, see {@link ParquetReadContext}. */
    public interface Resolver {

        /**
         * @param location the location stored in the segment
         * @param length   the length that the segment was attached with; an implementation must fail if the file has another length
         */
        StorageObject open(String location, long length) throws IOException;
    }
}

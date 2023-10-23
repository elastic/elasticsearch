/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Interface for loading data in a block shape. Instances of this class
 * must be immutable and thread safe.
 */
public interface BlockLoader {
    /**
     * Build a {@link LeafReaderContext leaf} level reader.
     */
    BlockDocValuesReader reader(LeafReaderContext context) throws IOException;

    /**
     * Does this loader support loading bytes via calling {@link #ordinals}.
     */
    default boolean supportsOrdinals() {
        return false;
    }

    /**
     * Load ordinals for the provided context.
     */
    default SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new IllegalStateException("ordinals not supported");
    }

    /**
     * A list of documents to load.
     */
    interface Docs {
        int count();

        int get(int i);
    }

    /**
     * Builds block "builders" for loading data into blocks for the compute engine.
     * It's important for performance that this only have one implementation in
     * production code. That implementation sits in the "compute" project. The is
     * also a test implementation, but there may be no more other implementations.
     */
    interface BuilderFactory {
        /**
         * Build a builder for booleans as loaded from doc values. Doc values
         * load booleans in sorted order.
         */
        BooleanBuilder booleansFromDocValues(int expectedCount);

        BooleanBuilder booleans(int expectedCount);

        BytesRefBuilder bytesRefsFromDocValues(int expectedCount);

        BytesRefBuilder bytesRefs(int expectedCount);

        DoubleBuilder doublesFromDocValues(int expectedCount);

        DoubleBuilder doubles(int expectedCount);

        IntBuilder intsFromDocValues(int expectedCount);

        IntBuilder ints(int expectedCount);

        LongBuilder longsFromDocValues(int expectedCount);

        LongBuilder longs(int expectedCount);

        Builder nulls(int expectedCount);

        SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count);

        // TODO support non-singleton ords
    }

    interface Builder {
        Builder appendNull();

        Builder beginPositionEntry();

        Builder endPositionEntry();
    }

    interface BooleanBuilder extends Builder {
        /**
         * Appends a boolean to the current entry.
         */
        BooleanBuilder appendBoolean(boolean value);
    }

    interface BytesRefBuilder extends Builder {
        /**
         * Appends a BytesRef to the current entry.
         */
        BytesRefBuilder appendBytesRef(BytesRef value);
    }

    interface DoubleBuilder extends Builder {
        /**
         * Appends a double to the current entry.
         */
        DoubleBuilder appendDouble(double value);
    }

    interface IntBuilder extends Builder {
        /**
         * Appends an int to the current entry.
         */
        IntBuilder appendInt(int value);
    }

    interface LongBuilder extends Builder {
        /**
         * Appends a long to the current entry.
         */
        LongBuilder appendLong(long value);
    }

    interface SingletonOrdinalsBuilder extends Builder {
        /**
         * Appends an ordinal to the builder.
         */
        SingletonOrdinalsBuilder appendOrd(int value);
    }
}

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

public interface BlockLoader {
    BlockDocValuesReader reader(LeafReaderContext context) throws IOException;

    default boolean supportsOrdinals() {
        return false;
    }

    default SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new IllegalStateException("ordinals not supported");
    }

    interface Docs {
        int count();

        int get(int i);
    }

    interface BuilderFactory {
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

        SingletonOrdinalBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int expectedCount);

        OrdinalsBuilder ordinalsBuilder(SortedSetDocValues ordinals, int expectedCount);
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

    interface SingletonOrdinalBuilder extends IntBuilder {}

    interface OrdinalsBuilder extends LongBuilder {}
}

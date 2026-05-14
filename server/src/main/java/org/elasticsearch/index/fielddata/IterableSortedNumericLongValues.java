/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;

import java.io.IOException;

/**
 * Iterable extention of {@link SortedNumericLongValues}.
 */
public abstract class IterableSortedNumericLongValues extends SortedNumericLongValues implements IterableNumericValues {

    /**
     * A {@link IterableSortedNumericLongValues} instance that does not have a value for any document
     */
    public static IterableSortedNumericLongValues EMPTY = new IterableSortedNumericLongValues() {
        @Override
        public int docID() {
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public boolean advanceExact(int target) {
            return false;
        }

        @Override
        public long nextValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docValueCount() {
            throw new UnsupportedOperationException();
        }
    };

    public abstract static class Singleton extends IterableSortedNumericLongValues {

        private LongValues longValues;

        @Override
        public final int docValueCount() {
            return 1;
        }

        public LongValues getLongValues() {
            if (longValues == null) {
                var iterableSingleton = this;
                longValues = new LongValues() {
                    @Override
                    public long longValue() throws IOException {
                        return iterableSingleton.nextValue();
                    }

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return iterableSingleton.advanceExact(doc);
                    }
                };
            }
            return longValues;
        }
    }
}

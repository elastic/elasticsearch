/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DoubleValues;

import java.io.IOException;

/**
 * Iterable extention of {@link SortedNumericDoubleValues}.
 */
public abstract class IterableSortedNumericDoubleValues extends SortedNumericDoubleValues implements IterableNumericValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected IterableSortedNumericDoubleValues() {}

    public abstract static class Singleton extends IterableSortedNumericDoubleValues {

        private DoubleValues doubleValues;

        protected Singleton() {}

        @Override
        public final int docValueCount() {
            return 1;
        }

        public DoubleValues getDoubleValues() {
            var singleton = this;
            if (doubleValues == null) {
                doubleValues = new DoubleValues() {
                    @Override
                    public double doubleValue() throws IOException {
                        return singleton.nextValue();
                    }

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return singleton.advanceExact(doc);
                    }
                };
            }
            return doubleValues;
        }
    }
}

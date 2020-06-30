/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;

public final class DoubleRuntimeValues extends AbstractRuntimeValues<DoubleRuntimeValues.SharedValues> {
    @FunctionalInterface
    public interface NewLeafLoader {
        IntConsumer leafLoader(LeafReaderContext ctx, DoubleConsumer sync) throws IOException;
    }

    private final NewLeafLoader newLeafLoader;

    public DoubleRuntimeValues(NewLeafLoader newLeafLoader) {
        this.newLeafLoader = newLeafLoader;
    }

    public CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValues() {
        return unstarted().docValues();
    }

    @Override
    protected SharedValues newSharedValues() {
        return new SharedValues();
    }

    protected class SharedValues extends AbstractRuntimeValues<SharedValues>.SharedValues {
        private double[] values = new double[1];

        @Override
        protected IntConsumer newLeafLoader(LeafReaderContext ctx) throws IOException {
            return newLeafLoader.leafLoader(ctx, this::add);
        }

        private void add(double value) {
            int newCount = count + 1;
            if (values.length < newCount) {
                values = Arrays.copyOf(values, ArrayUtil.oversize(newCount, 8));
            }
            values[count] = value;
            count = newCount;
        }

        @Override
        protected void sort() {
            Arrays.sort(values, 0, count);
        }

        private CheckedFunction<LeafReaderContext, SortedNumericDoubleValues, IOException> docValues() {
            alwaysSortResults();
            return DocValues::new;
        }

        private class DocValues extends SortedNumericDoubleValues {
            private final IntConsumer leafCursor;
            private int next;

            DocValues(LeafReaderContext ctx) throws IOException {
                leafCursor = leafCursor(ctx);
            }

            @Override
            public double nextValue() throws IOException {
                return values[next++];
            }

            @Override
            public int docValueCount() {
                return count;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                leafCursor.accept(target);
                next = 0;
                return count > 0;
            }
        }
    }
}

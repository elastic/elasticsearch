/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.comparators.NumericComparator;
import org.apache.lucene.util.BitUtil;

import java.io.IOException;

/**
 * Comparator for hal_float values.
 * This comparator provides a skipping functionality – an iterator that can skip over non-competitive documents.
 */
public class HalfFloatComparator extends NumericComparator<Float> {
    private final float[] values;
    protected float topValue;
    protected float bottom;

    public HalfFloatComparator(int numHits, String field, Float missingValue, boolean reverse, Pruning pruning) {
        super(field, missingValue != null ? missingValue : 0.0f, reverse, pruning, HalfFloatPoint.BYTES);
        values = new float[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        return Float.compare(values[slot1], values[slot2]);
    }

    @Override
    public void setTopValue(Float value) {
        super.setTopValue(value);
        topValue = value;
    }

    @Override
    public Float value(int slot) {
        return Float.valueOf(values[slot]);
    }

    @Override
    protected long missingValueAsComparableLong() {
        return HalfFloatPoint.halfFloatToSortableShort(missingValue);
    }

    @Override
    protected long sortableBytesToLong(byte[] bytes) {
        // Copied form HalfFloatPoint::sortableBytesToShort
        short x = (short) BitUtil.VH_BE_SHORT.get(bytes, 0);
        // Re-flip the sign bit to restore the original value:
        return (short) (x ^ 0x8000);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new HalfFloatLeafComparator(context);
    }

    /** Leaf comparator for {@link HalfFloatComparator} that provides skipping functionality */
    public class HalfFloatLeafComparator extends NumericLeafComparator {

        public HalfFloatLeafComparator(LeafReaderContext context) throws IOException {
            super(context);
        }

        private float getValueForDoc(int doc) throws IOException {
            if (docValues.advanceExact(doc)) {
                return Float.intBitsToFloat((int) docValues.longValue());
            } else {
                return missingValue;
            }
        }

        @Override
        public void setBottom(int slot) throws IOException {
            bottom = values[slot];
            super.setBottom(slot);
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return Float.compare(bottom, getValueForDoc(doc));
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return Float.compare(topValue, getValueForDoc(doc));
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = getValueForDoc(doc);
            super.copy(slot, doc);
        }

        @Override
        protected long bottomAsComparableLong() {
            return HalfFloatPoint.halfFloatToSortableShort(bottom);
        }

        @Override
        protected long topAsComparableLong() {
            return HalfFloatPoint.halfFloatToSortableShort(topValue);
        }
    }
}

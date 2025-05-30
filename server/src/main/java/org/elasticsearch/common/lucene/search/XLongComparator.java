/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.comparators.LongComparator;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

// TODO: remove once upgrading to Lucene 10.3, see base class why.
public class XLongComparator extends XNumericComparator<Long> {

    private final long[] values;
    protected long topValue;
    protected long bottom;

    public XLongComparator(int numHits, String field, Long missingValue, boolean reverse, Pruning pruning) {
        super(field, missingValue != null ? missingValue : 0L, reverse, pruning, Long.BYTES);
        values = new long[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public void setTopValue(Long value) {
        super.setTopValue(value);
        topValue = value;
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    protected long missingValueAsComparableLong() {
        return missingValue;
    }

    @Override
    protected long sortableBytesToLong(byte[] bytes) {
        return NumericUtils.sortableBytesToLong(bytes, 0);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new XLongComparator.LongLeafComparator(context);
    }

    /** Leaf comparator for {@link LongComparator} that provides skipping functionality */
    public class LongLeafComparator extends XNumericComparator<Long>.NumericLeafComparator {

        public LongLeafComparator(LeafReaderContext context) throws IOException {
            super(context);
        }

        private long getValueForDoc(int doc) throws IOException {
            if (docValues.advanceExact(doc)) {
                return docValues.longValue();
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
            return Long.compare(bottom, getValueForDoc(doc));
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return Long.compare(topValue, getValueForDoc(doc));
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = getValueForDoc(doc);
            super.copy(slot, doc);
        }

        @Override
        protected long bottomAsComparableLong() {
            return bottom;
        }

        @Override
        protected long topAsComparableLong() {
            return topValue;
        }
    }

}

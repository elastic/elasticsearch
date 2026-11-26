/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.List;

/**
 * A Lucene index sort on a key that an {@link IndexSortConfig#INDEX_SORT_SETTING} expression computes, such as
 * {@code DATE_TRUNC(1 hour, @timestamp)}. A segment records it as the key's text, order and position of missing keys, which
 * is all that Lucene needs to check that the segment's sort is the index sort. This sort field, as read back from a segment,
 * can't compute keys: the one that {@link IndexSortParser#resolve} builds from the mapping does.
 */
public class ComputedKeySortField extends SortField {

    public static final String PROVIDER_NAME = "ComputedKeySortField";

    private final boolean nullsFirst;

    /**
     * Keys sort as longs, and documents without a key take {@link Long#MIN_VALUE} or {@link Long#MAX_VALUE} to sort first or
     * last. A key equal to that value ties with them, as on long fields.
     */
    public ComputedKeySortField(String key, boolean reverse, boolean nullsFirst) {
        super(key, Type.LONG, reverse, nullsFirst == reverse ? Long.MAX_VALUE : Long.MIN_VALUE);
        this.nullsFirst = nullsFirst;
    }

    public boolean nullsFirst() {
        return nullsFirst;
    }

    @Override
    public IndexSorter getIndexSorter() {
        return new IndexSorter() {
            @Override
            public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) {
                throw cantComputeKeys();
            }

            @Override
            public DocComparator getDocComparator(LeafReader reader, int maxDoc) {
                throw cantComputeKeys();
            }

            @Override
            public String getProviderName() {
                return PROVIDER_NAME;
            }
        };
    }

    private IllegalStateException cantComputeKeys() {
        return new IllegalStateException("index sort key [" + getField() + "] read from a segment can't compute keys");
    }

    /**
     * Only {@code CheckIndex} compares documents on an index sort, with the sort read back from the segment. This reports every
     * document as sorted after the previous one, which stops the comparison at this sort field.
     */
    @Override
    public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
        return new InOrderComparator(getReverse());
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ComputedKeySortField other && super.equals(other) && nullsFirst == other.nullsFirst;
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + Boolean.hashCode(nullsFirst);
    }

    /** Reports every document as sorted after the previous one, whatever the order. */
    private static final class InOrderComparator extends FieldComparator<Void> {
        private final int after;

        InOrderComparator(boolean reverse) {
            // CheckIndex multiplies the comparison by -1 for a reverse order
            this.after = reverse ? 1 : -1;
        }

        @Override
        public int compare(int slot1, int slot2) {
            return 0;
        }

        @Override
        public void setTopValue(Void value) {}

        @Override
        public Void value(int slot) {
            return null;
        }

        @Override
        public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
            return new LeafFieldComparator() {
                @Override
                public void setBottom(int slot) {}

                @Override
                public int compareBottom(int doc) {
                    return after;
                }

                @Override
                public int compareTop(int doc) {
                    return after;
                }

                @Override
                public void copy(int slot, int doc) {}

                @Override
                public void setScorer(Scorable scorer) {}
            };
        }
    }

    /** Writes the key's text, order and position of missing keys to the segment. */
    public static final class Provider extends SortFieldProvider {

        public Provider() {
            super(PROVIDER_NAME);
        }

        @Override
        public SortField readSortField(DataInput in) throws IOException {
            String key = in.readString();
            boolean reverse = in.readByte() == 1;
            boolean nullsFirst = in.readByte() == 1;
            return new ComputedKeySortField(key, reverse, nullsFirst);
        }

        @Override
        public void writeSortField(SortField sf, DataOutput out) throws IOException {
            ComputedKeySortField sortField = (ComputedKeySortField) sf;
            out.writeString(sortField.getField());
            out.writeByte((byte) (sortField.getReverse() ? 1 : 0));
            out.writeByte((byte) (sortField.nullsFirst ? 1 : 0));
        }
    }
}

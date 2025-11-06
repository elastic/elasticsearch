/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.cluster.metadata.DataStream;

import java.io.IOException;

/**
 * Comparator when sorting using @timestamp field when logsdb or time series index modes are enabled.
 */
public class TimestampComparator extends FieldComparator<Long> {

    private static final String FIELD_NAME = DataStream.TIMESTAMP_FIELD_NAME;

    private final long[] values;
    private final boolean reverse;

    protected long topValue;
    protected long bottom;

    protected boolean topValueSet;
    protected boolean singleSort; // singleSort is true, if sort is based on a single sort field.
    protected boolean hitsThresholdReached;
    protected boolean queueFull;

    public TimestampComparator(int numHits, boolean reverse) {
        this.reverse = reverse;
        this.values = new long[numHits];
    }

    @Override
    public void setTopValue(Long value) {
        topValueSet = true;
        topValue = value;
    }

    @Override
    public void setSingleSort() {
        singleSort = true;
    }

    @Override
    public void disableSkipping() {}

    @Override
    public int compare(int slot1, int slot2) {
        return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public Long value(int slot) {
        return Long.valueOf(values[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        return new TimestampLeafComparator(context.reader());
    }

    class TimestampLeafComparator implements LeafFieldComparator {

        private final int maxDoc;
        private final Sort indexSort;
        private final boolean useTimestampFieldOnly;
        private final NumericDocValues timestamps;
        private final DocValuesSkipper timestampSkipper;
        private final DocValuesSkipper primaryFieldSkipper;

        private final boolean leafTopSet = topValueSet;

        private DocIdSetIterator competitiveIterator;
        private long iteratorCost = -1;

        TimestampLeafComparator(LeafReader reader) throws IOException {
            this.maxDoc = reader.maxDoc();
            this.indexSort = reader.getMetaData().sort();
            assert indexSort != null;
            assert indexSort.getSort().length == 1 || indexSort.getSort().length == 2;

            this.timestamps = TimestampQuery.getNumericDocValues(reader);
            this.timestampSkipper = reader.getDocValuesSkipper(FIELD_NAME);

            String primarySortField = indexSort.getSort()[0].getField();
            boolean timestampIsPrimarySort = primarySortField.equals(FIELD_NAME);
            var primaryFieldValues = timestampIsPrimarySort ? null : reader.getSortedDocValues(primarySortField);
            useTimestampFieldOnly = timestampIsPrimarySort || (primaryFieldValues == null || primaryFieldValues.getValueCount() <= 1);
            if (useTimestampFieldOnly == false) {
                primaryFieldSkipper = reader.getDocValuesSkipper(primarySortField);
            } else {
                primaryFieldSkipper = null;
            }

            if (timestampSkipper == null || timestampSkipper.maxValue() < bottom) {
                this.competitiveIterator = DocIdSetIterator.empty();
            } else {
                this.competitiveIterator = DocIdSetIterator.all(maxDoc);
            }
            this.iteratorCost = competitiveIterator.cost();
        }

        @Override
        public void setBottom(int slot) throws IOException {
            bottom = values[slot];
            queueFull = true; // if we are setting bottom, it means that we have collected enough hits
            updateCompetitiveIterator(); // update an iterator if we set a new bottom
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            values[slot] = getValueForDoc(doc);
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (iteratorCost == -1) {
                if (scorer instanceof Scorer) {
                    iteratorCost = ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
                } else {
                    iteratorCost = maxDoc;
                }
                updateCompetitiveIterator(); // update an iterator when we have a new segment
            }
        }

        @Override
        public void setHitsThresholdReached() throws IOException {
            hitsThresholdReached = true;
            updateCompetitiveIterator();
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return Long.compare(bottom, getValueForDoc(doc));
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return Long.compare(topValue, getValueForDoc(doc));
        }

        private long getValueForDoc(int doc) throws IOException {
            boolean found = timestamps.advanceExact(doc);
            assert found;
            return timestamps.longValue();
        }

        // update its iterator to include possibly only docs that are "stronger" than the current bottom
        // entry
        private void updateCompetitiveIterator() throws IOException {
            if (hitsThresholdReached == false || (leafTopSet == false && queueFull == false)) {
                return;
            }

            long minTimestamp;
            long maxTimestamp;
            if (reverse) {
                minTimestamp = bottom;
                maxTimestamp = Long.MAX_VALUE;
            } else {
                minTimestamp = Long.MIN_VALUE;
                maxTimestamp = bottom;
            }

            if (timestampSkipper.maxValue() < bottom) {
                competitiveIterator = DocIdSetIterator.empty();
            } else if (useTimestampFieldOnly) {
                competitiveIterator = TimestampQuery.getIteratorIfTimestampIfPrimarySort(
                    maxDoc,
                    timestamps,
                    timestampSkipper,
                    minTimestamp,
                    maxTimestamp
                );
            } else {
                competitiveIterator = new TimestampIterator(timestamps, timestampSkipper, primaryFieldSkipper, minTimestamp, maxTimestamp);
            }
            iteratorCost = competitiveIterator.cost();
        }

        @Override
        public DocIdSetIterator competitiveIterator() {
            return new DocIdSetIterator() {
                private int docID = competitiveIterator.docID();

                @Override
                public int nextDoc() throws IOException {
                    return advance(docID + 1);
                }

                @Override
                public int docID() {
                    return docID;
                }

                @Override
                public long cost() {
                    return competitiveIterator.cost();
                }

                @Override
                public int advance(int target) throws IOException {
                    return docID = competitiveIterator.advance(target);
                }

                @Override
                public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
                    // The competitive iterator is usually a BitSetIterator, which has an optimized
                    // implementation of #intoBitSet.
                    if (competitiveIterator.docID() < docID) {
                        competitiveIterator.advance(docID);
                    }
                    competitiveIterator.intoBitSet(upTo, bitSet, offset);
                    docID = competitiveIterator.docID();
                }
            };
        }

    }
}

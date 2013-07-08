/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;

/**
 * Sorts by field's natural Term sort order, using
 * ordinals.  This is functionally equivalent to {@link
 * org.apache.lucene.search.FieldComparator.TermValComparator}, but it first resolves the string
 * to their relative ordinal positions (using the index
 * returned by {@link org.apache.lucene.search.FieldCache#getTermsIndex}), and
 * does most comparisons using the ordinals.  For medium
 * to large results, this comparator will be much faster
 * than {@link org.apache.lucene.search.FieldComparator.TermValComparator}.  For very small
 * result sets it may be slower.
 */
public final class BytesRefOrdValComparator extends FieldComparator<BytesRef> {

    final IndexFieldData.WithOrdinals<?> indexFieldData;

    /* Ords for each slot.
       @lucene.internal */
    final long[] ords;

    final SortMode sortMode;

    /* Values for each slot.
       @lucene.internal */
    final BytesRef[] values;

    /* Which reader last copied a value into the slot. When
       we compare two slots, we just compare-by-ord if the
       readerGen is the same; else we must compare the
       values (slower).
       @lucene.internal */
    final int[] readerGen;

    /* Gen of current reader we are on.
       @lucene.internal */
    int currentReaderGen = -1;

    /* Current reader's doc ord/values.
       @lucene.internal */
    BytesValues.WithOrdinals termsIndex;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene.internal */
    long bottomOrd;

    /* True if current bottom slot matches the current
       reader.
       @lucene.internal */
    boolean bottomSameReader;

    /* Bottom value (same as values[bottomSlot] once
       bottomSlot is set).  Cached for faster compares.
      @lucene.internal */
    BytesRef bottomValue;

    final BytesRef tempBR = new BytesRef();

    public BytesRefOrdValComparator(IndexFieldData.WithOrdinals<?> indexFieldData, int numHits, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        ords = new long[numHits];
        values = new BytesRef[numHits];
        readerGen = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
        if (readerGen[slot1] == readerGen[slot2]) {
            return LongValuesComparator.compare(ords[slot1], ords[slot2]);
        }

        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
        BytesRef docValue = termsIndex.getValue(doc);
        if (docValue == null) {
            if (value == null) {
                return 0;
            }
            return -1;
        } else if (value == null) {
            return 1;
        }
        return docValue.compareTo(value);
    }

    /**
     * Base class for specialized (per bit width of the
     * ords) per-segment comparator.  NOTE: this is messy;
     * we do this only because hotspot can't reliably inline
     * the underlying array access when looking up doc->ord
     *
     * @lucene.internal
     */
    abstract class PerSegmentComparator extends FieldComparator<BytesRef> {

        @Override
        public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
            return BytesRefOrdValComparator.this.setNextReader(context);
        }

        @Override
        public int compare(int slot1, int slot2) {
            return BytesRefOrdValComparator.this.compare(slot1, slot2);
        }

        @Override
        public void setBottom(final int bottom) {
            BytesRefOrdValComparator.this.setBottom(bottom);
        }

        @Override
        public BytesRef value(int slot) {
            return BytesRefOrdValComparator.this.value(slot);
        }

        @Override
        public int compareValues(BytesRef val1, BytesRef val2) {
            if (val1 == null) {
                if (val2 == null) {
                    return 0;
                }
                return -1;
            } else if (val2 == null) {
                return 1;
            }
            return val1.compareTo(val2);
        }

        @Override
        public int compareDocToValue(int doc, BytesRef value) {
            return BytesRefOrdValComparator.this.compareDocToValue(doc, value);
        }
    }

    // Used per-segment when bit width of doc->ord is 8:
    private final class ByteOrdComparator extends PerSegmentComparator {
        private final byte[] readerOrds;
        private final BytesValues.WithOrdinals termsIndex;
        private final int docBase;

        public ByteOrdComparator(byte[] readerOrds, BytesValues.WithOrdinals termsIndex, int docBase) {
            this.readerOrds = readerOrds;
            this.termsIndex = termsIndex;
            this.docBase = docBase;
        }

        @Override
        public int compareBottom(int doc) {
            assert bottomSlot != -1;
            final int docOrd = (readerOrds[doc] & 0xFF);
            if (bottomSameReader) {
                // ord is precisely comparable, even in the equal case
                return (int) bottomOrd - docOrd;
            } else if (bottomOrd >= docOrd) {
                // the equals case always means bottom is > doc
                // (because we set bottomOrd to the lower bound in
                // setBottom):
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void copy(int slot, int doc) {
            final int ord = readerOrds[doc] & 0xFF;
            ords[slot] = ord;
            if (ord == 0) {
                values[slot] = null;
            } else {
                assert ord > 0;
                if (values[slot] == null) {
                    values[slot] = new BytesRef();
                }
                termsIndex.getValueScratchByOrd(ord, values[slot]);
            }
            readerGen[slot] = currentReaderGen;
        }
    }

    // Used per-segment when bit width of doc->ord is 16:
    private final class ShortOrdComparator extends PerSegmentComparator {
        private final short[] readerOrds;
        private final BytesValues.WithOrdinals termsIndex;
        private final int docBase;

        public ShortOrdComparator(short[] readerOrds, BytesValues.WithOrdinals termsIndex, int docBase) {
            this.readerOrds = readerOrds;
            this.termsIndex = termsIndex;
            this.docBase = docBase;
        }

        @Override
        public int compareBottom(int doc) {
            assert bottomSlot != -1;
            final int docOrd = (readerOrds[doc] & 0xFFFF);
            if (bottomSameReader) {
                // ord is precisely comparable, even in the equal case
                return (int) bottomOrd - docOrd;
            } else if (bottomOrd >= docOrd) {
                // the equals case always means bottom is > doc
                // (because we set bottomOrd to the lower bound in
                // setBottom):
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void copy(int slot, int doc) {
            final int ord = readerOrds[doc] & 0xFFFF;
            ords[slot] = ord;
            if (ord == 0) {
                values[slot] = null;
            } else {
                assert ord > 0;
                if (values[slot] == null) {
                    values[slot] = new BytesRef();
                }
                termsIndex.getValueScratchByOrd(ord, values[slot]);
            }
            readerGen[slot] = currentReaderGen;
        }
    }

    // Used per-segment when bit width of doc->ord is 32:
    private final class IntOrdComparator extends PerSegmentComparator {
        private final int[] readerOrds;
        private final BytesValues.WithOrdinals termsIndex;
        private final int docBase;

        public IntOrdComparator(int[] readerOrds, BytesValues.WithOrdinals termsIndex, int docBase) {
            this.readerOrds = readerOrds;
            this.termsIndex = termsIndex;
            this.docBase = docBase;
        }

        @Override
        public int compareBottom(int doc) {
            assert bottomSlot != -1;
            final int docOrd = readerOrds[doc];
            if (bottomSameReader) {
                // ord is precisely comparable, even in the equal case
                return (int) bottomOrd - docOrd;
            } else if (bottomOrd >= docOrd) {
                // the equals case always means bottom is > doc
                // (because we set bottomOrd to the lower bound in
                // setBottom):
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void copy(int slot, int doc) {
            final int ord = readerOrds[doc];
            ords[slot] = ord;
            if (ord == 0) {
                values[slot] = null;
            } else {
                assert ord > 0;
                if (values[slot] == null) {
                    values[slot] = new BytesRef();
                }
                termsIndex.getValueScratchByOrd(ord, values[slot]);
            }
            readerGen[slot] = currentReaderGen;
        }
    }

    // Used per-segment when bit width is not a native array
    // size (8, 16, 32):
    final class AnyOrdComparator extends PerSegmentComparator {
        private final IndexFieldData fieldData;
        private final Ordinals.Docs readerOrds;
        private final BytesValues.WithOrdinals termsIndex;
        private final int docBase;

        public AnyOrdComparator(IndexFieldData fieldData, BytesValues.WithOrdinals termsIndex, int docBase) {
            this.fieldData = fieldData;
            this.readerOrds = termsIndex.ordinals();
            this.termsIndex = termsIndex;
            this.docBase = docBase;
        }

        @Override
        public int compareBottom(int doc) {
            assert bottomSlot != -1;
            final long docOrd = readerOrds.getOrd(doc);
            if (bottomSameReader) {
                // ord is precisely comparable, even in the equal case
                return LongValuesComparator.compare(bottomOrd, docOrd);
            } else if (bottomOrd >= docOrd) {
                // the equals case always means bottom is > doc
                // (because we set bottomOrd to the lower bound in
                // setBottom):
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void copy(int slot, int doc) {
            final long ord = readerOrds.getOrd(doc);
            ords[slot] = ord;
            if (ord == 0) {
                values[slot] = null;
            } else {
                assert ord > 0;
                if (values[slot] == null) {
                    values[slot] = new BytesRef();
                }
                termsIndex.getValueScratchByOrd(ord, values[slot]);
            }
            readerGen[slot] = currentReaderGen;
        }
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        final int docBase = context.docBase;
        termsIndex = indexFieldData.load(context).getBytesValues();
        FieldComparator<BytesRef> perSegComp = null;
        if (termsIndex.isMultiValued()) {
            perSegComp = new MultiAnyOrdComparator(termsIndex);
        } else {
            final Ordinals.Docs docToOrd = termsIndex.ordinals();
            Object ordsStorage = docToOrd.ordinals().getBackingStorage();

            if (docToOrd.ordinals().hasSingleArrayBackingStorage()) {
                if (ordsStorage instanceof byte[]) {
                    perSegComp = new ByteOrdComparator((byte[]) ordsStorage, termsIndex, docBase);
                } else if (ordsStorage instanceof short[]) {
                    perSegComp = new ShortOrdComparator((short[]) ordsStorage, termsIndex, docBase);
                } else if (ordsStorage instanceof int[]) {
                    perSegComp = new IntOrdComparator((int[]) ordsStorage, termsIndex, docBase);
                }
            }
            // Don't specialize the long[] case since it's not
            // possible, ie, worse case is MAX_INT-1 docs with
            // every one having a unique value.

            // TODO: ES - should we optimize for the PackedInts.Reader case as well?
            if (perSegComp == null) {
                perSegComp = new AnyOrdComparator(indexFieldData, termsIndex, docBase);
            }
        }
        currentReaderGen++;
        if (bottomSlot != -1) {
            perSegComp.setBottom(bottomSlot);
        }
        return perSegComp;
    }

    @Override
    public void setBottom(final int bottom) {
        bottomSlot = bottom;

        bottomValue = values[bottomSlot];
        if (currentReaderGen == readerGen[bottomSlot]) {
            bottomOrd = ords[bottomSlot];
            bottomSameReader = true;
        } else {
            if (bottomValue == null) {
                // 0 ord is null for all segments
                assert ords[bottomSlot] == 0;
                bottomOrd = 0;
                bottomSameReader = true;
                readerGen[bottomSlot] = currentReaderGen;
            } else {
                final long index = binarySearch(termsIndex, bottomValue);
                if (index < 0) {
                    bottomOrd = -index - 2;
                    bottomSameReader = false;
                } else {
                    bottomOrd = index;
                    // exact value match
                    bottomSameReader = true;
                    readerGen[bottomSlot] = currentReaderGen;
                    ords[bottomSlot] = bottomOrd;
                }
            }
        }
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    final protected static long binarySearch(BytesValues.WithOrdinals a, BytesRef key) {
        return binarySearch(a, key, 1, a.ordinals().getNumOrds());
    }

    final protected static long binarySearch(BytesValues.WithOrdinals a, BytesRef key, long low, long high) {
        assert a.getValueByOrd(high) == null | a.getValueByOrd(high) != null; // make sure we actually can get these values
        assert a.getValueByOrd(low) == null | a.getValueByOrd(low) != null;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            BytesRef midVal = a.getValueByOrd(mid);
            int cmp;
            if (midVal != null) {
                cmp = midVal.compareTo(key);
            } else {
                cmp = -1;
            }

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
    }


    class MultiAnyOrdComparator extends PerSegmentComparator {

        private final BytesValues.WithOrdinals termsIndex;
        private final Ordinals.Docs readerOrds;

        private MultiAnyOrdComparator(BytesValues.WithOrdinals termsIndex) {
            this.termsIndex = termsIndex;
            this.readerOrds = termsIndex.ordinals();
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            final long docOrd = getRelevantOrd(readerOrds, doc, sortMode);
            if (bottomSameReader) {
                // ord is precisely comparable, even in the equal case
                return LongValuesComparator.compare(bottomOrd, docOrd);
            } else if (bottomOrd >= docOrd) {
                // the equals case always means bottom is > doc
                // (because we set bottomOrd to the lower bound in
                // setBottom):
                return 1;
            } else {
                return -1;
            }
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
            final long ord = getRelevantOrd(readerOrds, doc, sortMode);
            ords[slot] = ord;
            if (ord == 0) {
                values[slot] = null;
            } else {
                assert ord > 0;
                if (values[slot] == null) {
                    values[slot] = new BytesRef();
                }
                termsIndex.getValueScratchByOrd(ord, values[slot]);
            }
            readerGen[slot] = currentReaderGen;
        }

        @Override
        public int compareDocToValue(int doc, BytesRef value) {
            BytesRef docValue = getRelevantValue(termsIndex, doc, sortMode);
            if (docValue == null) {
                if (value == null) {
                    return 0;
                }
                return -1;
            } else if (value == null) {
                return 1;
            }
            return docValue.compareTo(value);
        }

    }

    static BytesRef getRelevantValue(BytesValues.WithOrdinals readerValues, int docId, SortMode sortMode) {
        BytesValues.Iter iter = readerValues.getIter(docId);
        if (!iter.hasNext()) {
            return null;
        }

        BytesRef currentVal = iter.next();
        BytesRef relevantVal = currentVal;
        while (true) {
            int cmp = currentVal.compareTo(relevantVal);
            if (sortMode == SortMode.MAX) {
                if (cmp > 0) {
                    relevantVal = currentVal;
                }
            } else {
                if (cmp < 0) {
                    relevantVal = currentVal;
                }
            }
            if (!iter.hasNext()) {
                break;
            }
            currentVal = iter.next();
        }
        return relevantVal;
    }

    static long getRelevantOrd(Ordinals.Docs readerOrds, int docId, SortMode sortMode) {
        Ordinals.Docs.Iter iter = readerOrds.getIter(docId);
        long currentVal = iter.next();
        if (currentVal == 0) {
            return 0;
        }

        long relevantVal = currentVal;
        while (true) {
            if (sortMode == SortMode.MAX) {
                if (currentVal > relevantVal) {
                    relevantVal = currentVal;
                }
            } else {
                if (currentVal < relevantVal) {
                    relevantVal = currentVal;
                }
            }
            currentVal = iter.next();
            if (currentVal == 0) {
                break;
            }
        }
        return relevantVal;
        // Enable this when the api can tell us that the ords per doc are ordered
        /*if (reversed) {
            IntArrayRef ref = readerOrds.getOrds(docId);
            if (ref.isEmpty()) {
                return 0;
            } else {
                return ref.values[ref.end - 1]; // last element is the highest value.
            }
        } else {
            return readerOrds.getOrd(docId); // returns the lowest value
        }*/
    }

}

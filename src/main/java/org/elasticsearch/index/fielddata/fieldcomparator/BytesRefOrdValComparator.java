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
 *
 * Internally this comparator multiplies ordinals by 4 so that virtual ordinals can be inserted in-between the original field data ordinals.
 * Thanks to this, an ordinal for the missing value and the bottom value can be computed and all ordinals are directly comparable. For example,
 * if the field data ordinals are (a,1), (b,2) and (c,3), they will be internally stored as (a,4), (b,8), (c,12). Then the ordinal for the
 * missing value will be computed by binary searching. For example, if the missing value is 'ab', it will be assigned 6 as an ordinal (between
 * 'a' and 'b'. And if the bottom value is 'ac', it will be assigned 7 as an ordinal (between 'ab' and 'b').
 */
public final class BytesRefOrdValComparator extends NestedWrappableComparator<BytesRef> {

    final IndexFieldData.WithOrdinals<?> indexFieldData;
    final BytesRef missingValue;

    /* Ords for each slot, times 4.
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
    long missingOrd;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene.internal */
    long bottomOrd;

    final BytesRef tempBR = new BytesRef();

    public BytesRefOrdValComparator(IndexFieldData.WithOrdinals<?> indexFieldData, int numHits, SortMode sortMode, BytesRef missingValue) {
        this.indexFieldData = indexFieldData;
        this.sortMode = sortMode;
        this.missingValue = missingValue;
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
    public int compareBottomMissing() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void missing(int slot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
        throw new UnsupportedOperationException();
    }

    class PerSegmentComparator extends NestedWrappableComparator<BytesRef> {
        final Ordinals.Docs readerOrds;
        final BytesValues.WithOrdinals termsIndex;

        public PerSegmentComparator(BytesValues.WithOrdinals termsIndex) {
            this.readerOrds = termsIndex.ordinals();
            this.termsIndex = termsIndex;
            if (readerOrds.getNumOrds() > Long.MAX_VALUE / 4) {
                throw new IllegalStateException("Current terms index pretends it has more than " + (Long.MAX_VALUE / 4) + " ordinals, which is unsupported by this impl");
            }
        }

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
            final long ord = getOrd(doc);
            final BytesRef docValue = ord == Ordinals.MISSING_ORDINAL ? missingValue : termsIndex.getValueByOrd(ord);
            return compareValues(docValue, value);
        }

        protected long getOrd(int doc) {
            return readerOrds.getOrd(doc);
        }

        @Override
        public int compareBottom(int doc) {
            assert bottomSlot != -1;
            final long docOrd = getOrd(doc);
            final long comparableOrd = docOrd == Ordinals.MISSING_ORDINAL ? missingOrd : docOrd << 2;
            return LongValuesComparator.compare(bottomOrd, comparableOrd);
        }

        @Override
        public int compareBottomMissing() {
            assert bottomSlot != -1;
            return LongValuesComparator.compare(bottomOrd, missingOrd);
        }

        @Override
        public void copy(int slot, int doc) {
            final long ord = getOrd(doc);
            if (ord == Ordinals.MISSING_ORDINAL) {
                ords[slot] = missingOrd;
                values[slot] = missingValue;
            } else {
                assert ord > 0;
                ords[slot] = ord << 2;
                if (values[slot] == null || values[slot] == missingValue) {
                    values[slot] = new BytesRef();
                }
                values[slot].copyBytes(termsIndex.getValueByOrd(ord));
            }
            readerGen[slot] = currentReaderGen;
        }

        @Override
        public void missing(int slot) {
            ords[slot] = missingOrd;
            values[slot] = missingValue;
        }
    }

    // for assertions
    private boolean consistentInsertedOrd(BytesValues.WithOrdinals termsIndex, long ord, BytesRef value) {
        assert ord >= 0 : ord;
        assert (ord == 0) == (value == null) : "ord=" + ord + ", value=" + value;
        final long previousOrd = ord >>> 2;
        final long nextOrd = previousOrd + 1;
        final BytesRef previous = previousOrd == 0 ? null : termsIndex.getValueByOrd(previousOrd);
        if ((ord & 3) == 0) { // there was an existing ord with the inserted value
            assert compareValues(previous, value) == 0;
        } else {
            assert compareValues(previous, value) < 0;
        }
        if (nextOrd < termsIndex.ordinals().getMaxOrd()) {
            final BytesRef next = termsIndex.getValueByOrd(nextOrd);
            assert compareValues(value, next) < 0;
        }
        return true;
    }

    // find where to insert an ord in the current terms index
    private long ordInCurrentReader(BytesValues.WithOrdinals termsIndex, BytesRef value) {
        final long docOrd = binarySearch(termsIndex, value);
        assert docOrd != -1; // would mean smaller than null
        final long ord;
        if (docOrd >= 0) {
            // value exists in the current segment
            ord = docOrd << 2;
        } else {
            // value doesn't exist, use the ord between the previous and the next term
            ord = ((-2 - docOrd) << 2) + 2;
        }
        assert (ord & 1) == 0;
        return ord;
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        termsIndex = indexFieldData.load(context).getBytesValues(false);
        assert termsIndex.ordinals() != null && termsIndex.ordinals().ordinals() != null;
        if (missingValue == null) {
            missingOrd = Ordinals.MISSING_ORDINAL;
        } else {
            missingOrd = ordInCurrentReader(termsIndex, missingValue);
            assert consistentInsertedOrd(termsIndex, missingOrd, missingValue);
        }
        FieldComparator<BytesRef> perSegComp = null;
        assert termsIndex.ordinals() != null && termsIndex.ordinals().ordinals() != null;
        if (termsIndex.isMultiValued()) {
            perSegComp = new PerSegmentComparator(termsIndex) {
                @Override
                protected long getOrd(int doc) {
                    return getRelevantOrd(readerOrds, doc, sortMode);
                }
            };
        } else {
            perSegComp = new PerSegmentComparator(termsIndex);
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
        final BytesRef bottomValue = values[bottomSlot];

        if (bottomValue == null) {
            bottomOrd = Ordinals.MISSING_ORDINAL;
        } else if (currentReaderGen == readerGen[bottomSlot]) {
            bottomOrd = ords[bottomSlot];
        } else {
            // insert an ord
            bottomOrd = ordInCurrentReader(termsIndex, bottomValue);
            if (bottomOrd == missingOrd) {
                // bottomValue and missingValue and in-between the same field data values -> tie-break
                // this is why we multiply ords by 4
                assert missingValue != null;
                final int cmp = bottomValue.compareTo(missingValue);
                if (cmp < 0) {
                    --bottomOrd;
                } else if (cmp > 0) {
                    ++bottomOrd;
                }
            }
            assert consistentInsertedOrd(termsIndex, bottomOrd, bottomValue);
        }
        readerGen[bottomSlot] = currentReaderGen;
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    final protected static long binarySearch(BytesValues.WithOrdinals a, BytesRef key) {
        return binarySearch(a, key, 1, a.ordinals().getNumOrds());
    }

    final protected static long binarySearch(BytesValues.WithOrdinals a, BytesRef key, long low, long high) {
        assert low != Ordinals.MISSING_ORDINAL;
        assert high == Ordinals.MISSING_ORDINAL || (a.getValueByOrd(high) == null | a.getValueByOrd(high) != null); // make sure we actually can get these values
        assert low == high + 1 || a.getValueByOrd(low) == null | a.getValueByOrd(low) != null;
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

    static long getRelevantOrd(Ordinals.Docs readerOrds, int docId, SortMode sortMode) {
        int length = readerOrds.setDocument(docId);
        long relevantVal = sortMode.startLong();
        long result = 0;
        assert sortMode == SortMode.MAX || sortMode == SortMode.MIN;
        for (int i = 0; i < length; i++) {
            result = relevantVal = sortMode.apply(readerOrds.nextOrd(), relevantVal);
        }
        assert result >= 0;
        assert result <= readerOrds.getMaxOrd();
        return result;
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

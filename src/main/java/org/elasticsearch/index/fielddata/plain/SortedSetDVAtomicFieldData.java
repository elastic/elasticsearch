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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.IntArrays;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;

import java.io.IOException;

/**
 * {@link AtomicFieldData} impl based on Lucene's {@link SortedSetDocValues}.
 * <p><b>Implementation note</b>: Lucene's ordinal for unset values is -1 whereas Elasticsearch's is 0, this is why there are all
 * these +1 to translate from Lucene's ordinals to ES's.
 */
abstract class SortedSetDVAtomicFieldData {

    private final AtomicReader reader;
    private final String field;
    private volatile IntArray hashes;

    SortedSetDVAtomicFieldData(AtomicReader reader, String field) {
        this.reader = reader;
        this.field = field;
    }

    public boolean isMultiValued() {
        // we could compute it when loading the values for the first time and then cache it but it would defeat the point of
        // doc values which is to make loading faster
        return true;
    }

    public int getNumDocs() {
        return reader.maxDoc();
    }

    public long getNumberUniqueValues() {
        final SortedSetDocValues values = getValuesNoException(reader, field);
        return values.getValueCount();
    }

    public long getMemorySizeInBytes() {
        // There is no API to access memory usage per-field and RamUsageEstimator can't help since there are often references
        // from a per-field instance to all other instances handled by the same format
        return -1L;
    }

    public void close() {
        // no-op
    }

    public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getBytesValues() {
        final SortedSetDocValues values = getValuesNoException(reader, field);
        return new SortedSetValues(reader, field, values);
    }

    public org.elasticsearch.index.fielddata.BytesValues.WithOrdinals getHashedBytesValues() {
        final SortedSetDocValues values = getValuesNoException(reader, field);
        if (hashes == null) {
            synchronized (this) {
                if (hashes == null) {
                    final long valueCount = values.getValueCount();
                    final IntArray hashes = IntArrays.allocate(1L + valueCount);
                    BytesRef scratch = new BytesRef(16);
                    hashes.set(0, scratch.hashCode());
                    for (long i = 0; i < valueCount; ++i) {
                        values.lookupOrd(i, scratch);
                        hashes.set(1L + i, scratch.hashCode());
                    }
                    this.hashes = hashes;
                }
            }
        }
        return new SortedSetHashedValues(reader, field, values, hashes);
    }

    private static SortedSetDocValues getValuesNoException(AtomicReader reader, String field) {
        try {
            SortedSetDocValues values = reader.getSortedSetDocValues(field);
            if (values == null) {
                // This field has not been populated
                assert reader.getFieldInfos().fieldInfo(field) == null;
                values = SortedSetDocValues.EMPTY;
            }
            return values;
        } catch (IOException e) {
            throw new ElasticSearchIllegalStateException("Couldn't load doc values", e);
        }
    }

    static abstract class AbstractSortedSetValues extends BytesValues.WithOrdinals {

        protected final SortedSetDocValues values;
        protected BytesValues.Iter.Multi iter;

        AbstractSortedSetValues(AtomicReader reader, String field, SortedSetDocValues values) {
            super(new SortedSetDocs(new SortedSetOrdinals(reader, field, values.getValueCount()), values));
            this.values = values;
        }

        @Override
        public BytesRef getValueScratchByOrd(long ord, BytesRef ret) {
            if (ord == 0) {
                ret.length = 0;
                return ret;
            }
            values.lookupOrd(ord - 1, ret);
            return ret;
        }

        @Override
        public Iter getIter(int docId) {
            return iter.reset(ordinals.getIter(docId));
        }

    }

    static class SortedSetValues extends AbstractSortedSetValues {

        SortedSetValues(AtomicReader reader, String field, SortedSetDocValues values) {
            super(reader, field, values);
            this.iter = new Iter.Multi(this);
        }

    }

    static class SortedSetHashedValues extends AbstractSortedSetValues {

        private final IntArray hashes;

        SortedSetHashedValues(AtomicReader reader, String field, SortedSetDocValues values, IntArray hashes) {
            super(reader, field, values);
            this.hashes = hashes;
            this.iter = new Iter.Multi(this) {
                @Override
                public int hash() {
                    return SortedSetHashedValues.this.hashes.get(ord);
                }
            };
        }

        @Override
        public int getValueHashed(int docId, BytesRef spare) {
            long ord = ordinals.getOrd(docId);
            getValueScratchByOrd(ord, spare);
            return hashes.get(ord);
        }

    }

    static class SortedSetOrdinals implements Ordinals {

        // We don't store SortedSetDocValues as a member because Ordinals must be thread-safe
        private final AtomicReader reader;
        private final String field;
        private final long numOrds;

        public SortedSetOrdinals(AtomicReader reader, String field, long numOrds) {
            super();
            this.reader = reader;
            this.field = field;
            this.numOrds = numOrds;
        }

        @Override
        public boolean hasSingleArrayBackingStorage() {
            return false;
        }

        @Override
        public Object getBackingStorage() {
            return null;
        }

        @Override
        public long getMemorySizeInBytes() {
            // Ordinals can't be distinguished from the atomic field data instance
            return -1;
        }

        @Override
        public boolean isMultiValued() {
            return true;
        }

        @Override
        public int getNumDocs() {
            return reader.maxDoc();
        }

        @Override
        public long getNumOrds() {
            return numOrds;
        }

        @Override
        public long getMaxOrd() {
            return 1 + numOrds;
        }

        @Override
        public Docs ordinals() {
            final SortedSetDocValues values = getValuesNoException(reader, field);
            assert values.getValueCount() == numOrds;
            return new SortedSetDocs(this, values);
        }

    }

    static class SortedSetDocs implements Ordinals.Docs {

        private final SortedSetOrdinals ordinals;
        private final SortedSetDocValues values;
        private final LongsRef longScratch;
        private final LongsIter iter = new LongsIter();

        SortedSetDocs(SortedSetOrdinals ordinals, SortedSetDocValues values) {
            this.ordinals = ordinals;
            this.values = values;
            longScratch = new LongsRef(8);
        }

        @Override
        public Ordinals ordinals() {
            return ordinals;
        }

        @Override
        public int getNumDocs() {
            return ordinals.getNumDocs();
        }

        @Override
        public long getNumOrds() {
            return ordinals.getNumOrds();
        }

        @Override
        public long getMaxOrd() {
            return ordinals.getMaxOrd();
        }

        @Override
        public boolean isMultiValued() {
            return ordinals.isMultiValued();
        }

        @Override
        public long getOrd(int docId) {
            values.setDocument(docId);
            return 1 + values.nextOrd();
        }

        @Override
        public LongsRef getOrds(int docId) {
            values.setDocument(docId);
            longScratch.offset = 0;
            longScratch.length = 0;
            for (long ord = values.nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values.nextOrd()) {
                longScratch.longs = ArrayUtil.grow(longScratch.longs, longScratch.length + 1);
                longScratch.longs[longScratch.length++] = 1 + ord;
            }
            return longScratch;
        }

        @Override
        public Iter getIter(int docId) {
            // For now, we consume all ords and pass them to the iter instead of doing it in a streaming way because Lucene's
            // SORTED_SET doc values are cached per thread, you can't have a fully independent instance
            iter.reset(getOrds(docId));
            return iter;
        }

    }

    static class LongsIter implements Ordinals.Docs.Iter {

        private LongsRef ords;
        private int i;

        @Override
        public long next() {
            if (i == ords.length) {
                return 0L;
            }
            return ords.longs[i++];
        }

        public void reset(LongsRef ords) {
            this.ords = ords;
            assert ords.offset == 0;
            i = 0;
        }

    }

}

package org.elasticsearch.index.fielddata.ordinals;
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
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.IntBlockPool.Allocator;
import org.apache.lucene.util.IntBlockPool.DirectAllocator;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.util.IntArrayRef;

/**
 * Simple class to build document ID <-> ordinal mapping. Note: Ordinals are
 * <tt>1</tt> based monotocially increasing positive integers. <tt>0</tt>
 * donates the missing value in this context.
 */
public final class OrdinalsBuilder implements Closeable {

    private final int[] ords;
    private int[] offsets;
    private final IntBlockPool pool;
    private final IntBlockPool.SliceWriter writer;
    private final IntArrayRef intsRef = new IntArrayRef(new int[1]);
    private final IntBlockPool.SliceReader reader;
    private int currentOrd = 0;
    private int numDocsWithValue = 0;
    private int numMultiValuedDocs = 0;
    private int totalNumOrds = 0;

    public OrdinalsBuilder(Terms terms, int maxDoc, Allocator allocator) {
        this.ords = new int[maxDoc];
        pool = new IntBlockPool(allocator);
        reader = new IntBlockPool.SliceReader(pool);
        writer = new IntBlockPool.SliceWriter(pool);
    }
    
    public OrdinalsBuilder(int maxDoc) {
        this(null, maxDoc);
    }

    public OrdinalsBuilder(Terms terms, int maxDoc) {
        this(terms, maxDoc, new DirectAllocator());
    }

    /**
     * Advances the {@link OrdinalsBuilder} to the next ordinal and
     * return the current ordinal.
     */
    public int nextOrdinal() {
        return ++currentOrd;
    }
    
    /**
     * Retruns the current ordinal or <tt>0</tt> if this build has not been advanced via
     * {@link #nextOrdinal()}.
     */
    public int currentOrdinal() {
        return currentOrd;
    }

    /**
     * Associates the given document id with the current ordinal. 
     */
    public OrdinalsBuilder addDoc(int doc) {
        totalNumOrds++;
        int docsOrd = ords[doc];
        if (docsOrd == 0) {
            ords[doc] = currentOrd;
            numDocsWithValue++;
        } else if (docsOrd > 0) {
            numMultiValuedDocs++;
            int offset = writer.startNewSlice();
            writer.writeInt(docsOrd);
            writer.writeInt(currentOrd);
            if (offsets == null) {
                offsets = new int[ords.length];
            }
            offsets[doc] = writer.getCurrentOffset();
            ords[doc] = (-1 * offset) - 1;
        } else {
            assert offsets != null;
            writer.reset(offsets[doc]);
            writer.writeInt(currentOrd);
            offsets[doc] = writer.getCurrentOffset();
        }
        return this;
    }

    /**
     * Returns <code>true</code> iff this builder contains a document ID that is associated with more than one ordinal. Otherwise <code>false</code>;
     */
    public boolean isMultiValued() {
        return offsets != null;
    }

    /**
     * Returns the number distinct of document IDs with one or more values.
     */
    public int getNumDocsWithValue() {
        return numDocsWithValue;
    }

    /**
     * Returns the number distinct of document IDs associated with exactly one value.
     */
    public int getNumSingleValuedDocs() {
        return numDocsWithValue - numMultiValuedDocs;
    }

    /**
     * Returns the number distinct of document IDs associated with two or more values.
     */
    public int getNumMultiValuesDocs() {
        return numMultiValuedDocs;
    }

    /**
     * Returns the number of document ID to ordinal pairs in this builder.
     */
    public int getTotalNumOrds() {
        return totalNumOrds;
    }

    /**
     * Returns the number of distinct ordinals in this builder.  
     */
    public int getNumOrds() {
        return currentOrd;
    }

    /**
     * Builds a {@link FixedBitSet} where each documents bit is that that has one or more ordinals associated with it.
     * if every document has an ordinal associated with it this method returns <code>null</code>
     */
    public FixedBitSet buildDocsWithValuesSet() {
        if (numDocsWithValue == this.ords.length)
            return null;
        final FixedBitSet bitSet = new FixedBitSet(this.ords.length);
        for (int i = 0; i < ords.length; i++) {
            if (ords[i] != 0) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    /**
     * Builds an {@link Ordinals} instance from the builders current state. 
     */
    public Ordinals build(Settings settings) {
        if (numMultiValuedDocs == 0) {
            return new SingleArrayOrdinals(ords, getNumOrds());
        }
        final String multiOrdinals = settings.get("multi_ordinals", "sparse");
        if ("flat".equals(multiOrdinals)) {
            final ArrayList<int[]> ordinalBuffer = new ArrayList<int[]>();
            for (int i = 0; i < ords.length; i++) {
                IntArrayRef docOrds = docOrds(i);
                while (ordinalBuffer.size() < docOrds.size()) {
                    ordinalBuffer.add(new int[ords.length]);
                }
                for (int j = docOrds.start; j < docOrds.end; j++) {
                    ordinalBuffer.get(j)[i] = docOrds.values[j];
                }
            }
            int[][] nativeOrdinals = new int[ordinalBuffer.size()][];
            for (int i = 0; i < nativeOrdinals.length; i++) {
                nativeOrdinals[i] = ordinalBuffer.get(i);
            }
            return new MultiFlatArrayOrdinals(nativeOrdinals, getNumOrds());
        } else if ("sparse".equals(multiOrdinals)) {
            int multiOrdinalsMaxDocs = settings.getAsInt("multi_ordinals_max_docs", 16777216 /* Equal to 64MB per storeage array */);
            return new SparseMultiArrayOrdinals(this, multiOrdinalsMaxDocs);
        } else {
            throw new ElasticSearchIllegalArgumentException("no applicable fielddata multi_ordinals value, got [" + multiOrdinals + "]");
        }
    }

    /**
     * Returns a shared {@link IntArrayRef} instance for the given doc ID holding all ordinals associated with it.
     */
    public IntArrayRef docOrds(int doc) {
        int docsOrd = ords[doc];
        intsRef.start = 0;
        if (docsOrd == 0) {
            intsRef.end = 0;
        } else if (docsOrd > 0) {
            intsRef.values[0] = ords[doc];
            intsRef.end = 1;
        } else {
            assert offsets != null;
            reader.reset(-1 * (ords[doc] + 1), offsets[doc]);
            int pos = 0;
            while (!reader.endOfSlice()) {
                if (intsRef.values.length <= pos) {
                    intsRef.values = ArrayUtil.grow(intsRef.values, pos + 1);
                }
                intsRef.values[pos++] = reader.readInt();
            }
            intsRef.end = pos;
        }
        return intsRef;
    }

    /**
     * Returns the maximum document ID this builder can associate with an ordinal
     */
    public int maxDoc() {
        return ords.length;
    }
    
    /**
     * A {@link TermsEnum} that iterates only full precision prefix coded 64 bit values.
     * @see #buildFromTerms(TermsEnum, Bits)
     */
    public TermsEnum wrapNumeric64Bit(TermsEnum termsEnum) {
        return new FilteredTermsEnum(termsEnum, false) {
            @Override
            protected AcceptStatus accept(BytesRef term) throws IOException {
                if (NumericUtils.getPrefixCodedLongShift(term) > 0) {
                    return AcceptStatus.END;
                } // we stop accepting terms once we moved across the prefix codec terms - redundant values!
                return AcceptStatus.YES;
            }
        };
    }

    /**
     * A {@link TermsEnum} that iterates only full precision prefix coded 32 bit values.
     * @see #buildFromTerms(TermsEnum, Bits)
     */
    public TermsEnum wrapNumeric32Bit(TermsEnum termsEnum) {
        return new FilteredTermsEnum(termsEnum, false) {
            
            @Override
            protected AcceptStatus accept(BytesRef term) throws IOException {
                if (NumericUtils.getPrefixCodedIntShift(term) > 0) {
                    return AcceptStatus.END;
                } // we stop accepting terms once we moved across the prefix codec terms - redundant values!
                return AcceptStatus.YES;
            }
        };
    }

    /**
     * This method iterates all terms in the given {@link TermsEnum} and
     * associates each terms ordinal with the terms documents. The caller must
     * exhaust the returned {@link BytesRefIterator} which returns all values
     * where the first returned value is associted with the ordinal <tt>1</tt>
     * etc.
     * <p>
     * If the {@link TermsEnum} contains prefix coded numerical values the terms
     * enum should be wrapped with either {@link #wrapNumeric32Bit(TermsEnum)}
     * or {@link #wrapNumeric64Bit(TermsEnum)} depending on its precision. If
     * the {@link TermsEnum} is not wrapped the returned
     * {@link BytesRefIterator} will contain partial precision terms rather than
     * only full-precision terms.
     * </p>
     */
    public BytesRefIterator buildFromTerms(final TermsEnum termsEnum, final Bits liveDocs) throws IOException {
        return new BytesRefIterator() {
            private DocsEnum docsEnum = null;

            @Override
            public BytesRef next() throws IOException {
                BytesRef ref;
                if ((ref = termsEnum.next()) != null) {
                    docsEnum = termsEnum.docs(liveDocs, docsEnum, DocsEnum.FLAG_NONE);
                    nextOrdinal();
                    int docId;
                    while((docId = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
                        addDoc(docId);
                    }
                }
                return ref;
            }

            @Override
            public Comparator<BytesRef> getComparator() {
                return termsEnum.getComparator();
            }
        };
    }
    
    /**
     * Closes this builder and release all resources.
     */
    @Override
    public void close() throws IOException {
        pool.reset(true, false);
        offsets = null;
    }
}

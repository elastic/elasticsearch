/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.XDocIdSetIterator;

import java.io.IOException;

/**
 */
public class DocIdSets {

    /**
     * Return the size of the doc id set, plus a reference to it.
     */
    public static long sizeInBytes(DocIdSet docIdSet) {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF + docIdSet.ramBytesUsed();
    }

    /**
     * Is it an empty {@link DocIdSet}?
     */
    public static boolean isEmpty(@Nullable DocIdSet set) {
        return set == null || set == DocIdSet.EMPTY;
    }

    /**
     * Check if the given iterator can nextDoc() or advance() in sub-linear time
     * of the number of documents. For instance, an iterator that would need to
     * iterate one document at a time to check for its value would be considered
     * broken.
     */
    public static boolean isBroken(DocIdSetIterator iterator) {
        while (iterator instanceof FilteredDocIdSetIterator) {
            // this iterator is filtered (likely by some bits)
            // unwrap in order to check if the underlying iterator is fast
            iterator = ((FilteredDocIdSetIterator) iterator).getDelegate();
        }
        if (iterator instanceof XDocIdSetIterator) {
            return ((XDocIdSetIterator) iterator).isBroken();
        }
        if (iterator instanceof MatchDocIdSetIterator) {
            return true;
        }
        // DocValuesDocIdSet produces anonymous slow iterators
        if (iterator != null && DocValuesDocIdSet.class.equals(iterator.getClass().getEnclosingClass())) {
            return true;
        }
        return false;
    }

    /**
     * Converts to a cacheable {@link DocIdSet}
     * <p/>
     * This never returns <code>null</code>.
     */
    public static DocIdSet toCacheable(LeafReader reader, @Nullable DocIdSet set) throws IOException {
        if (set == null || set == DocIdSet.EMPTY) {
            return DocIdSet.EMPTY;
        }
        final DocIdSetIterator it = set.iterator();
        if (it == null) {
            return DocIdSet.EMPTY;
        }
        final int firstDoc = it.nextDoc();
        if (firstDoc == DocIdSetIterator.NO_MORE_DOCS) {
            return DocIdSet.EMPTY;
        }
        if (set instanceof BitDocIdSet) {
            return set;
        }

        final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(reader.maxDoc());
        builder.add(firstDoc);
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
           builder.add(doc);
        }

        return builder.build();
    }

    /**
     * Gets a set to bits.
     */
    public static Bits toSafeBits(LeafReader reader, @Nullable DocIdSet set) throws IOException {
        if (set == null) {
            return new Bits.MatchNoBits(reader.maxDoc());
        }
        Bits bits = set.bits();
        if (bits != null) {
            return bits;
        }
        DocIdSetIterator iterator = set.iterator();
        if (iterator == null) {
            return new Bits.MatchNoBits(reader.maxDoc());
        }
        return toBitSet(iterator, reader.maxDoc());
    }

    /**
     * Creates a {@link BitSet} from an iterator.
     */
    public static BitSet toBitSet(DocIdSetIterator iterator, int numBits) throws IOException {
        BitDocIdSet.Builder builder = new BitDocIdSet.Builder(numBits);
        builder.or(iterator);
        BitDocIdSet result = builder.build();
        if (result != null) {
            return result.bits();
        } else {
            return new SparseFixedBitSet(numBits);
        }
    }

}

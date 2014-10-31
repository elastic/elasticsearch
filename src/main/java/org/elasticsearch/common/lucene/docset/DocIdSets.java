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
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.common.Nullable;

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
     * Is {@link org.apache.lucene.search.DocIdSetIterator} implemented in a "fast" manner.
     * For example, it does not ends up iterating one doc at a time check for its "value".
     */
    public static boolean isFastIterator(DocIdSet set) {
        // TODO: this is really horrible
        while (set instanceof BitsFilteredDocIdSet) {
            set = ((BitsFilteredDocIdSet) set).getDelegate();
        }
        return set instanceof BitDocIdSet || set instanceof RoaringDocIdSet;
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
        return toFixedBitSet(iterator, reader.maxDoc());
    }

    /**
     * Creates a {@link FixedBitSet} from an iterator.
     */
    public static FixedBitSet toFixedBitSet(DocIdSetIterator iterator, int numBits) throws IOException {
        FixedBitSet set = new FixedBitSet(numBits);
        set.or(iterator);
        return set;
    }
    
    /**
     * Creates a new DocIDSet if you have no idea of the cardinality,
     * and are afraid of the cost of computing the cost.
     * @deprecated remove usages of this.
     */
    @Deprecated
    public static BitDocIdSet newDocIDSet(BitSet bs) {
        if (bs == null) {
            return null;
        }
        final int cost;
        if (bs instanceof FixedBitSet) {
            cost = guessCost((FixedBitSet) bs);
        } else {
            cost = bs.approximateCardinality();
        }
        return new BitDocIdSet(bs, cost);
    }
    
    // nocommit: we should instead base this always on cost of clauses and stuff???
    private static int guessCost(FixedBitSet bs) {
        if (bs.length() < 8192) {
            return bs.cardinality();
        } else {
            return bs.length() / 8192 * new FixedBitSet(bs.getBits(), 8192).cardinality();
        }
    }
}

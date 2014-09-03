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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.WAH8DocIdSet;
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 * Utility methods to work with {@link DocIdSet}s.
 */
public class DocIdSets {

    /**
     * Return the size of the doc id set, plus a reference to it.
     */
    public static long sizeInBytes(DocIdSet docIdSet) {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF + docIdSet.ramBytesUsed();
    }

    /**
     * Is it an empty {@link DocIdSet}? This is best effort: a return value of
     * <tt>false</tt> does not mean that the set contains at least one
     * document.
     */
    public static boolean isEmpty(@Nullable DocIdSet set) {
        return set == null || set == DocIdSet.EMPTY;
    }

    /**
     * In case a {@link DocIdSet} provides both sequential and random access,
     * return whether using the iterator is faster than checking each document
     * individually in the random-access view.
     */
    public static boolean hasFasterIteratorThanRandomAccess(DocIdSet set) {
        // In the case of FixedBitSet, the iterator is faster since it can
        // check up to 64 bits at a time. However the other random-access sets
        // (mainly script or geo filters) don't make iteration faster.
        return set instanceof FixedBitSet;
    }

    /**
     * Converts to a cacheable {@link DocIdSet}
     * <p/>
     * We effectively always either return an empty {@link DocIdSet} or
     * {@link FixedBitSet} but never <tt>null</tt>.
     */
    public static DocIdSet toCacheable(AtomicReader reader, @Nullable DocIdSet set) throws IOException {
        if (set == null) {
            return DocIdSet.EMPTY;
        }
        // Some filters return doc id sets that are already cacheable. This is
        // for instance the case of the terms filter which creates fixed bitsets
        // In that case don't spend time converting it.
        if (set.isCacheable()) { // covers DocIdSet.EMPTY as well
            return set;
        }
        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return DocIdSet.EMPTY;
        }
        int doc = it.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            return DocIdSet.EMPTY;
        }
        // We use WAH8DocIdSet like Lucene does by default
        // Compared to FixedBitset, it doesn't have random access but is faster
        // to iterate on, better compressed, and skips faster thanks to an index
        WAH8DocIdSet.Builder builder = new WAH8DocIdSet.Builder();
        builder.add(doc);
        builder.add(it);
        return builder.build();
    }

    /**
     * Gives random-access to a {@link DocIdSet}, potentially copying the
     * {@link DocIdSet} to another data-structure that gives random access.
     */
    public static Bits toSafeBits(AtomicReader reader, @Nullable DocIdSet set) throws IOException {
        if (isEmpty(set)) {
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
        int doc;
        while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            set.set(doc);
        }
        return set;
    }
}

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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSetIterator;
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 */
public class DocIdSets {

    /**
     * Is it an empty {@link DocIdSet}?
     */
    public static boolean isEmpty(@Nullable DocIdSet set) {
        return set == null || set == DocIdSet.EMPTY_DOCIDSET;
    }

    /**
     * Is {@link org.apache.lucene.search.DocIdSetIterator} implemented in a "fast" manner.
     * For example, it does not ends up iterating one doc at a time check for its "value".
     */
    public static boolean isFastIterator(DocIdSet set) {
        return set instanceof FixedBitSet;
    }

    /**
     * Is {@link org.apache.lucene.search.DocIdSetIterator} implemented in a "fast" manner.
     * For example, it does not ends up iterating one doc at a time check for its "value".
     */
    public static boolean isFastIterator(DocIdSetIterator iterator) {
        // this is the iterator in the FixedBitSet.
        return iterator instanceof OpenBitSetIterator;
    }

    /**
     * Converts to a cacheable {@link DocIdSet}
     * <p/>
     * Note, we don't use {@link org.apache.lucene.search.DocIdSet#isCacheable()} because execution
     * might be expensive even if its cacheable (i.e. not going back to the reader to execute). We effectively
     * always either return {@link DocIdSet#EMPTY_DOCIDSET} or {@link FixedBitSet}.
     */
    public static DocIdSet toCacheable(AtomicReader reader, @Nullable DocIdSet set) throws IOException {
        if (set == null) {
            return DocIdSet.EMPTY_DOCIDSET;
        }
        if (set == DocIdSet.EMPTY_DOCIDSET) {
            return DocIdSet.EMPTY_DOCIDSET;
        }
        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return DocIdSet.EMPTY_DOCIDSET;
        }
        int doc = it.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            return DocIdSet.EMPTY_DOCIDSET;
        }
        if (set instanceof FixedBitSet) {
            return set;
        }
        return toFixedBitSet(set.iterator(), reader.maxDoc());
    }

    /**
     * Gets a set to bits.
     */
    public static Bits toSafeBits(AtomicReader reader, @Nullable DocIdSet set) throws IOException {
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
        return new FixedBitSet(toFixedBitSet(iterator, reader.maxDoc()));
    }

    /**
     * Creates a {@link FixedBitSet} from an iterator.
     */
    public static FixedBitSet toFixedBitSet(DocIdSetIterator iteartor, int numBits) throws IOException {
        FixedBitSet set = new FixedBitSet(numBits);
        int doc;
        while ((doc = iteartor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            set.set(doc);
        }
        return set;
    }
}

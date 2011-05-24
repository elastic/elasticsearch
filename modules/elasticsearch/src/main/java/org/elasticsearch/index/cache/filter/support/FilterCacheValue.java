/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.cache.filter.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lab.LongsLAB;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.docset.OpenBitDocSet;
import org.elasticsearch.common.lucene.docset.SlicedOpenBitSet;

import java.io.IOException;

public class FilterCacheValue<T> {

    private final T value;
    private final LongsLAB longsLAB;

    public FilterCacheValue(T value, LongsLAB longsLAB) {
        this.value = value;
        this.longsLAB = longsLAB;
    }

    public T value() {
        return value;
    }

    public LongsLAB longsLAB() {
        return longsLAB;
    }


    public static DocSet cacheable(IndexReader reader, @Nullable LongsLAB longsLAB, DocIdSet set) throws IOException {
        if (set == null) {
            return DocSet.EMPTY_DOC_SET;
        }
        if (set == DocIdSet.EMPTY_DOCIDSET) {
            return DocSet.EMPTY_DOC_SET;
        }

        DocIdSetIterator it = set.iterator();
        if (it == null) {
            return DocSet.EMPTY_DOC_SET;
        }
        int doc = it.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
            return DocSet.EMPTY_DOC_SET;
        }

        // we have a LAB, check if can be used...
        if (longsLAB == null) {
            return DocSets.cacheable(reader, set);
        }

        int numOfWords = OpenBitSet.bits2words(reader.maxDoc());
        LongsLAB.Allocation allocation = longsLAB.allocateLongs(numOfWords);
        if (allocation == null) {
            return DocSets.cacheable(reader, set);
        }
        // we have an allocation, use it to create SlicedOpenBitSet
        if (set instanceof OpenBitSet) {
            return new SlicedOpenBitSet(allocation.getData(), allocation.getOffset(), (OpenBitSet) set);
        } else if (set instanceof OpenBitDocSet) {
            return new SlicedOpenBitSet(allocation.getData(), allocation.getOffset(), ((OpenBitDocSet) set).set());
        } else {
            SlicedOpenBitSet slicedSet = new SlicedOpenBitSet(allocation.getData(), numOfWords, allocation.getOffset());
            slicedSet.fastSet(doc); // we already have an open iterator, so use it, and don't forget to set the initial one
            while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                slicedSet.fastSet(doc);
            }
            return slicedSet;
        }
    }
}
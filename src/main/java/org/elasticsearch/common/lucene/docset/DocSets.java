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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.Nullable;

import java.io.IOException;

/**
 */
public class DocSets {

    public static FixedBitSet createFixedBitSet(DocIdSetIterator disi, int numBits) throws IOException {
        FixedBitSet set = new FixedBitSet(numBits);
        int doc;
        while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            set.set(doc);
        }
        return set;
    }

    public static void or(FixedBitSet into, DocIdSet other) throws IOException {
        if (other == null) {
            return;
        }
        if (other instanceof FixedBitSet) {
            into.or((FixedBitSet) other);
        } else if (other instanceof FixedBitDocSet) {
            into.or(((FixedBitDocSet) other).set());
        } else {
            DocIdSetIterator disi = other.iterator();
            if (disi != null) {
                into.or(disi);
            }
        }
    }

    public static void and(FixedBitSet into, DocIdSet other) throws IOException {
        if (other instanceof FixedBitDocSet) {
            other = ((FixedBitDocSet) other).set();
        }
        if (other instanceof FixedBitSet) {
            into.and((FixedBitSet) other);
        } else {
            if (other == null) {
                into.clear(0, into.length());
            } else {
                // copied from OpenBitSetDISI#inPlaceAnd
                DocIdSetIterator disi = other.iterator();
                if (disi == null) {
                    into.clear(0, into.length());
                } else {
                    into.and(disi);
                }
            }
        }
    }

    public static void andNot(FixedBitSet into, DocIdSet other) throws IOException {
        if (other == null) {
            return;
        }
        if (other instanceof FixedBitDocSet) {
            other = ((FixedBitDocSet) other).set();
        }
        if (other instanceof FixedBitSet) {
            into.andNot((FixedBitSet) other);
        } else {
            // copied from OpenBitSetDISI#inPlaceNot
            DocIdSetIterator disi = other.iterator();
            if (disi != null) {
                into.andNot(disi);
            }
        }
    }

    public static DocSet convert(IndexReader reader, DocIdSet docIdSet) throws IOException {
        if (docIdSet == null) {
            return DocSet.EMPTY_DOC_SET;
        } else if (docIdSet instanceof DocSet) {
            return (DocSet) docIdSet;
        } else if (docIdSet instanceof FixedBitSet) {
            return new FixedBitDocSet((FixedBitSet) docIdSet);
        } else if (docIdSet instanceof OpenBitSet) {
            return new OpenBitDocSet((OpenBitSet) docIdSet);
        } else {
            final DocIdSetIterator it = docIdSet.iterator();
            // null is allowed to be returned by iterator(),
            // in this case we wrap with the empty set,
            // which is cacheable.
            return (it == null) ? DocSet.EMPTY_DOC_SET : new FixedBitDocSet(createFixedBitSet(it, reader.maxDoc()));
        }
    }

    /**
     * Returns a cacheable version of the doc id set (might be the same instance provided as a parameter).
     */
    public static DocSet cacheable(IndexReader reader, @Nullable DocIdSet set) throws IOException {
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

        if (set.isCacheable() && (set instanceof DocSet)) {
            return (DocSet) set;
        }
        if (set instanceof FixedBitSet) {
            return new FixedBitDocSet((FixedBitSet) set);
        }
        if (set instanceof OpenBitSet) {
            return new OpenBitDocSet((OpenBitSet) set);
        }

        // work with the iterator...
        FixedBitSet fixedBitSet = new FixedBitSet(reader.maxDoc());
        fixedBitSet.set(doc);
        while ((doc = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            fixedBitSet.set(doc);
        }
        return new FixedBitDocSet(fixedBitSet);
    }

    private DocSets() {

    }

}

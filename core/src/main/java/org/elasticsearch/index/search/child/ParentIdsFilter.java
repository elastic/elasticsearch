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
package org.elasticsearch.index.search.child;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.apache.lucene.util.SuppressForbidden;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Advantages over using this filter over Lucene's TermsFilter in the parent child context:
 * 1) Don't need to copy all values over to a list from the id cache and then
 * copy all the ids values over to one continuous byte array. Should save a lot of of object creations and gcs..
 * 2) We filter docs by one field only.
 * @deprecated Use queries from the lucene/join module instead
 */
@SuppressForbidden(reason="Old p/c queries still use filters")
@Deprecated
final class ParentIdsFilter extends Filter {

    static Filter createShortCircuitFilter(BitSetProducer nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, SortedDocValues globalValues,
                                           LongBitSet parentOrds, long numFoundParents) {
        BytesRefHash parentIds= null;
        boolean constructed = false;
        try {
            parentIds = new BytesRefHash(numFoundParents, searchContext.bigArrays());
            for (long parentOrd = parentOrds.nextSetBit(0); parentOrd != -1; parentOrd = parentOrds.nextSetBit(parentOrd + 1)) {
                parentIds.add(globalValues.lookupOrd((int) parentOrd));
            }
            constructed = true;
        } finally {
            if (!constructed) {
                Releasables.close(parentIds);
            }
        }
        searchContext.addReleasable(parentIds, SearchContext.Lifetime.COLLECTION);
        return new ParentIdsFilter(parentType, nonNestedDocsFilter, parentIds);
    }

    static Filter createShortCircuitFilter(BitSetProducer nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, SortedDocValues globalValues,
                                           LongHash parentIdxs, long numFoundParents) {
        BytesRefHash parentIds = null;
        boolean constructed = false;
        try {
            parentIds = new BytesRefHash(numFoundParents, searchContext.bigArrays());
            for (int id = 0; id < parentIdxs.size(); id++) {
                parentIds.add(globalValues.lookupOrd((int) parentIdxs.get(id)));
            }
            constructed = true;
        } finally {
            if (!constructed) {
                Releasables.close(parentIds);
            }
        }
        searchContext.addReleasable(parentIds, SearchContext.Lifetime.COLLECTION);
        return new ParentIdsFilter(parentType, nonNestedDocsFilter, parentIds);
    }

    private final BytesRef parentTypeBr;
    private final BitSetProducer nonNestedDocsFilter;
    private final BytesRefHash parentIds;

    private ParentIdsFilter(String parentType, BitSetProducer nonNestedDocsFilter, BytesRefHash parentIds) {
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        this.parentTypeBr = new BytesRef(parentType);
        this.parentIds = parentIds;
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(UidFieldMapper.NAME);
        if (terms == null) {
            return null;
        }

        TermsEnum termsEnum = terms.iterator();
        BytesRefBuilder uidSpare = new BytesRefBuilder();
        BytesRef idSpare = new BytesRef();

        if (acceptDocs == null) {
            acceptDocs = context.reader().getLiveDocs();
        }

        BitSet nonNestedDocs = null;
        if (nonNestedDocsFilter != null) {
            nonNestedDocs = nonNestedDocsFilter.getBitSet(context);
        }

        PostingsEnum docsEnum = null;
        BitSet result = null;
        int size = (int) parentIds.size();
        for (int i = 0; i < size; i++) {
            parentIds.get(i, idSpare);
            BytesRef uid = Uid.createUidAsBytes(parentTypeBr, idSpare, uidSpare);
            if (termsEnum.seekExact(uid)) {
                docsEnum = termsEnum.postings(docsEnum, PostingsEnum.NONE);
                int docId;
                for (docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    if (acceptDocs == null || acceptDocs.get(docId)) {
                        break;
                    }
                }
                if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                    continue;
                }
                if (result == null) {
                    // very rough heuristic that tries to get an idea of the number of documents
                    // in the set based on the number of parent ids that we didn't find in this segment
                    final int expectedCardinality = size / (i + 1);
                    // similar heuristic to BitDocIdSet.Builder
                    if (expectedCardinality >= (context.reader().maxDoc() >>> 10)) {
                        result = new FixedBitSet(context.reader().maxDoc());
                    } else {
                        result = new SparseFixedBitSet(context.reader().maxDoc());
                    }
                }
                if (nonNestedDocs != null) {
                    docId = nonNestedDocs.nextSetBit(docId);
                }
                result.set(docId);
                assert docsEnum.advance(docId + 1) == DocIdSetIterator.NO_MORE_DOCS : "DocId " + docId + " should have been the last one but docId " + docsEnum.docID() + " exists.";
            }
        }
        return result == null ? null : new BitDocIdSet(result);
    }

    @Override
    public String toString(String field) {
        return "parentsFilter(type=" + parentTypeBr.utf8ToString() + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ParentIdsFilter other = (ParentIdsFilter) obj;
        return parentTypeBr.equals(other.parentTypeBr)
                && parentIds.equals(other.parentIds)
                && nonNestedDocsFilter.equals(nonNestedDocsFilter);
    }

    @Override
    public int hashCode() {
        int h = super.hashCode();
        h = 31 * h + parentTypeBr.hashCode();
        h = 31 * h + parentIds.hashCode();
        h = 31 * h + nonNestedDocsFilter.hashCode();
        return h;
    }
}
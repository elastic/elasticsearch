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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.Queries;
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
 */
final class ParentIdsFilter extends Filter {

    static Filter createShortCircuitFilter(BitDocIdSetFilter nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, SortedDocValues globalValues,
                                           LongBitSet parentOrds, long numFoundParents) {
        if (numFoundParents == 1) {
            BytesRef id = globalValues.lookupOrd((int) parentOrds.nextSetBit(0));
            if (nonNestedDocsFilter != null) {
                BooleanQuery bq = new BooleanQuery();
                bq.add(new TermQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))), Occur.MUST);
                bq.add(nonNestedDocsFilter, Occur.MUST);
                return Queries.wrap(bq);
            } else {
                return Queries.wrap(new TermQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))));
            }
        } else {
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
    }

    static Filter createShortCircuitFilter(BitDocIdSetFilter nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, SortedDocValues globalValues,
                                           LongHash parentIdxs, long numFoundParents) {
        if (numFoundParents == 1) {
            BytesRef id = globalValues.lookupOrd((int) parentIdxs.get(0));
            if (nonNestedDocsFilter != null) {
                BooleanQuery bq = new BooleanQuery();
                bq.add(new TermQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))), Occur.MUST);
                bq.add(nonNestedDocsFilter, Occur.MUST);
                return Queries.wrap(bq);
            } else {
                return Queries.wrap(new TermQuery(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))));
            }
        } else {
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
    }

    private final BytesRef parentTypeBr;
    private final BitDocIdSetFilter nonNestedDocsFilter;
    private final BytesRefHash parentIds;

    private ParentIdsFilter(String parentType, BitDocIdSetFilter nonNestedDocsFilter, BytesRefHash parentIds) {
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
            nonNestedDocs = nonNestedDocsFilter.getDocIdSet(context).bits();
        }

        PostingsEnum docsEnum = null;
        BitSet result = null;
        int size = (int) parentIds.size();
        for (int i = 0; i < size; i++) {
            parentIds.get(i, idSpare);
            BytesRef uid = Uid.createUidAsBytes(parentTypeBr, idSpare, uidSpare);
            if (termsEnum.seekExact(uid)) {
                int docId;
                docsEnum = termsEnum.postings(acceptDocs, docsEnum, PostingsEnum.NONE);
                if (result == null) {
                    docId = docsEnum.nextDoc();
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        // very rough heuristic that tries to get an idea of the number of documents
                        // in the set based on the number of parent ids that we didn't find in this segment
                        final int expectedCardinality = size / (i + 1);
                        // similar heuristic to BitDocIdSet.Builder
                        if (expectedCardinality >= (context.reader().maxDoc() >>> 10)) {
                            result = new FixedBitSet(context.reader().maxDoc());
                        } else {
                            result = new SparseFixedBitSet(context.reader().maxDoc());
                        }
                    } else {
                        continue;
                    }
                } else {
                    docId = docsEnum.nextDoc();
                    if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                        continue;
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
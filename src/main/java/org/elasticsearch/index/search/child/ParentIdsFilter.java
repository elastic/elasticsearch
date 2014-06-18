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

import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongBitSet;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Advantages over using this filter over Lucene's TermsFilter in the parent child context:
 * 1) Don't need to copy all values over to a list from the id cache and then
 * copy all the ids values over to one continuous byte array. Should save a lot of of object creations and gcs..
 * 2) We filter docs by one field only.
 */
final class ParentIdsFilter extends Filter {

    static Filter createShortCircuitFilter(Filter nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, BytesValues.WithOrdinals globalValues,
                                           LongBitSet parentOrds, long numFoundParents) {
        if (numFoundParents == 1) {
            BytesRef id = globalValues.getValueByOrd(parentOrds.nextSetBit(0));
            if (nonNestedDocsFilter != null) {
                List<Filter> filters = Arrays.asList(
                        new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))),
                        nonNestedDocsFilter
                );
                return new AndFilter(filters);
            } else {
                return new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            }
        } else {
            BytesRefHash parentIds= null;
            boolean constructed = false;
            try {
                parentIds = new BytesRefHash(numFoundParents, searchContext.bigArrays());
                for (long parentOrd = parentOrds.nextSetBit(0l); parentOrd != -1; parentOrd = parentOrds.nextSetBit(parentOrd + 1)) {
                    parentIds.add(globalValues.getValueByOrd(parentOrd));
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

    static Filter createShortCircuitFilter(Filter nonNestedDocsFilter, SearchContext searchContext,
                                           String parentType, BytesValues.WithOrdinals globalValues,
                                           LongHash parentIdxs, long numFoundParents) {
        if (numFoundParents == 1) {
            BytesRef id = globalValues.getValueByOrd(parentIdxs.get(0));
            if (nonNestedDocsFilter != null) {
                List<Filter> filters = Arrays.asList(
                        new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))),
                        nonNestedDocsFilter
                );
                return new AndFilter(filters);
            } else {
                return new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            }
        } else {
            BytesRefHash parentIds = null;
            boolean constructed = false;
            try {
                parentIds = new BytesRefHash(numFoundParents, searchContext.bigArrays());
                for (int id = 0; id < parentIdxs.size(); id++) {
                    parentIds.add(globalValues.getValueByOrd(parentIdxs.get(id)));
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
    private final Filter nonNestedDocsFilter;
    private final BytesRefHash parentIds;

    private ParentIdsFilter(String parentType, Filter nonNestedDocsFilter, BytesRefHash parentIds) {
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        this.parentTypeBr = new BytesRef(parentType);
        this.parentIds = parentIds;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        Terms terms = context.reader().terms(UidFieldMapper.NAME);
        if (terms == null) {
            return null;
        }

        TermsEnum termsEnum = terms.iterator(null);
        BytesRef uidSpare = new BytesRef();
        BytesRef idSpare = new BytesRef();

        if (acceptDocs == null) {
            acceptDocs = context.reader().getLiveDocs();
        }

        FixedBitSet nonNestedDocs = null;
        if (nonNestedDocsFilter != null) {
            nonNestedDocs = (FixedBitSet) nonNestedDocsFilter.getDocIdSet(context, acceptDocs);
        }

        DocsEnum docsEnum = null;
        FixedBitSet result = null;
        long size = parentIds.size();
        for (int i = 0; i < size; i++) {
            parentIds.get(i, idSpare);
            Uid.createUidAsBytes(parentTypeBr, idSpare, uidSpare);
            if (termsEnum.seekExact(uidSpare)) {
                int docId;
                docsEnum = termsEnum.docs(acceptDocs, docsEnum, DocsEnum.FLAG_NONE);
                if (result == null) {
                    docId = docsEnum.nextDoc();
                    if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        result = new FixedBitSet(context.reader().maxDoc());
                    } else {
                        continue;
                    }
                } else {
                    docId = docsEnum.nextDoc();
                    if (docId == DocIdSetIterator.NO_MORE_DOCS) {
                        continue;
                    }
                }
                if (nonNestedDocs != null && !nonNestedDocs.get(docId)) {
                    docId = nonNestedDocs.nextSetBit(docId);
                }
                result.set(docId);
                assert docsEnum.advance(docId + 1) == DocIdSetIterator.NO_MORE_DOCS : "DocId " + docId + " should have been the last one but docId " + docsEnum.docID() + " exists.";
            }
        }
        return result;
    }
}
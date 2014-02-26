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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class ChildrenConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        BytesRefHash parentIds = new BytesRefHash(512, searchContext.bigArrays());
        ParentIdCollector collector = new ParentIdCollector(parentType, parentChildIndexFieldData, parentIds);
        final Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        indexSearcher.search(childQuery, collector);

        long remaining = parentIds.size();
        if (remaining == 0) {
            Releasables.release(parentIds);
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        Filter shortCircuitFilter = null;
        if (remaining == 1) {
            BytesRef id = parentIds.get(0, new BytesRef());
            shortCircuitFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
        } else if (remaining <= shortCircuitParentDocSet) {
            shortCircuitFilter = new ParentIdsFilter(parentType, nonNestedDocsFilter, parentIds);
        }

        ParentWeight parentWeight = new ParentWeight(parentFilter, shortCircuitFilter, parentIds);
        searchContext.addReleasable(parentWeight);
        return parentWeight;
    }

    private final class ParentWeight extends Weight implements Releasable  {

        private final Filter parentFilter;
        private final Filter shortCircuitFilter;
        private final BytesRefHash parentIds;

        private long remaining;
        private float queryNorm;
        private float queryWeight;

        public ParentWeight(Filter parentFilter, Filter shortCircuitFilter, BytesRefHash parentIds) {
            this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
            this.shortCircuitFilter = shortCircuitFilter;
            this.parentIds = parentIds;
            this.remaining = parentIds.size();
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenConstantScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            queryWeight = getBoost();
            return queryWeight * queryWeight;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            if (remaining == 0) {
                return null;
            }

            if (shortCircuitFilter != null) {
                DocIdSet docIdSet = shortCircuitFilter.getDocIdSet(context, acceptDocs);
                if (!DocIdSets.isEmpty(docIdSet)) {
                    DocIdSetIterator iterator = docIdSet.iterator();
                    if (iterator != null) {
                        return ConstantScorer.create(iterator, this, queryWeight);
                    }
                }
                return null;
            }

            DocIdSet parentDocIdSet = this.parentFilter.getDocIdSet(context, acceptDocs);
            if (!DocIdSets.isEmpty(parentDocIdSet)) {
                BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
                // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
                // count down (short circuit) logic will then work as expected.
                parentDocIdSet = BitsFilteredDocIdSet.wrap(parentDocIdSet, context.reader().getLiveDocs());
                if (bytesValues != null) {
                    DocIdSetIterator innerIterator = parentDocIdSet.iterator();
                    if (innerIterator != null) {
                        ParentDocIdIterator parentDocIdIterator = new ParentDocIdIterator(innerIterator, parentIds, bytesValues);
                        return ConstantScorer.create(parentDocIdIterator, this, queryWeight);
                    }
                }
            }
            return null;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(parentIds);
            return true;
        }

        private final class ParentDocIdIterator extends FilteredDocIdSetIterator {

            private final BytesRefHash parentIds;
            private final BytesValues values;

            private ParentDocIdIterator(DocIdSetIterator innerIterator, BytesRefHash parentIds, BytesValues values) {
                super(innerIterator);
                this.parentIds = parentIds;
                this.values = values;
            }

            @Override
            protected boolean match(int doc) {
                if (remaining == 0) {
                    try {
                        advance(DocIdSetIterator.NO_MORE_DOCS);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }

                values.setDocument(doc);
                BytesRef parentId = values.nextValue();
                int hash = values.currentValueHash();
                boolean match = parentIds.find(parentId, hash) >= 0;
                if (match) {
                    remaining--;
                }
                return match;
            }
        }
    }

    private final static class ParentIdCollector extends NoopCollector {

        private final BytesRefHash parentIds;
        private final String parentType;
        private final ParentChildIndexFieldData indexFieldData;

        protected BytesValues.WithOrdinals values;
        private Ordinals.Docs ordinals;

        // This remembers what ordinals have already been seen in the current segment
        // and prevents from fetch the actual id from FD and checking if it exists in parentIds
        private FixedBitSet seenOrdinals;

        protected ParentIdCollector(String parentType, ParentChildIndexFieldData indexFieldData, BytesRefHash parentIds) {
            this.parentType = parentType;
            this.indexFieldData = indexFieldData;
            this.parentIds = parentIds;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values != null) {
                int ord = (int) ordinals.getOrd(doc);
                if (!seenOrdinals.get(ord)) {
                    final BytesRef bytes  = values.getValueByOrd(ord);
                    final int hash = values.currentValueHash();
                    parentIds.add(bytes, hash);
                    seenOrdinals.set(ord);
                }
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(parentType);
            if (values != null) {
                ordinals = values.ordinals();
                final int maxOrd = (int) ordinals.getMaxOrd();
                if (seenOrdinals == null || seenOrdinals.length() < maxOrd) {
                    seenOrdinals = new FixedBitSet(maxOrd);
                } else {
                    seenOrdinals.clear(0, maxOrd);
                }
            }

        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenConstantScoreQuery that = (ChildrenConstantScoreQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (shortCircuitParentDocSet != that.shortCircuitParentDocSet) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
        result = 31 * result + shortCircuitParentDocSet;
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery).append(')');
        return sb.toString();
    }

}

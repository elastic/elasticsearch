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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link ParentChildIndexFieldData}.
 */
public class ParentQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentQuery that = (ParentQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        return "ParentQuery[" + parentType + "](" + originalParentQuery.toString(field) + ')' + ToStringUtils.boost(getBoost());
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewriteIndexReader = reader;
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
        }
        return this;
    }

    @Override
    public Query clone() {
        ParentQuery q = (ParentQuery) super.clone();
        q.originalParentQuery = originalParentQuery.clone();
        if (q.rewrittenParentQuery != null) {
            q.rewrittenParentQuery = rewrittenParentQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        SearchContext sc = SearchContext.current();
        ChildWeight childWeight;
        boolean releaseCollectorResource = true;
        ParentOrdAndScoreCollector collector = null;
        IndexParentChildFieldData globalIfd = parentChildIndexFieldData.loadGlobal(searcher.getIndexReader());
        if (globalIfd == null) {
            // No docs of the specified type don't exist on this shard
            return new BooleanQuery().createWeight(searcher, needsScores);
        }

        try {
            assert rewrittenParentQuery != null;
            assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();
            final Query  parentQuery = rewrittenParentQuery;
            collector = new ParentOrdAndScoreCollector(sc, globalIfd, parentType);
            IndexSearcher indexSearcher = new IndexSearcher(sc.searcher().getIndexReader());
            indexSearcher.setSimilarity(searcher.getSimilarity());
            indexSearcher.search(parentQuery, collector);
            if (collector.parentCount() == 0) {
                return new BooleanQuery().createWeight(searcher, needsScores);
            }
            childWeight = new ChildWeight(this, parentQuery.createWeight(searcher, needsScores), childrenFilter, collector, globalIfd);
            releaseCollectorResource = false;
        } finally {
            if (releaseCollectorResource) {
                // either if we run into an exception or if we return early
                Releasables.close(collector);
            }
        }
        sc.addReleasable(collector, Lifetime.COLLECTION);
        return childWeight;
    }

    private static class ParentOrdAndScoreCollector extends NoopCollector implements Releasable {

        private final LongHash parentIdxs;
        private FloatArray scores;
        private final IndexParentChildFieldData globalIfd;
        private final BigArrays bigArrays;
        private final String parentType;

        private Scorer scorer;
        private SortedDocValues values;

        ParentOrdAndScoreCollector(SearchContext searchContext, IndexParentChildFieldData globalIfd, String parentType) {
            this.bigArrays = searchContext.bigArrays();
            this.parentIdxs = new LongHash(512, bigArrays);
            this.scores = bigArrays.newFloatArray(512, false);
            this.globalIfd = globalIfd;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (values != null) {
                long globalOrdinal = values.getOrd(doc);
                if (globalOrdinal != SortedSetDocValues.NO_MORE_ORDS) {
                    long parentIdx = parentIdxs.add(globalOrdinal);
                    if (parentIdx >= 0) {
                        scores = bigArrays.grow(scores, parentIdx + 1);
                        scores.set(parentIdx, scorer.score());
                    } else {
                        assert false : "parent id should only match once, since there can only be one parent doc";
                    }
                }
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            values = globalIfd.load(context).getOrdinalsValues(parentType);
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIdxs, scores);
        }

        public long parentCount() {
            return parentIdxs.size();
        }

    }

    private class ChildWeight extends Weight {

        private final Weight parentWeight;
        private final Filter childrenFilter;
        private final LongHash parentIdxs;
        private final FloatArray scores;
        private final IndexParentChildFieldData globalIfd;

        private ChildWeight(Query query, Weight parentWeight, Filter childrenFilter, ParentOrdAndScoreCollector collector, IndexParentChildFieldData globalIfd) {
            super(query);
            this.parentWeight = parentWeight;
            this.childrenFilter = childrenFilter;
            this.parentIdxs = collector.parentIdxs;
            this.scores = collector.scores;
            this.globalIfd = globalIfd;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return Explanation.match(getBoost(), "not implemented yet...");
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = parentWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocSet)) {
                return null;
            }
            SortedDocValues bytesValues = globalIfd.load(context).getOrdinalsValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            return new ChildScorer(this, parentIdxs, scores, childrenDocSet.iterator(), bytesValues);
        }

    }

    private static class ChildScorer extends Scorer {

        private final LongHash parentIdxs;
        private final FloatArray scores;
        private final DocIdSetIterator childrenIterator;
        private final SortedDocValues ordinals;

        private int currentChildDoc = -1;
        private float currentScore;

        ChildScorer(Weight weight, LongHash parentIdxs, FloatArray scores, DocIdSetIterator childrenIterator, SortedDocValues ordinals) {
            super(weight);
            this.parentIdxs = parentIdxs;
            this.scores = scores;
            this.childrenIterator = childrenIterator;
            this.ordinals = ordinals;
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        @Override
        public int freq() throws IOException {
            // We don't have the original child query hit info here...
            // But the freq of the children could be collector and returned here, but makes this Scorer more expensive.
            return 1;
        }

        @Override
        public int docID() {
            return currentChildDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            while (true) {
                currentChildDoc = childrenIterator.nextDoc();
                if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentChildDoc;
                }

                int globalOrdinal = (int) ordinals.getOrd(currentChildDoc);
                if (globalOrdinal < 0) {
                    continue;
                }

                final long parentIdx = parentIdxs.find(globalOrdinal);
                if (parentIdx != -1) {
                    currentScore = scores.get(parentIdx);
                    return currentChildDoc;
                }
            }
        }

        @Override
        public int advance(int target) throws IOException {
            currentChildDoc = childrenIterator.advance(target);
            if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                return currentChildDoc;
            }

            int globalOrdinal = (int) ordinals.getOrd(currentChildDoc);
            if (globalOrdinal < 0) {
                return nextDoc();
            }

            final long parentIdx = parentIdxs.find(globalOrdinal);
            if (parentIdx != -1) {
                currentScore = scores.get(parentIdx);
                return currentChildDoc;
            } else {
                return nextDoc();
            }
        }

        @Override
        public long cost() {
            return childrenIterator.cost();
        }
    }
}

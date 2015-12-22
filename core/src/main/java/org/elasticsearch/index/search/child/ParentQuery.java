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
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.IndexCacheableQuery;
import org.elasticsearch.common.lucene.Lucene;
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
 * @deprecated Use queries from lucene/join instead
 */
@Deprecated
@SuppressForbidden(reason="Old p/c queries still use filters")
public class ParentQuery extends IndexCacheableQuery {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private Query parentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    public ParentQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (super.equals(obj) == false) {
            return false;
        }

        ParentQuery that = (ParentQuery) obj;
        if (!parentQuery.equals(that.parentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + parentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        return "ParentQuery[" + parentType + "](" + parentQuery.toString(field) + ')' + ToStringUtils.boost(getBoost());
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query parentRewritten = parentQuery.rewrite(reader);
        if (parentRewritten != parentQuery) {
            Query rewritten = new ParentQuery(parentChildIndexFieldData, parentRewritten, parentType, childrenFilter);
            rewritten.setBoost(getBoost());
            return rewritten;
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight doCreateWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        SearchContext sc = SearchContext.current();
        ChildWeight childWeight;
        boolean releaseCollectorResource = true;
        ParentOrdAndScoreCollector collector = null;
        IndexParentChildFieldData globalIfd = parentChildIndexFieldData.loadGlobal((DirectoryReader)searcher.getIndexReader());
        if (globalIfd == null) {
            // No docs of the specified type don't exist on this shard
            return new BooleanQuery.Builder().build().createWeight(searcher, needsScores);
        }

        try {
            collector = new ParentOrdAndScoreCollector(sc, globalIfd, parentType);
            searcher.search(parentQuery, collector);
            if (collector.parentCount() == 0) {
                return new BooleanQuery.Builder().build().createWeight(searcher, needsScores);
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

    private static class ParentOrdAndScoreCollector implements Collector, Releasable {

        private final LongHash parentIdxs;
        private FloatArray scores;
        private final IndexParentChildFieldData globalIfd;
        private final BigArrays bigArrays;
        private final String parentType;

        ParentOrdAndScoreCollector(SearchContext searchContext, IndexParentChildFieldData globalIfd, String parentType) {
            this.bigArrays = searchContext.bigArrays();
            this.parentIdxs = new LongHash(512, bigArrays);
            this.scores = bigArrays.newFloatArray(512, false);
            this.globalIfd = globalIfd;
            this.parentType = parentType;
        }

        @Override
        public boolean needsScores() {
            return true;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final SortedDocValues values = globalIfd.load(context).getOrdinalsValues(parentType);
            if (values == null) {
                throw new CollectionTerminatedException();
            }
            return new LeafCollector() {
                Scorer scorer;
                @Override
                public void setScorer(Scorer scorer) throws IOException {
                    this.scorer = scorer;
                }
                @Override
                public void collect(int doc) throws IOException {
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
            };
        }

        @Override
        public void close() {
            Releasables.close(parentIdxs, scores);
        }

        public long parentCount() {
            return parentIdxs.size();
        }

    }

    @SuppressForbidden(reason="Old p/c queries still use filters")
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
        public Scorer scorer(LeafReaderContext context) throws IOException {
            DocIdSet childrenDocSet = childrenFilter.getDocIdSet(context, null);
            // we forcefully apply live docs here so that deleted children don't give matching parents
            childrenDocSet = BitsFilteredDocIdSet.wrap(childrenDocSet, context.reader().getLiveDocs());
            if (Lucene.isEmpty(childrenDocSet)) {
                return null;
            }
            final DocIdSetIterator childIterator = childrenDocSet.iterator();
            if (childIterator == null) {
                return null;
            }
            SortedDocValues bytesValues = globalIfd.load(context).getOrdinalsValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            return new ChildScorer(this, parentIdxs, scores, childIterator, bytesValues);
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
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {

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
            };
        }
    }
}

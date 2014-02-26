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
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.*;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A query implementation that executes the wrapped child query and connects all the matching child docs to the related
 * parent documents using {@link ParentChildIndexFieldData}.
 * <p/>
 * This query is executed in two rounds. The first round resolves all the matching child documents and groups these
 * documents by parent uid value. Also the child scores are aggregated per parent uid value. During the second round
 * all parent documents having the same uid value that is collected in the first phase are emitted as hit including
 * a score based on the aggregated child scores and score type.
 */
public class ChildrenQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final ScoreType scoreType;
    private final Query originalChildQuery;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenQuery(ParentChildIndexFieldData parentChildIndexFieldData, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentType = parentType;
        this.childType = childType;
        this.parentFilter = parentFilter;
        this.originalChildQuery = childQuery;
        this.scoreType = scoreType;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenQuery that = (ChildrenQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
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
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ChildrenQuery[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery
                .toString(field)).append(')').append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewriteIndexReader = reader;
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
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

        final Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());

        final BytesRefHash parentIds;
        final FloatArray scores;
        final IntArray occurrences;
        switch (scoreType) {
            case MAX:
                MaxCollector maxCollector = new MaxCollector(parentChildIndexFieldData, parentType, searchContext);
                indexSearcher.search(childQuery, maxCollector);
                parentIds = maxCollector.parentIds;
                scores = maxCollector.scores;
                occurrences = null;
                Releasables.release(maxCollector.parentIdsIndex);
                break;
            case SUM:
                SumCollector sumCollector = new SumCollector(parentChildIndexFieldData, parentType, searchContext);
                indexSearcher.search(childQuery, sumCollector);
                parentIds = sumCollector.parentIds;
                scores = sumCollector.scores;
                occurrences = null;
                Releasables.release(sumCollector.parentIdsIndex);
                break;
            case AVG:
                AvgCollector avgCollector = new AvgCollector(parentChildIndexFieldData, parentType, searchContext);
                indexSearcher.search(childQuery, avgCollector);
                parentIds = avgCollector.parentIds;
                scores = avgCollector.scores;
                occurrences = avgCollector.occurrences;
                Releasables.release(avgCollector.parentIdsIndex);
                break;
            default:
                throw new RuntimeException("Are we missing a score type here? -- " + scoreType);
        }

        int size = (int) parentIds.size();
        if (size == 0) {
            Releasables.release(parentIds, scores, occurrences);
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        final Filter parentFilter;
        if (size == 1) {
            BytesRef id = parentIds.get(0, new BytesRef());
            if (nonNestedDocsFilter != null) {
                List<Filter> filters = Arrays.asList(
                        new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))),
                        nonNestedDocsFilter
                );
                parentFilter = new AndFilter(filters);
            } else {
                parentFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            }
        } else if (size <= shortCircuitParentDocSet) {
            parentFilter = new ParentIdsFilter(parentType, nonNestedDocsFilter, parentIds);
        } else {
            parentFilter = new ApplyAcceptedDocsFilter(this.parentFilter);
        }
        ParentWeight parentWeight = new ParentWeight(rewrittenChildQuery.createWeight(searcher), parentFilter, size, parentIds, scores, occurrences);
        searchContext.addReleasable(parentWeight);
        return parentWeight;
    }

    private final class ParentWeight extends Weight implements Releasable {

        private final Weight childWeight;
        private final Filter parentFilter;
        private final BytesRefHash parentIds;
        private final FloatArray scores;
        private final IntArray occurrences;

        private int remaining;

        private ParentWeight(Weight childWeight, Filter parentFilter, int remaining, BytesRefHash parentIds, FloatArray scores, IntArray occurrences) {
            this.childWeight = childWeight;
            this.parentFilter = parentFilter;
            this.remaining = remaining;
            this.parentIds = parentIds;
            this.scores = scores;
            this.occurrences = occurrences;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = childWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet parentsSet = parentFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(parentsSet) || remaining == 0) {
                return null;
            }

            BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
            // count down (short circuit) logic will then work as expected.
            DocIdSetIterator parentsIterator = BitsFilteredDocIdSet.wrap(parentsSet, context.reader().getLiveDocs()).iterator();
            switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, bytesValues, parentIds, scores, occurrences, parentsIterator);
                default:
                    return new ParentScorer(this, bytesValues, parentIds, scores, parentsIterator);
            }
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(parentIds, scores, occurrences);
            return true;
        }

        private class ParentScorer extends Scorer {

            final BytesRefHash parentIds;
            final FloatArray scores;

            final BytesValues bytesValues;
            final DocIdSetIterator parentsIterator;

            int currentDocId = -1;
            float currentScore;

            ParentScorer(Weight weight, BytesValues bytesValues, BytesRefHash parentIds, FloatArray scores, DocIdSetIterator parentsIterator) {
                super(weight);
                this.bytesValues = bytesValues;
                this.parentsIterator = parentsIterator;
                this.parentIds = parentIds;
                this.scores = scores;
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
                return currentDocId;
            }

            @Override
            public int nextDoc() throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    bytesValues.setDocument(currentDocId);
                    long index = parentIds.find(bytesValues.nextValue(), bytesValues.currentValueHash());
                    if (index != -1) {
                        currentScore = scores.get(index);
                        remaining--;
                        return currentDocId;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                bytesValues.setDocument(currentDocId);
                long index = parentIds.find(bytesValues.nextValue(), bytesValues.currentValueHash());
                if (index != -1) {
                    currentScore = scores.get(index);
                    remaining--;
                    return currentDocId;
                } else {
                    return nextDoc();
                }
            }

            @Override
            public long cost() {
                return parentsIterator.cost();
            }
        }

        private final class AvgParentScorer extends ParentScorer {

            final IntArray occurrences;

            AvgParentScorer(Weight weight, BytesValues values, BytesRefHash parentIds, FloatArray scores, IntArray occurrences, DocIdSetIterator parentsIterator) {
                super(weight, values, parentIds, scores, parentsIterator);
                this.occurrences = occurrences;
            }

            @Override
            public int nextDoc() throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    bytesValues.setDocument(currentDocId);
                    long index = parentIds.find(bytesValues.nextValue(), bytesValues.currentValueHash());
                    if (index != -1) {
                        currentScore = scores.get(index);
                        currentScore /= occurrences.get(index);
                        remaining--;
                        return currentDocId;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                bytesValues.setDocument(currentDocId);
                long index = parentIds.find(bytesValues.nextValue(), bytesValues.currentValueHash());
                if (index != -1) {
                    currentScore = scores.get(index);
                    currentScore /= occurrences.get(index);
                    remaining--;
                    return currentDocId;
                } else {
                    return nextDoc();
                }
            }
        }

    }

    private abstract static class ParentIdAndScoreCollector extends NoopCollector {

        final BytesRefHash parentIds;
        protected final String parentType;
        private final ParentChildIndexFieldData indexFieldData;
        protected final BigArrays bigArrays;

        protected FloatArray scores;

        protected BytesValues.WithOrdinals values;
        protected Ordinals.Docs ordinals;
        protected Scorer scorer;

        // This remembers what ordinals have already been seen in the current segment
        // and prevents from fetch the actual id from FD and checking if it exists in parentIds
        protected LongArray parentIdsIndex;

        private ParentIdAndScoreCollector(ParentChildIndexFieldData indexFieldData, String parentType, SearchContext searchContext) {
            this.parentType = parentType;
            this.indexFieldData = indexFieldData;
            this.bigArrays = searchContext.bigArrays();
            this.parentIds = new BytesRefHash(512, bigArrays);
            this.scores = bigArrays.newFloatArray(512, false);
        }


        @Override
        public void collect(int doc) throws IOException {
            if (values != null) {
                long ord = ordinals.getOrd(doc);
                long parentIdx = parentIdsIndex.get(ord);
                if (parentIdx < 0) {
                    final BytesRef bytes  = values.getValueByOrd(ord);
                    final int hash = values.currentValueHash();
                    parentIdx = parentIds.add(bytes, hash);
                    if (parentIdx < 0) {
                        parentIdx = -parentIdx - 1;
                        doScore(parentIdx);
                    } else {
                        scores = bigArrays.grow(scores, parentIdx + 1);
                        scores.set(parentIdx, scorer.score());
                    }
                    parentIdsIndex.set(ord, parentIdx);
                } else {
                    doScore(parentIdx);
                }
            }
        }

        protected void doScore(long index) throws IOException {
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(parentType);
            if (values != null) {
                ordinals = values.ordinals();
                final long maxOrd = ordinals.getMaxOrd();
                if (parentIdsIndex == null) {
                    parentIdsIndex = bigArrays.newLongArray(BigArrays.overSize(maxOrd), false);
                } else if (parentIdsIndex.size() < maxOrd) {
                    parentIdsIndex = bigArrays.grow(parentIdsIndex, maxOrd);
                }
                parentIdsIndex.fill(0, maxOrd, -1L);
            }

        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

    }

    private final static class SumCollector extends ParentIdAndScoreCollector {

        private SumCollector(ParentChildIndexFieldData indexFieldData, String parentType, SearchContext searchContext) {
            super(indexFieldData, parentType, searchContext);
        }

        @Override
        protected void doScore(long index) throws IOException {
            scores.increment(index, scorer.score());
        }
    }

    private final static class MaxCollector extends ParentIdAndScoreCollector {

        private MaxCollector(ParentChildIndexFieldData indexFieldData, String childType, SearchContext searchContext) {
            super(indexFieldData, childType, searchContext);
        }

        @Override
        protected void doScore(long index) throws IOException {
            float currentScore = scorer.score();
            if (currentScore > scores.get(index)) {
                scores.set(index, currentScore);
            }
        }
    }

    private final static class AvgCollector extends ParentIdAndScoreCollector {

        private IntArray occurrences;

        AvgCollector(ParentChildIndexFieldData indexFieldData, String childType, SearchContext searchContext) {
            super(indexFieldData, childType, searchContext);
            this.occurrences = bigArrays.newIntArray(512, false);
        }

        @Override
        public void collect(int doc) throws IOException {
            if (values != null) {
                int ord = (int) ordinals.getOrd(doc);
                long parentIdx = parentIdsIndex.get(ord);
                if (parentIdx < 0) {
                    final BytesRef bytes = values.getValueByOrd(ord);
                    final int hash = values.currentValueHash();
                    parentIdx = parentIds.add(bytes, hash);
                    if (parentIdx < 0) {
                        parentIdx = -parentIdx - 1;
                        scores.increment(parentIdx, scorer.score());
                        occurrences.increment(parentIdx, 1);
                    } else {
                        scores = bigArrays.grow(scores, parentIdx + 1);
                        scores.set(parentIdx, scorer.score());
                        occurrences = bigArrays.grow(occurrences, parentIdx + 1);
                        occurrences.set(parentIdx, 1);
                    }
                    parentIdsIndex.set(ord, parentIdx);
                } else {
                    scores.increment(parentIdx, scorer.score());
                    occurrences.increment(parentIdx, 1);
                }
            }
        }

    }

}

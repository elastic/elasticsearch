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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.XFilteredDocIdSetIterator;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

import java.io.IOException;
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

    protected final ParentChildIndexFieldData ifd;
    protected final String parentType;
    protected final String childType;
    protected final Filter parentFilter;
    protected final ScoreType scoreType;
    protected Query originalChildQuery;
    protected final int minChildren;
    protected final int maxChildren;
    protected final int shortCircuitParentDocSet;
    protected final BitDocIdSetFilter nonNestedDocsFilter;

    protected Query rewrittenChildQuery;
    protected IndexReader rewriteIndexReader;

    public ChildrenQuery(ParentChildIndexFieldData ifd, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType, int minChildren, int maxChildren, int shortCircuitParentDocSet, BitDocIdSetFilter nonNestedDocsFilter) {
        this.ifd = ifd;
        this.parentType = parentType;
        this.childType = childType;
        this.parentFilter = parentFilter;
        this.originalChildQuery = childQuery;
        this.scoreType = scoreType;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
        assert maxChildren == 0 || minChildren <= maxChildren;
        this.minChildren = minChildren > 1 ? minChildren : 0;
        this.maxChildren = maxChildren;
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
        if (minChildren != that.minChildren) {
            return false;
        }
        if (maxChildren != that.maxChildren) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        result = 31 * result + minChildren;
        result = 31 * result + maxChildren;
        return result;
    }

    @Override
    public String toString(String field) {
        int max = maxChildren == 0 ? Integer.MAX_VALUE : maxChildren;
        return "ChildrenQuery[min(" + Integer.toString(minChildren) + ") max(" + Integer.toString(max) + ")of " + childType + "/"
                + parentType + "](" + originalChildQuery.toString(field) + ')' + ToStringUtils.boost(getBoost());
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
    public Query clone() {
        ChildrenQuery q = (ChildrenQuery) super.clone();
        q.originalChildQuery = originalChildQuery.clone();
        if (q.rewrittenChildQuery != null) {
            q.rewrittenChildQuery = rewrittenChildQuery.clone();
        }
        return q;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        SearchContext sc = SearchContext.current();
        assert rewrittenChildQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader
                + " searcher.getIndexReader()=" + searcher.getIndexReader();
        final Query childQuery = rewrittenChildQuery;

        IndexParentChildFieldData globalIfd = ifd.loadGlobal(searcher.getIndexReader());
        if (globalIfd == null) {
            // No docs of the specified type exist on this shard
            return new BooleanQuery().createWeight(searcher, needsScores);
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());

        boolean abort = true;
        long numFoundParents;
        ParentCollector collector = null;
        try {
            if (minChildren == 0 && maxChildren == 0 && scoreType != ScoreType.NONE) {
                switch (scoreType) {
                case MIN:
                    collector = new MinCollector(globalIfd, sc, parentType);
                    break;
                case MAX:
                    collector = new MaxCollector(globalIfd, sc, parentType);
                    break;
                case SUM:
                    collector = new SumCollector(globalIfd, sc, parentType);
                    break;
                }
            }
            if (collector == null) {
                switch (scoreType) {
                case MIN:
                    collector = new MinCountCollector(globalIfd, sc, parentType);
                    break;
                case MAX:
                    collector = new MaxCountCollector(globalIfd, sc, parentType);
                    break;
                case SUM:
                case AVG:
                    collector = new SumCountAndAvgCollector(globalIfd, sc, parentType);
                    break;
                case NONE:
                    collector = new CountCollector(globalIfd, sc, parentType);
                    break;
                default:
                    throw new RuntimeException("Are we missing a score type here? -- " + scoreType);
                }
            }

            indexSearcher.search(childQuery, collector);
            numFoundParents = collector.foundParents();
            if (numFoundParents == 0) {
                return new BooleanQuery().createWeight(searcher, needsScores);
            }
            abort = false;
        } finally {
            if (abort) {
                Releasables.close(collector);
            }
        }
        sc.addReleasable(collector, Lifetime.COLLECTION);
        final Filter parentFilter;
        if (numFoundParents <= shortCircuitParentDocSet) {
            parentFilter = ParentIdsFilter.createShortCircuitFilter(nonNestedDocsFilter, sc, parentType, collector.values,
                    collector.parentIdxs, numFoundParents);
        } else {
            parentFilter = this.parentFilter;
        }
        return new ParentWeight(this, rewrittenChildQuery.createWeight(searcher, needsScores), parentFilter, numFoundParents, collector, minChildren,
                maxChildren);
    }

    protected class ParentWeight extends Weight {

        protected final Weight childWeight;
        protected final Filter parentFilter;
        protected final ParentCollector collector;
        protected final int minChildren;
        protected final int maxChildren;

        protected long remaining;
        protected float queryNorm;
        protected float queryWeight;

        protected ParentWeight(Query query, Weight childWeight, Filter parentFilter, long remaining, ParentCollector collector, int minChildren, int maxChildren) {
            super(query);
            this.childWeight = childWeight;
            this.parentFilter = parentFilter;
            this.remaining = remaining;
            this.collector = collector;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return Explanation.match(getBoost(), "not implemented yet...");
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            queryWeight = getBoost();
            if (scoreType == ScoreType.NONE) {
                return queryWeight * queryWeight;
            }
            float sum = childWeight.getValueForNormalization();
            sum *= queryWeight * queryWeight;
            return sum;
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            DocIdSet parentsSet = parentFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(parentsSet) || remaining == 0) {
                return null;
            }

            // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
            // count down (short circuit) logic will then work as expected.
            DocIdSetIterator parents = BitsFilteredDocIdSet.wrap(parentsSet, context.reader().getLiveDocs()).iterator();

            if (parents != null) {
                SortedDocValues bytesValues = collector.globalIfd.load(context).getOrdinalsValues(parentType);
                if (bytesValues == null) {
                    return null;
                }

                if (minChildren > 0 || maxChildren != 0 || scoreType == ScoreType.NONE) {
                    switch (scoreType) {
                    case NONE:
                        DocIdSetIterator parentIdIterator = new CountParentOrdIterator(this, parents, collector, bytesValues,
                                minChildren, maxChildren);
                        return ConstantScorer.create(parentIdIterator, this, queryWeight);
                    case AVG:
                        return new AvgParentCountScorer(this, parents, collector, bytesValues, minChildren, maxChildren);
                    default:
                        return new ParentCountScorer(this, parents, collector, bytesValues, minChildren, maxChildren);
                    }
                }
                switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, parents, collector, bytesValues);
                default:
                    return new ParentScorer(this, parents, collector, bytesValues);
                }
            }
            return null;
        }
    }

    protected abstract static class ParentCollector extends NoopCollector implements Releasable {

        protected final IndexParentChildFieldData globalIfd;
        protected final LongHash parentIdxs;
        protected final BigArrays bigArrays;
        protected final SearchContext searchContext;
        protected final String parentType;

        protected SortedDocValues values;
        protected Scorer scorer;

        protected ParentCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            this.globalIfd = globalIfd;
            this.searchContext = searchContext;
            this.bigArrays = searchContext.bigArrays();
            this.parentIdxs = new LongHash(512, bigArrays);
            this.parentType = parentType;
        }

        @Override
        public final void collect(int doc) throws IOException {
            if (values != null) {
                final long globalOrdinal = values.getOrd(doc);
                if (globalOrdinal >= 0) {
                    long parentIdx = parentIdxs.add(globalOrdinal);
                    if (parentIdx >= 0) {
                        newParent(parentIdx);
                    } else {
                        parentIdx = -1 - parentIdx;
                        existingParent(parentIdx);
                    }
                }
            }
        }

        protected void newParent(long parentIdx) throws IOException {
        }

        protected void existingParent(long parentIdx) throws IOException {
        }

        public long foundParents() {
            return parentIdxs.size();
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) throws IOException {
            values = globalIfd.load(context).getOrdinalsValues(parentType);
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIdxs);
        }
    }

    protected abstract static class ParentScoreCollector extends ParentCollector implements Releasable {

        protected FloatArray scores;

        protected ParentScoreCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
            this.scores = this.bigArrays.newFloatArray(512, false);
        }

        @Override
        public boolean needsScores() {
            return true;
        }

        @Override
        protected void newParent(long parentIdx) throws IOException {
            scores = bigArrays.grow(scores, parentIdx + 1);
            scores.set(parentIdx, scorer.score());
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIdxs, scores);
        }
    }

    protected abstract static class ParentScoreCountCollector extends ParentScoreCollector implements Releasable {

        protected IntArray occurrences;

        protected ParentScoreCountCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
            this.occurrences = bigArrays.newIntArray(512, false);
        }

        @Override
        protected void newParent(long parentIdx) throws IOException {
            scores = bigArrays.grow(scores, parentIdx + 1);
            scores.set(parentIdx, scorer.score());
            occurrences = bigArrays.grow(occurrences, parentIdx + 1);
            occurrences.set(parentIdx, 1);
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIdxs, scores, occurrences);
        }
    }

    private final static class CountCollector extends ParentCollector implements Releasable {

        protected IntArray occurrences;

        protected CountCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
            this.occurrences = bigArrays.newIntArray(512, false);
        }

        @Override
        protected void newParent(long parentIdx) throws IOException {
            occurrences = bigArrays.grow(occurrences, parentIdx + 1);
            occurrences.set(parentIdx, 1);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            occurrences.increment(parentIdx, 1);
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(parentIdxs, occurrences);
        }
    }

    private final static class SumCollector extends ParentScoreCollector {

        private SumCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            scores.increment(parentIdx, scorer.score());
        }
    }

    private final static class MaxCollector extends ParentScoreCollector {

        private MaxCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            float currentScore = scorer.score();
            if (currentScore > scores.get(parentIdx)) {
                scores.set(parentIdx, currentScore);
            }
        }
    }

    private final static class MinCollector extends ParentScoreCollector {

        private MinCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            float currentScore = scorer.score();
            if (currentScore < scores.get(parentIdx)) {
                scores.set(parentIdx, currentScore);
            }
        }
    }

    private final static class MaxCountCollector extends ParentScoreCountCollector {

        private MaxCountCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            float currentScore = scorer.score();
            if (currentScore > scores.get(parentIdx)) {
                scores.set(parentIdx, currentScore);
            }
            occurrences.increment(parentIdx, 1);
        }
    }

    private final static class MinCountCollector extends ParentScoreCountCollector {

        private MinCountCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            float currentScore = scorer.score();
            if (currentScore < scores.get(parentIdx)) {
                scores.set(parentIdx, currentScore);
            }
            occurrences.increment(parentIdx, 1);
        }
    }

    private final static class SumCountAndAvgCollector extends ParentScoreCountCollector {

        SumCountAndAvgCollector(IndexParentChildFieldData globalIfd, SearchContext searchContext, String parentType) {
            super(globalIfd, searchContext, parentType);
        }

        @Override
        protected void existingParent(long parentIdx) throws IOException {
            scores.increment(parentIdx, scorer.score());
            occurrences.increment(parentIdx, 1);
        }
    }

    private static class ParentScorer extends Scorer {

        final ParentWeight parentWeight;
        final LongHash parentIds;
        final FloatArray scores;

        final SortedDocValues globalOrdinals;
        final DocIdSetIterator parentsIterator;

        int currentDocId = -1;
        float currentScore;

        ParentScorer(ParentWeight parentWeight, DocIdSetIterator parentsIterator, ParentCollector collector, SortedDocValues globalOrdinals) {
            super(parentWeight);
            this.parentWeight = parentWeight;
            this.globalOrdinals = globalOrdinals;
            this.parentsIterator = parentsIterator;
            this.parentIds = collector.parentIdxs;
            this.scores = ((ParentScoreCollector) collector).scores;
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        protected boolean acceptAndScore(long parentIdx) {
            currentScore = scores.get(parentIdx);
            return true;
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
            if (parentWeight.remaining == 0) {
                return currentDocId = NO_MORE_DOCS;
            }

            while (true) {
                currentDocId = parentsIterator.nextDoc();
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                final int globalOrdinal = globalOrdinals.getOrd(currentDocId);
                if (globalOrdinal < 0) {
                    continue;
                }

                final long parentIdx = parentIds.find(globalOrdinal);
                if (parentIdx != -1) {
                    parentWeight.remaining--;
                    if (acceptAndScore(parentIdx)) {
                        return currentDocId;
                    }
                }
            }
        }

        @Override
        public int advance(int target) throws IOException {
            if (parentWeight.remaining == 0) {
                return currentDocId = NO_MORE_DOCS;
            }

            currentDocId = parentsIterator.advance(target);
            if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                return currentDocId;
            }

            final long globalOrdinal = globalOrdinals.getOrd(currentDocId);
            if (globalOrdinal < 0) {
                return nextDoc();
            }

            final long parentIdx = parentIds.find(globalOrdinal);
            if (parentIdx != -1) {
                parentWeight.remaining--;
                if (acceptAndScore(parentIdx)) {
                    return currentDocId;
                }
            }
            return nextDoc();
        }

        @Override
        public long cost() {
            return parentsIterator.cost();
        }
    }

    private static class ParentCountScorer extends ParentScorer {

        protected final IntArray occurrences;
        protected final int minChildren;
        protected final int maxChildren;

        ParentCountScorer(ParentWeight parentWeight, DocIdSetIterator parentsIterator, ParentCollector collector, SortedDocValues globalOrdinals, int minChildren, int maxChildren) {
            super(parentWeight, parentsIterator, (ParentScoreCollector) collector, globalOrdinals);
            this.minChildren = minChildren;
            this.maxChildren = maxChildren == 0 ? Integer.MAX_VALUE : maxChildren;
            this.occurrences = ((ParentScoreCountCollector) collector).occurrences;
        }

        @Override
        protected boolean acceptAndScore(long parentIdx) {
            int count = occurrences.get(parentIdx);
            if (count < minChildren || count > maxChildren) {
                return false;
            }
            return super.acceptAndScore(parentIdx);
        }
    }

    private static final class AvgParentScorer extends ParentCountScorer {

        AvgParentScorer(ParentWeight weight, DocIdSetIterator parentsIterator, ParentCollector collector, SortedDocValues globalOrdinals) {
            super(weight, parentsIterator, collector, globalOrdinals, 0, 0);
        }

        @Override
        protected boolean acceptAndScore(long parentIdx) {
            currentScore = scores.get(parentIdx);
            currentScore /= occurrences.get(parentIdx);
            return true;
        }

    }

    private static final class AvgParentCountScorer extends ParentCountScorer {

        AvgParentCountScorer(ParentWeight weight, DocIdSetIterator parentsIterator, ParentCollector collector, SortedDocValues globalOrdinals, int minChildren, int maxChildren) {
            super(weight, parentsIterator, collector, globalOrdinals, minChildren, maxChildren);
        }

        @Override
        protected boolean acceptAndScore(long parentIdx) {
            int count = occurrences.get(parentIdx);
            if (count < minChildren || count > maxChildren) {
                return false;
            }
            currentScore = scores.get(parentIdx);
            currentScore /= occurrences.get(parentIdx);
            return true;
        }
    }

    private final static class CountParentOrdIterator extends XFilteredDocIdSetIterator {

        private final LongHash parentIds;
        protected final IntArray occurrences;
        private final int minChildren;
        private final int maxChildren;
        private final SortedDocValues ordinals;
        private final ParentWeight parentWeight;

        private CountParentOrdIterator(ParentWeight parentWeight, DocIdSetIterator innerIterator, ParentCollector collector, SortedDocValues ordinals, int minChildren, int maxChildren) {
            super(innerIterator);
            this.parentIds = ((CountCollector) collector).parentIdxs;
            this.occurrences = ((CountCollector) collector).occurrences;
            this.ordinals = ordinals;
            this.parentWeight = parentWeight;
            this.minChildren = minChildren;
            this.maxChildren = maxChildren == 0 ? Integer.MAX_VALUE : maxChildren;
        }

        @Override
        protected boolean match(int doc) {
            if (parentWeight.remaining == 0) {
                throw new CollectionTerminatedException();
            }

            final long parentOrd = ordinals.getOrd(doc);
            if (parentOrd >= 0) {
                final long parentIdx = parentIds.find(parentOrd);
                if (parentIdx != -1) {
                    parentWeight.remaining--;
                    int count = occurrences.get(parentIdx);
                    if (count >= minChildren && count <= maxChildren) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}

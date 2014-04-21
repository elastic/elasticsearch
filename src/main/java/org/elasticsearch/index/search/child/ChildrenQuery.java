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
import org.apache.lucene.util.OpenBitSet;
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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;

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

    private final ParentChildIndexFieldData ifd;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final ScoreType scoreType;
    private Query originalChildQuery;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenQuery(ParentChildIndexFieldData ifd, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.ifd = ifd;
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
        return "ChildrenQuery[" + childType + "/" + parentType + "](" + originalChildQuery
                .toString(field) + ')' + ToStringUtils.boost(getBoost());
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
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        assert rewrittenChildQuery != null;
        assert rewriteIndexReader == searcher.getIndexReader() : "not equal, rewriteIndexReader=" + rewriteIndexReader + " searcher.getIndexReader()=" + searcher.getIndexReader();
        final Query childQuery = rewrittenChildQuery;

        IndexFieldData.WithOrdinals globalIfd = ifd.getGlobalParentChild(parentType, searchContext.searcher().getIndexReader());
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.setSimilarity(searcher.getSimilarity());
        final ParentIdAndScoreCollector collector;
        switch (scoreType) {
            case MAX:
                collector = new MaxCollector(globalIfd, parentType, searchContext);
                break;
            case SUM:
                collector = new SumCollector(globalIfd, parentType, searchContext);
                break;
            case AVG:
                collector = new AvgCollector(globalIfd, parentType, searchContext);
                break;
            default:
                throw new RuntimeException("Are we missing a score type here? -- " + scoreType);
        }

        boolean abort = true;
        long numFoundParents;
        try {
            searcher.search(childQuery, collector);
            numFoundParents = collector.foundParents();
            if (numFoundParents == 0) {
                Releasables.close(collector);
                return Queries.newMatchNoDocsQuery().createWeight(searcher);
            }
            abort = false;
        } finally {
            if (abort) {
                Releasables.close(collector);
            }
        }
        searchContext.addReleasable(collector, Lifetime.COLLECTION);
        final Filter parentFilter;
        if (numFoundParents <= shortCircuitParentDocSet) {
            if (numFoundParents == 1) {
                collector.values.getValueByOrd(collector.parentOrds.nextSetBit(0));
                BytesRef id = collector.values.copyShared();
                if (nonNestedDocsFilter != null) {
                    List<Filter> filters = Arrays.asList(
                            new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))),
                            nonNestedDocsFilter
                    );
                    parentFilter = new AndFilter(filters);
                } else {
                    parentFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
                }
            } else {
                BytesRefHash parentIds= null;
                boolean constructed = false;
                try {
                    parentIds = new BytesRefHash(numFoundParents, searchContext.bigArrays());
                    for (long parentOrd = collector.parentOrds.nextSetBit(0l); parentOrd != -1; parentOrd = collector.parentOrds.nextSetBit(parentOrd + 1)) {
                        parentIds.add(collector.values.getValueByOrd(parentOrd));
                    }
                    constructed = true;
                } finally {
                    if (!constructed) {
                        Releasables.close(parentIds);
                    }
                }
                searchContext.addReleasable(parentIds, SearchContext.Lifetime.COLLECTION);
                parentFilter = new ParentIdsFilter(parentType, nonNestedDocsFilter, parentIds);
            }
        } else {
            parentFilter = new ApplyAcceptedDocsFilter(this.parentFilter);
        }
        return new ParentWeight(rewrittenChildQuery.createWeight(searcher), parentFilter, numFoundParents, collector);
    }

    private final class ParentWeight extends Weight {

        private final Weight childWeight;
        private final Filter parentFilter;
        private final ParentIdAndScoreCollector collector;

        private long remaining;

        private ParentWeight(Weight childWeight, Filter parentFilter, long remaining, ParentIdAndScoreCollector collector) {
            this.childWeight = childWeight;
            this.parentFilter = parentFilter;
            this.remaining = remaining;
            this.collector = collector;
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

            // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
            // count down (short circuit) logic will then work as expected.
            DocIdSetIterator parents = BitsFilteredDocIdSet.wrap(parentsSet, context.reader().getLiveDocs()).iterator();
            BytesValues.WithOrdinals bytesValues = collector.globalIfd.load(context).getBytesValues(false);
            if (bytesValues == null) {
                return null;
            }
            switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, parents, collector, bytesValues.ordinals());
                default:
                    return new ParentScorer(this, parents, collector, bytesValues.ordinals());
            }
        }

    }

    private abstract static class ParentIdAndScoreCollector extends NoopCollector implements Releasable {

        // Maybe use a LongHash instead, then the scores array will be smaller if not too many docs match
        protected final OpenBitSet parentOrds;
        protected final String parentType;
        private final IndexFieldData.WithOrdinals globalIfd;
        protected final BigArrays bigArrays;
        protected FloatArray scores;
        protected final SearchContext searchContext;

        protected Ordinals.Docs ordinals;
        protected BytesValues.WithOrdinals values;
        protected Scorer scorer;

        private ParentIdAndScoreCollector(IndexFieldData.WithOrdinals globalIfd, String parentType, SearchContext searchContext) {
            this.parentType = parentType;
            this.globalIfd = globalIfd;
            this.bigArrays = searchContext.bigArrays();
            // TODO: look into setting it to maxOrd
            this.parentOrds = new OpenBitSet(512);
            this.scores = bigArrays.newFloatArray(512, false);
            this.searchContext = searchContext;
        }


        @Override
        public void collect(int doc) throws IOException {
            if (ordinals != null) {
                long ord = ordinals.getOrd(doc);
                if (!parentOrds.get(ord)) {
                    parentOrds.set(ord);
                    scores = bigArrays.grow(scores, ord + 1);
                    scores.set(ord, scorer.score());
                } else {
                    doScore(ord);
                }
            }
        }

        protected void doScore(long index) throws IOException {
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = globalIfd.load(context).getBytesValues(false);
            if (values != null) {
                ordinals = values.ordinals();
            }

        }

        public long foundParents() {
            return parentOrds.cardinality();
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void close() throws ElasticsearchException {
            Releasables.close(scores);
        }
    }

    private final static class SumCollector extends ParentIdAndScoreCollector {

        private SumCollector(IndexFieldData.WithOrdinals globalIfd, String parentType, SearchContext searchContext) {
            super(globalIfd, parentType, searchContext);
        }

        @Override
        protected void doScore(long index) throws IOException {
            scores.increment(index, scorer.score());
        }
    }

    private final static class MaxCollector extends ParentIdAndScoreCollector {

        private MaxCollector(IndexFieldData.WithOrdinals globalIfd, String childType, SearchContext searchContext) {
            super(globalIfd, childType, searchContext);
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

        AvgCollector(IndexFieldData.WithOrdinals globalIfd, String childType, SearchContext searchContext) {
            super(globalIfd, childType, searchContext);
            // TODO: look into setting it to maxOrd
            this.occurrences = bigArrays.newIntArray(512, false);
        }

        @Override
        public void collect(int doc) throws IOException {
            if (ordinals != null) {
                long ord = ordinals.getOrd(doc);
                if (!parentOrds.get(ord)) {
                    parentOrds.set(ord);
                    scores = bigArrays.grow(scores, ord + 1);
                    occurrences = bigArrays.grow(occurrences, ord + 1);
                    scores.set(ord, scorer.score());
                    occurrences.set(ord, 1);
                } else {
                    scores.increment(ord, scorer.score());
                    occurrences.increment(ord, 1);
                }
            }
        }

    }

    private static class ParentScorer extends Scorer {

        final ParentWeight parentWeight;
        final OpenBitSet parentIds;
        final FloatArray scores;

        final Ordinals.Docs globalOrdinals;
        final DocIdSetIterator parentsIterator;

        int currentDocId = -1;
        float currentScore;

        ParentScorer(ParentWeight parentWeight, DocIdSetIterator parentsIterator, ParentIdAndScoreCollector collector, Ordinals.Docs globalOrdinals) {
            super(parentWeight);
            this.parentWeight = parentWeight;
            this.globalOrdinals = globalOrdinals;
            this.parentsIterator = parentsIterator;
            this.parentIds = collector.parentOrds;
            this.scores = collector.scores;
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
            if (parentWeight.remaining == 0) {
                return currentDocId = NO_MORE_DOCS;
            }

            while (true) {
                currentDocId = parentsIterator.nextDoc();
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                long ord = globalOrdinals.getOrd(currentDocId);
                if (parentIds.get(ord)) {
                    currentScore = scores.get(ord);
                    parentWeight.remaining--;
                    return currentDocId;
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

            long ord  = globalOrdinals.getOrd(currentDocId);
            if (parentIds.get(ord)) {
                currentScore = scores.get(ord);
                parentWeight.remaining--;
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

    private static final class AvgParentScorer extends ParentScorer {

        final IntArray occurrences;

        AvgParentScorer(ParentWeight weight, DocIdSetIterator parentsIterator, ParentIdAndScoreCollector collector, Ordinals.Docs globalOrdinals) {
            super(weight, parentsIterator, collector, globalOrdinals);
            this.occurrences = ((AvgCollector) collector).occurrences;
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

                long ord = globalOrdinals.getOrd(currentDocId);
                if (parentIds.get(ord)) {
                    currentScore = scores.get(ord);
                    currentScore /= occurrences.get(ord);
                    parentWeight.remaining--;
                    return currentDocId;
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

            long ord  = globalOrdinals.getOrd(currentDocId);
            if (parentIds.get(ord)) {
                currentScore = scores.get(ord);
                currentScore /= occurrences.get(ord);
                parentWeight.remaining--;
                return currentDocId;
            } else {
                return nextDoc();
            }
        }
    }

}

/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.search.child;

import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped child query and connects all the matching child docs to the related
 * parent documents using the {@link IdReaderTypeCache}.
 * <p/>
 * This query is executed in two rounds. The first round resolves all the matching child documents and groups these
 * documents by parent uid value. Also the child scores are aggregated per parent uid value. During the second round
 * all parent documents having the same uid value that is collected in the first phase are emitted as hit including
 * a score based on the aggregated child scores and score type.
 */
// TODO We use a score of 0 to indicate a doc was not scored in uidToScore, this means score of 0 can be problematic, if we move to HPCC, we can use lset/...
public class ChildrenQuery extends Query implements SearchContext.Rewrite {

    private final SearchContext searchContext;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final ScoreType scoreType;
    private final Query originalChildQuery;
    private final int shortCircuitParentDocSet;

    private Query rewrittenChildQuery;
    private TObjectFloatHashMap<HashedBytesArray> uidToScore;
    private TObjectIntHashMap<HashedBytesArray> uidToCount;

    public ChildrenQuery(SearchContext searchContext, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType, int shortCircuitParentDocSet) {
        this.searchContext = searchContext;
        this.parentType = parentType;
        this.childType = childType;
        this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
        this.originalChildQuery = childQuery;
        this.scoreType = scoreType;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
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
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
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
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());

        uidToScore = searchContext.cacheRecycler().popObjectFloatMap();
        Collector collector;
        switch (scoreType) {
            case AVG:
                uidToCount = searchContext.cacheRecycler().popObjectIntMap();
                collector = new AvgChildUidCollector(scoreType, searchContext, parentType, uidToScore, uidToCount);
                break;
            default:
                collector = new ChildUidCollector(scoreType, searchContext, parentType, uidToScore);
        }
        Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searchContext.searcher().rewrite(originalChildQuery);
        } else {
            childQuery = rewrittenChildQuery;
        }
        searchContext.searcher().search(childQuery, collector);
    }

    @Override
    public void contextClear() {
        if (uidToScore != null) {
            searchContext.cacheRecycler().pushObjectFloatMap(uidToScore);
        }
        uidToScore = null;
        if (uidToCount != null) {
            searchContext.cacheRecycler().pushObjectIntMap(uidToCount);
        }
        uidToCount = null;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        if (uidToScore == null) {
            throw new ElasticSearchIllegalStateException("has_child query hasn't executed properly");
        }

        int size = uidToScore.size();
        if (size == 0) {
            return Queries.NO_MATCH_QUERY.createWeight(searcher);
        }

        Filter parentFilter;
        if (size == 1) {
            BytesRef id = uidToScore.keySet().iterator().next().toBytesRef();
            parentFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
        } else if (size <= shortCircuitParentDocSet) {
            parentFilter = new ParentIdsFilter(parentType, uidToScore.keySet());
        } else {
            parentFilter = this.parentFilter;
        }
        return new ParentWeight(rewrittenChildQuery.createWeight(searcher), parentFilter, size);
    }

    final class ParentWeight extends Weight {

        final Weight childWeight;
        final Filter parentFilter;
        int remaining;

        public ParentWeight(Weight childWeight, Filter parentFilter, int remaining) {
            this.childWeight = childWeight;
            this.parentFilter = parentFilter;
            this.remaining = remaining;
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

            IdReaderTypeCache idTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
            DocIdSetIterator parentsIterator = parentsSet.iterator();
            switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, idTypeCache, parentsIterator);
                default:
                    return new ParentScorer(this, idTypeCache, parentsIterator);
            }
        }

        class ParentScorer extends Scorer {

            final IdReaderTypeCache idTypeCache;
            final DocIdSetIterator parentsIterator;

            int remaining;
            int currentDocId = -1;
            float currentScore;

            ParentScorer(Weight weight, IdReaderTypeCache idTypeCache, DocIdSetIterator parentsIterator) {
                super(weight);
                this.idTypeCache = idTypeCache;
                this.parentsIterator = parentsIterator;
                this.remaining = uidToScore.size();
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
                    return NO_MORE_DOCS;
                }

                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    HashedBytesArray uid = idTypeCache.idByDoc(currentDocId);
                    currentScore = uidToScore.get(uid);
                    if (currentScore != 0) {
                        remaining--;
                        return currentDocId;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                if (remaining == 0) {
                    return NO_MORE_DOCS;
                }

                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                HashedBytesArray uid = idTypeCache.idByDoc(currentDocId);
                currentScore = uidToScore.get(uid);
                if (currentScore != 0) {
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

        final class AvgParentScorer extends ParentScorer {

            HashedBytesArray currentUid;

            AvgParentScorer(Weight weight, IdReaderTypeCache idTypeCache, DocIdSetIterator parentsIterator) {
                super(weight, idTypeCache, parentsIterator);
            }

            @Override
            public int nextDoc() throws IOException {
                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    currentUid = idTypeCache.idByDoc(currentDocId);
                    currentScore = uidToScore.get(currentUid);
                    if (currentScore != 0) {
                        remaining--;
                        currentScore /= uidToCount.get(currentUid);
                        return currentDocId;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                HashedBytesArray uid = idTypeCache.idByDoc(currentDocId);
                currentScore = uidToScore.get(uid);
                if (currentScore != 0) {
                    remaining--;
                    currentScore /= uidToCount.get(currentUid);
                    return currentDocId;
                } else {
                    return nextDoc();
                }
            }
        }

    }

    static class ChildUidCollector extends ParentIdCollector {

        final TObjectFloatHashMap<HashedBytesArray> uidToScore;
        final ScoreType scoreType;
        Scorer scorer;

        ChildUidCollector(ScoreType scoreType, SearchContext searchContext, String childType, TObjectFloatHashMap<HashedBytesArray> uidToScore) {
            super(childType, searchContext);
            this.uidToScore = uidToScore;
            this.scoreType = scoreType;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        protected void collect(int doc, HashedBytesArray parentUid) throws IOException {
            float previousScore = uidToScore.get(parentUid);
            float currentScore = scorer.score();
            if (previousScore == 0) {
                uidToScore.put(parentUid, currentScore);
            } else {
                switch (scoreType) {
                    case SUM:
                        uidToScore.adjustValue(parentUid, currentScore);
                        break;
                    case MAX:
                        if (currentScore > previousScore) {
                            uidToScore.put(parentUid, currentScore);
                        }
                        break;
                    case AVG:
                        assert false : "AVG has it's own collector";
                        
                    default:
                        assert false : "Are we missing a score type here? -- " + scoreType;
                        break;
                }
            }
        }

    }

    final static class AvgChildUidCollector extends ChildUidCollector {

        final TObjectIntHashMap<HashedBytesArray> uidToCount;

        AvgChildUidCollector(ScoreType scoreType, SearchContext searchContext, String childType, TObjectFloatHashMap<HashedBytesArray> uidToScore, TObjectIntHashMap<HashedBytesArray> uidToCount) {
            super(scoreType, searchContext, childType, uidToScore);
            this.uidToCount = uidToCount;
            assert scoreType == ScoreType.AVG;
        }

        @Override
        protected void collect(int doc, HashedBytesArray parentUid) throws IOException {
            float previousScore = uidToScore.get(parentUid);
            float currentScore = scorer.score();
            if (previousScore == 0) {
                uidToScore.put(parentUid, currentScore);
                uidToCount.put(parentUid, 1);
            } else {
                uidToScore.adjustValue(parentUid, currentScore);
                uidToCount.increment(parentUid);
            }
        }

    }

}

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

import gnu.trove.map.TObjectFloatMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
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
public class ChildrenQuery extends Query implements SearchContext.Rewrite {

    private final SearchContext searchContext;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final ScoreType scoreType;
    private final Query originalChildQuery;

    private Query rewrittenChildQuery;
    private TObjectFloatHashMap<HashedBytesArray> uidToScore;
    private TObjectIntHashMap<HashedBytesArray> uidToCount;

    public ChildrenQuery(SearchContext searchContext, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType) {
        this.searchContext = searchContext;
        this.parentType = parentType;
        this.childType = childType;
        this.parentFilter = parentFilter;
        this.originalChildQuery = childQuery;
        this.scoreType = scoreType;
    }

    private ChildrenQuery(ChildrenQuery unProcessedQuery, Query rewrittenChildQuery) {
        this.searchContext = unProcessedQuery.searchContext;
        this.parentType = unProcessedQuery.parentType;
        this.childType = unProcessedQuery.childType;
        this.parentFilter = unProcessedQuery.parentFilter;
        this.scoreType = unProcessedQuery.scoreType;
        this.originalChildQuery = unProcessedQuery.originalChildQuery;
        this.rewrittenChildQuery = rewrittenChildQuery;

        this.uidToScore = unProcessedQuery.uidToScore;
        this.uidToCount = unProcessedQuery.uidToCount;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ChildrenQuery[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery
                .toString(field)).append(')').append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten;
        if (rewrittenChildQuery == null) {
            rewritten = originalChildQuery.rewrite(reader);
        } else {
            rewritten = rewrittenChildQuery;
        }
        if (rewritten == rewrittenChildQuery) {
            return this;
        }

        // See TopChildrenQuery#rewrite
        int index = searchContext.rewrites().indexOf(this);
        ChildrenQuery rewrite = new ChildrenQuery(this, rewritten);
        searchContext.rewrites().set(index, rewrite);
        return rewrite;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public void contextRewrite(SearchContext searchContext) throws Exception {
        searchContext.idCache().refresh(searchContext.searcher().getTopReaderContext().leaves());

        uidToScore = CacheRecycler.popObjectFloatMap();
        Collector collector;
        switch (scoreType) {
            case AVG:
                uidToCount = CacheRecycler.popObjectIntMap();
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
            CacheRecycler.pushObjectFloatMap(uidToScore);
        }
        uidToScore = null;
        if (uidToCount != null) {
            CacheRecycler.pushObjectIntMap(uidToCount);
        }
        uidToCount = null;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        if (uidToScore == null) {
            throw new ElasticSearchIllegalStateException("has_child query hasn't executed properly");
        }

        return new ParentWeight(rewrittenChildQuery.createWeight(searcher));
    }

    class ParentWeight extends Weight {

        final Weight childWeight;

        public ParentWeight(Weight childWeight) {
            this.childWeight = childWeight;
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
            if (parentsSet == null || parentsSet == DocIdSet.EMPTY_DOCIDSET) {
                return null;
            }

            IdReaderTypeCache idTypeCache = searchContext.idCache().reader(context.reader()).type(parentType);
            DocIdSetIterator parentsIterator = parentsSet.iterator();
            switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, idTypeCache, uidToScore, uidToCount, parentsIterator);
                default:
                    return new ParentScorer(this, idTypeCache, uidToScore, parentsIterator);
            }
        }

    }

    static class ParentScorer extends Scorer {

        final IdReaderTypeCache idTypeCache;
        final TObjectFloatMap<HashedBytesArray> uidToScore;
        final DocIdSetIterator parentsIterator;

        int currentDocId = -1;
        float currentScore;

        ParentScorer(Weight weight, IdReaderTypeCache idTypeCache, TObjectFloatMap<HashedBytesArray> uidToScore, DocIdSetIterator parentsIterator) {
            super(weight);
            this.idTypeCache = idTypeCache;
            this.uidToScore = uidToScore;
            this.parentsIterator = parentsIterator;
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
            while (true) {
                currentDocId = parentsIterator.nextDoc();
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                HashedBytesArray uid = idTypeCache.idByDoc(currentDocId);
                currentScore = uidToScore.get(uid);
                if (Float.compare(currentScore, 0) > 0) {
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
            if (Float.compare(currentScore, 0) > 0) {
                return currentDocId;
            } else {
                return nextDoc();
            }
        }
    }

    static class AvgParentScorer extends ParentScorer {

        final TObjectIntMap<HashedBytesArray> uidToCount;
        HashedBytesArray currentUid;

        AvgParentScorer(Weight weight, IdReaderTypeCache idTypeCache, TObjectFloatMap<HashedBytesArray> uidToScore, TObjectIntMap<HashedBytesArray> uidToCount, DocIdSetIterator parentsIterator) {
            super(weight, idTypeCache, uidToScore, parentsIterator);
            this.uidToCount = uidToCount;
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
                if (Float.compare(currentScore, 0) > 0) {
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

            currentUid = idTypeCache.idByDoc(currentDocId);
            currentScore = uidToScore.get(currentUid);
            if (Float.compare(currentScore, 0) > 0) {
                currentScore /= uidToCount.get(currentUid);
                return currentDocId;
            }
            else
            {
            	return nextDoc();
            }
        }
    }

    static class ChildUidCollector extends NoopCollector {

        final TObjectFloatHashMap<HashedBytesArray> uidToScore;
        final ScoreType scoreType;
        final SearchContext searchContext;
        final String childType;

        Scorer scorer;
        IdReaderTypeCache typeCache;

        ChildUidCollector(ScoreType scoreType, SearchContext searchContext, String childType, TObjectFloatHashMap<HashedBytesArray> uidToScore) {
            this.uidToScore = uidToScore;
            this.scoreType = scoreType;
            this.searchContext = searchContext;
            this.childType = childType;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (typeCache == null) {
                return;
            }

            HashedBytesArray parentUid = typeCache.parentIdByDoc(doc);
            float previousScore = uidToScore.get(parentUid);
            float currentScore = scorer.score();
            if (Float.compare(previousScore, 0) == 0) {
                uidToScore.put(parentUid, currentScore);
            } else {
                switch (scoreType) {
                    case SUM:
                        uidToScore.adjustValue(parentUid, currentScore);
                        break;
                    case MAX:
                        if (Float.compare(previousScore, currentScore) < 0) {
                            uidToScore.put(parentUid, currentScore);
                        }
                        break;
                }
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            typeCache = searchContext.idCache().reader(context.reader()).type(childType);
        }

    }

    static class AvgChildUidCollector extends ChildUidCollector {

        final TObjectIntHashMap<HashedBytesArray> uidToCount;

        AvgChildUidCollector(ScoreType scoreType, SearchContext searchContext, String childType, TObjectFloatHashMap<HashedBytesArray> uidToScore, TObjectIntHashMap<HashedBytesArray> uidToCount) {
            super(scoreType, searchContext, childType, uidToScore);
            this.uidToCount = uidToCount;
            assert scoreType == ScoreType.AVG;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (typeCache == null) {
                return;
            }

            HashedBytesArray parentUid = typeCache.parentIdByDoc(doc);
            float previousScore = uidToScore.get(parentUid);
            float currentScore = scorer.score();
            if (Float.compare(previousScore, 0) == 0) {
                uidToScore.put(parentUid, currentScore);
                uidToCount.put(parentUid, 1);
            } else {
                uidToScore.adjustValue(parentUid, currentScore);
                uidToCount.increment(parentUid);
            }
        }

    }

}

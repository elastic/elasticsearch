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

package org.elasticsearch.search.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_COUNT;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_TOP_HITS;

/**
 * A {@link QueryCollectorContext} that creates top docs collector
 */
abstract class TopDocsCollectorContext extends QueryCollectorContext {
    protected final int numHits;

    TopDocsCollectorContext(String profilerName, int numHits) {
        super(profilerName);
        this.numHits = numHits;
    }

    /**
     * Returns the number of top docs to retrieve
     */
    final int numHits() {
        return numHits;
    }

    /**
     * Returns true if the top docs should be re-scored after initial search
     */
    boolean shouldRescore() {
        return false;
    }

    static class TotalHitCountCollectorContext extends TopDocsCollectorContext {
        private final TotalHitCountCollector collector;
        private final int hitCount;

        /**
         * Ctr
         * @param reader The index reader
         * @param query The query to execute
         * @param shouldCollect True if any previous collector context in the chain forces the search to be executed, false otherwise
         */
        private TotalHitCountCollectorContext(IndexReader reader, Query query, boolean shouldCollect) throws IOException {
            super(REASON_SEARCH_COUNT, 0);
            this.collector = new TotalHitCountCollector();
            // implicit total hit counts are valid only when there is no filter collector in the chain
            // so we check the shortcut only if shouldCollect is true
            this.hitCount = shouldCollect ? -1 : shortcutTotalHitCount(reader, query);
        }

        @Override
        boolean shouldCollect() {
            return hitCount == -1;
        }

        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result, boolean hasCollected) {
            final int totalHitCount;
            if (hasCollected) {
                totalHitCount = collector.getTotalHits();
            } else {
                assert hitCount != -1;
                totalHitCount = hitCount;
            }
            result.topDocs(new TopDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS, 0), null);
        }
    }

    static class CollapsingTopDocsCollectorContext extends TopDocsCollectorContext {
        private final DocValueFormat[] sortFmt;
        private final CollapsingTopDocsCollector<?> topDocsCollector;

        /**
         * Ctr
         * @param collapseContext The collapsing context
         * @param sortAndFormats The query sort
         * @param numHits The number of collapsed top hits to retrieve.
         * @param trackMaxScore True if max score should be tracked
         */
        private CollapsingTopDocsCollectorContext(CollapseContext collapseContext,
                                                  @Nullable SortAndFormats sortAndFormats,
                                                  int numHits,
                                                  boolean trackMaxScore) {
            super(REASON_SEARCH_TOP_HITS, numHits);
            assert numHits > 0;
            assert collapseContext != null;
            Sort sort = sortAndFormats == null ? Sort.RELEVANCE : sortAndFormats.sort;
            this.sortFmt = sortAndFormats == null ? new DocValueFormat[] { DocValueFormat.RAW } : sortAndFormats.formats;
            this.topDocsCollector = collapseContext.createTopDocs(sort, numHits, trackMaxScore);
        }

        @Override
        Collector create(Collector in) throws IOException {
            assert in == null;
            return topDocsCollector;
        }

        @Override
        void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {
            assert hasCollected;
            result.topDocs(topDocsCollector.getTopDocs(), sortFmt);
        }
    }

    abstract static class SimpleTopDocsCollectorContext extends TopDocsCollectorContext {
        private final @Nullable SortAndFormats sortAndFormats;
        private final TopDocsCollector<?> topDocsCollector;

        /**
         * Ctr
         * @param sortAndFormats The query sort
         * @param numHits The number of top hits to retrieve
         * @param searchAfter The doc this request should "search after"
         * @param trackMaxScore True if max score should be tracked
         */
        private SimpleTopDocsCollectorContext(@Nullable SortAndFormats sortAndFormats,
                                              @Nullable ScoreDoc searchAfter,
                                              int numHits,
                                              boolean trackMaxScore) throws IOException {
            super(REASON_SEARCH_TOP_HITS, numHits);
            this.sortAndFormats = sortAndFormats;
            if (sortAndFormats == null) {
                this.topDocsCollector = TopScoreDocCollector.create(numHits, searchAfter);
            } else {
                this.topDocsCollector = TopFieldCollector.create(sortAndFormats.sort, numHits,
                    (FieldDoc) searchAfter, true, trackMaxScore, trackMaxScore);
            }
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return topDocsCollector;
        }

        @Override
        void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {
            assert hasCollected;
            final TopDocs topDocs = topDocsCollector.topDocs();
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    static class ScrollingTopDocsCollectorContext extends SimpleTopDocsCollectorContext {
        private final ScrollContext scrollContext;
        private final int numberOfShards;

        private ScrollingTopDocsCollectorContext(ScrollContext scrollContext,
                                                 @Nullable SortAndFormats sortAndFormats,
                                                 int numHits,
                                                 boolean trackMaxScore,
                                                 int numberOfShards) throws IOException {
            super(sortAndFormats, scrollContext.lastEmittedDoc, numHits, trackMaxScore);
            this.scrollContext = Objects.requireNonNull(scrollContext);
            this.numberOfShards = numberOfShards;
        }

        @Override
        void postProcess(QuerySearchResult result, boolean hasCollected) throws IOException {
            super.postProcess(result, hasCollected);
            final TopDocs topDocs = result.topDocs();
            if (scrollContext.totalHits == -1) {
                // first round
                scrollContext.totalHits = topDocs.totalHits;
                scrollContext.maxScore = topDocs.getMaxScore();
            } else {
                // subsequent round: the total number of hits and
                // the maximum score were computed on the first round
                topDocs.totalHits = scrollContext.totalHits;
                topDocs.setMaxScore(scrollContext.maxScore);
            }
            if (numberOfShards == 1) {
                // if we fetch the document in the same roundtrip, we already know the last emitted doc
                if (topDocs.scoreDocs.length > 0) {
                    // set the last emitted doc
                    scrollContext.lastEmittedDoc = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                }
            }
            result.topDocs(topDocs, result.sortValueFormats());
        }
    }

    /**
     * Returns query total hit count if the <code>query</code> is a {@link MatchAllDocsQuery}
     * or a {@link TermQuery} and the <code>reader</code> has no deletions,
     * -1 otherwise.
     */
    static int shortcutTotalHitCount(IndexReader reader, Query query) throws IOException {
        while (true) {
            // remove wrappers that don't matter for counts
            // this is necessary so that we don't only optimize match_all
            // queries but also match_all queries that are nested in
            // a constant_score query
            if (query instanceof ConstantScoreQuery) {
                query = ((ConstantScoreQuery) query).getQuery();
            } else if (query instanceof BoostQuery) {
                query = ((BoostQuery) query).getQuery();
            } else {
                break;
            }
        }
        if (query.getClass() == MatchAllDocsQuery.class) {
            return reader.numDocs();
        } else if (query.getClass() == TermQuery.class && reader.hasDeletions() == false) {
            final Term term = ((TermQuery) query).getTerm();
            int count = 0;
            for (LeafReaderContext context : reader.leaves()) {
                count += context.reader().docFreq(term);
            }
            return count;
        } else {
            return -1;
        }
    }

    /**
     * Creates a {@link TopDocsCollectorContext} from the provided <code>searchContext</code>
     */
    static TopDocsCollectorContext createTopDocsCollectorContext(SearchContext searchContext,
                                                                 IndexReader reader,
                                                                 boolean shouldCollect) throws IOException {
        final Query query = searchContext.query();
        // top collectors don't like a size of 0
        final int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.size() == 0) {
            // no matter what the value of from is
            return new TotalHitCountCollectorContext(reader, query, shouldCollect);
        } else if (searchContext.scrollContext() != null) {
            // no matter what the value of from is
            int numDocs = Math.min(searchContext.size(), totalNumDocs);
            return new ScrollingTopDocsCollectorContext(searchContext.scrollContext(),
                searchContext.sort(), numDocs, searchContext.trackScores(), searchContext.numberOfShards());
        } else if (searchContext.collapse() != null) {
            boolean trackScores = searchContext.sort() == null ? true : searchContext.trackScores();
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
            return new CollapsingTopDocsCollectorContext(searchContext.collapse(),
                searchContext.sort(), numDocs, trackScores);
        } else {
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
            final boolean rescore = searchContext.rescore().isEmpty() == false;
            if (rescore) {
                assert searchContext.sort() == null;
                for (RescoreContext rescoreContext : searchContext.rescore()) {
                    numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
                }
            }
            return new SimpleTopDocsCollectorContext(searchContext.sort(),
                                                     searchContext.searchAfter(),
                                                     numDocs,
                                                     searchContext.trackScores()) {
                @Override
                boolean shouldRescore() {
                    return rescore;
                }
            };
        }
    }
}

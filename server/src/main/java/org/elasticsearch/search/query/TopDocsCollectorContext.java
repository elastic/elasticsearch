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
import org.apache.lucene.search.MultiCollector;
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
import java.util.function.IntSupplier;
import java.util.function.Supplier;

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

    static class EmptyTopDocsCollectorContext extends TopDocsCollectorContext {
        private final Collector collector;
        private final IntSupplier hitCountSupplier;

        /**
         * Ctr
         * @param reader The index reader
         * @param query The query to execute
         * @param trackTotalHits True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains a filter
         */
        private EmptyTopDocsCollectorContext(IndexReader reader, Query query,
                                             boolean trackTotalHits, boolean hasFilterCollector) throws IOException {
            super(REASON_SEARCH_COUNT, 0);
            if (trackTotalHits) {
                TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
                // implicit total hit counts are valid only when there is no filter collector in the chain
                int hitCount =  hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (hitCount == -1) {
                    this.collector = hitCountCollector;
                    this.hitCountSupplier = hitCountCollector::getTotalHits;
                } else {
                    this.collector = new EarlyTerminatingCollector(hitCountCollector, 0, false);
                    this.hitCountSupplier = () -> hitCount;
                }
            } else {
                this.collector = new EarlyTerminatingCollector(new TotalHitCountCollector(), 0, false);
                // for bwc hit count is set to 0, it will be converted to -1 by the coordinating node
                this.hitCountSupplier = () -> 0;
            }
        }

        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result) {
            final int totalHitCount = hitCountSupplier.getAsInt();
            result.topDocs(new TopDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS, Float.NaN), null);
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
        void postProcess(QuerySearchResult result) throws IOException {
            result.topDocs(topDocsCollector.getTopDocs(), sortFmt);
        }
    }

    abstract static class SimpleTopDocsCollectorContext extends TopDocsCollectorContext {
        private final @Nullable SortAndFormats sortAndFormats;
        private final Collector collector;
        private final IntSupplier totalHitsSupplier;
        private final Supplier<TopDocs> topDocsSupplier;

        /**
         * Ctr
         * @param reader The index reader
         * @param query The Lucene query
         * @param sortAndFormats The query sort
         * @param numHits The number of top hits to retrieve
         * @param searchAfter The doc this request should "search after"
         * @param trackMaxScore True if max score should be tracked
         * @param trackTotalHits True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document
         */
        private SimpleTopDocsCollectorContext(IndexReader reader,
                                              Query query,
                                              @Nullable SortAndFormats sortAndFormats,
                                              @Nullable ScoreDoc searchAfter,
                                              int numHits,
                                              boolean trackMaxScore,
                                              boolean trackTotalHits,
                                              boolean hasFilterCollector) throws IOException {
            super(REASON_SEARCH_TOP_HITS, numHits);
            this.sortAndFormats = sortAndFormats;
            if (sortAndFormats == null) {
                final TopDocsCollector<?> topDocsCollector = TopScoreDocCollector.create(numHits, searchAfter);
                this.collector = topDocsCollector;
                this.topDocsSupplier = topDocsCollector::topDocs;
                this.totalHitsSupplier = topDocsCollector::getTotalHits;
            } else {
                /**
                 * We explicitly don't track total hits in the topdocs collector, it can early terminate
                 * if the sort matches the index sort.
                 */
                final TopDocsCollector<?> topDocsCollector = TopFieldCollector.create(sortAndFormats.sort, numHits,
                    (FieldDoc) searchAfter, true, trackMaxScore, trackMaxScore, false);
                this.topDocsSupplier = topDocsCollector::topDocs;
                if (trackTotalHits) {
                    // implicit total hit counts are valid only when there is no filter collector in the chain
                    int count = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                    if (count != -1) {
                        // we can extract the total count from the shard statistics directly
                        this.totalHitsSupplier = () -> count;
                        this.collector = topDocsCollector;
                    } else {
                        // wrap a collector that counts the total number of hits even
                        // if the top docs collector terminates early
                        final TotalHitCountCollector countingCollector = new TotalHitCountCollector();
                        this.collector = MultiCollector.wrap(topDocsCollector, countingCollector);
                        this.totalHitsSupplier = countingCollector::getTotalHits;
                    }
                } else {
                    // total hit count is not needed
                    this.collector = topDocsCollector;
                    this.totalHitsSupplier = topDocsCollector::getTotalHits;
                }
            }
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            final TopDocs topDocs = topDocsSupplier.get();
            topDocs.totalHits = totalHitsSupplier.getAsInt();
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    static class ScrollingTopDocsCollectorContext extends SimpleTopDocsCollectorContext {
        private final ScrollContext scrollContext;
        private final int numberOfShards;

        private ScrollingTopDocsCollectorContext(IndexReader reader,
                                                 Query query,
                                                 ScrollContext scrollContext,
                                                 @Nullable SortAndFormats sortAndFormats,
                                                 int numHits,
                                                 boolean trackMaxScore,
                                                 int numberOfShards,
                                                 boolean trackTotalHits,
                                                 boolean hasFilterCollector) throws IOException {
            super(reader, query, sortAndFormats, scrollContext.lastEmittedDoc, numHits, trackMaxScore,
                trackTotalHits, hasFilterCollector);
            this.scrollContext = Objects.requireNonNull(scrollContext);
            this.numberOfShards = numberOfShards;
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            super.postProcess(result);
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
     * Creates a {@link TopDocsCollectorContext} from the provided <code>searchContext</code>.
     * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document.
     */
    static TopDocsCollectorContext createTopDocsCollectorContext(SearchContext searchContext,
                                                                 IndexReader reader,
                                                                 boolean hasFilterCollector) throws IOException {
        final Query query = searchContext.query();
        // top collectors don't like a size of 0
        final int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.size() == 0) {
            // no matter what the value of from is
            return new EmptyTopDocsCollectorContext(reader, query, searchContext.trackTotalHits(), hasFilterCollector);
        } else if (searchContext.scrollContext() != null) {
            // no matter what the value of from is
            int numDocs = Math.min(searchContext.size(), totalNumDocs);
            return new ScrollingTopDocsCollectorContext(reader, query, searchContext.scrollContext(),
                searchContext.sort(), numDocs, searchContext.trackScores(), searchContext.numberOfShards(),
                searchContext.trackTotalHits(), hasFilterCollector);
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
            return new SimpleTopDocsCollectorContext(reader, query, searchContext.sort(), searchContext.searchAfter(), numDocs,
                                                     searchContext.trackScores(), searchContext.trackTotalHits(), hasFilterCollector) {
                @Override
                boolean shouldRescore() {
                    return rescore;
                }
            };
        }
    }
}

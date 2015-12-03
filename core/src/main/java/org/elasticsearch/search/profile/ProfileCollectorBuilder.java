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

package org.elasticsearch.search.profile;

import org.apache.lucene.search.*;
import org.apache.lucene.util.Counter;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.MinimumScoreCollector;
import org.elasticsearch.common.lucene.search.FilteredCollector;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.internal.ScrollContext;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Convenience object to help wrap collectors when profiling is enabled
 */
public class ProfileCollectorBuilder{

    private final boolean profile;
    private boolean collectionDisabled = false;
    private Collector currentCollector;
    private Callable<TopDocs> topDocsCallable;

    public ProfileCollectorBuilder(boolean profile) {
        this.profile = profile;
    }

    public void addTotalHitCountCollector() {
        if (collectionDisabled) { return; }

        TotalHitCountCollector newCollector = new TotalHitCountCollector();
        topDocsCallable = () -> new TopDocs(newCollector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
        currentCollector = newCollector;

        if (profile) {
            currentCollector = new InternalProfileCollector(currentCollector,
                    CollectorResult.REASON_SEARCH_COUNT, Collections.emptyList());
        }
    }


    public void addTopFieldCollector(Sort sort, int numHits, FieldDoc after, boolean fillFields, boolean trackDocScores,
                                     boolean trackMaxScore, ScrollContext scrollContext, SearchType searchType) throws IOException {
        if (collectionDisabled) { return; }

        TopFieldCollector newCollector = TopFieldCollector.create(sort, numHits, after, fillFields, trackDocScores, trackMaxScore);
        topDocsCallable = getScrollCallable(newCollector.topDocs(), scrollContext, searchType);
        currentCollector = newCollector;

        if (profile) {
            currentCollector = new InternalProfileCollector(currentCollector,
                    CollectorResult.REASON_SEARCH_TOP_HITS, Collections.emptyList());
        }
    }

    public void addTopScoreDocCollector(int numHits, ScoreDoc after, ScrollContext scrollContext, SearchType searchType) {
        if (collectionDisabled) { return; }

        TopScoreDocCollector newCollector = TopScoreDocCollector.create(numHits, after);
        topDocsCallable = getScrollCallable(newCollector.topDocs(), scrollContext, searchType);
        currentCollector = newCollector;

        if (profile) {
            currentCollector = new InternalProfileCollector(currentCollector,
                    CollectorResult.REASON_SEARCH_TOP_HITS, Collections.emptyList());
        }
    }

    // nocommit I don't like having to pass in scrollcontext here...
    private Callable<TopDocs> getScrollCallable(final TopDocs topDocs, final ScrollContext scrollContext, final SearchType searchType) {
        return () -> {
            if (scrollContext != null) {
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
                switch (searchType) {
                    case QUERY_AND_FETCH:
                    case DFS_QUERY_AND_FETCH:
                        // for (DFS_)QUERY_AND_FETCH, we already know the last emitted doc
                        if (topDocs.scoreDocs.length > 0) {
                            // set the last emitted doc
                            scrollContext.lastEmittedDoc = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                        }
                        break;
                    default:
                        break;
                }
            }
            return topDocs;
        };
    }

    public void addEarlyTerminatingCollector(int maxCountHits) {
        if (collectionDisabled) { return; }

        Collector newCollector = Lucene.wrapCountBasedEarlyTerminatingCollector(currentCollector, maxCountHits);
        if (profile) {
            newCollector = new InternalProfileCollector(newCollector,
                    CollectorResult.REASON_SEARCH_TERMINATE_AFTER_COUNT,
                    Collections.singletonList((InternalProfileCollector) currentCollector));
        }
        currentCollector = newCollector;
    }

    public void addFilteredCollector(Weight filter) {
        if (collectionDisabled) { return; }

        Collector newCollector = new FilteredCollector(currentCollector, filter);
        if (profile) {
            newCollector = new InternalProfileCollector(newCollector,
                    CollectorResult.REASON_SEARCH_POST_FILTER,
                    Collections.singletonList((InternalProfileCollector) currentCollector));
        }
        currentCollector = newCollector;
    }

    public void addMultiCollector(List<Collector> subCollectors) {
        if (collectionDisabled) { return; }

        subCollectors.add(currentCollector);
        Collector newCollector = MultiCollector.wrap(subCollectors);
        if (profile) {

            if (newCollector instanceof MultiCollector) {
                List<InternalProfileCollector> children = new AbstractList<InternalProfileCollector>() {
                    @Override
                    public InternalProfileCollector get(int index) {
                        return (InternalProfileCollector) subCollectors.get(index);
                    }
                    @Override
                    public int size() {
                        return subCollectors.size();
                    }
                };
                newCollector = new InternalProfileCollector(newCollector,
                        CollectorResult.REASON_SEARCH_MULTI,
                        children);
            }
        }
        currentCollector = newCollector;
    }

    public void addMinimumScoreCollector(float minimumScore) {
        if (collectionDisabled) { return; }

        Collector newCollector = new MinimumScoreCollector(currentCollector, minimumScore);
        if (profile) {
            newCollector = new InternalProfileCollector(newCollector,
                    CollectorResult.REASON_SEARCH_MIN_SCORE,
                    Collections.singletonList((InternalProfileCollector) currentCollector));
        }
        currentCollector = newCollector;
    }

    public void addTimeLimitingCollector(final Counter counter, long timeoutInMillis) {
        if (collectionDisabled) { return; }

        Collector newCollector = Lucene.wrapTimeLimitingCollector(currentCollector, counter, timeoutInMillis);
        if (profile) {
            newCollector = new InternalProfileCollector(newCollector,
                    CollectorResult.REASON_SEARCH_TIMEOUT,
                    Collections.singletonList((InternalProfileCollector) currentCollector));
        }
        currentCollector = newCollector;
    }

    public void addBucketCollector(List<Aggregator> collectors, String reason) throws IOException {
        if (collectionDisabled) { return; }

        Collector newCollector = BucketCollector.wrap(collectors);
        currentCollector = newCollector;

        // nocommit I don't like how this is hidden away in here...
        ((BucketCollector)newCollector).preCollection();
        if (profile) {
            currentCollector = new InternalProfileCollector(newCollector, reason, Collections.emptyList());
        }
    }

    public Collector buildCollector() {
        return currentCollector;
    }

    public Callable<TopDocs> buildTopDocsCallable() {
        return topDocsCallable;
    }

    /**
     *  The rest of the methods are required because of the singular TotalHitCountCollector
     *  optimization in QueryPhase (e.g. removing the collector, using a special TopDocs callable)
     *
     *  Not elegant at all :(
     */

    public void disableCollection() {
        currentCollector = null;
        collectionDisabled = true;
    }

    public boolean isCollectionEnabled() {
        return !collectionDisabled;
    }

    public Class<? extends Collector> getCollectorClass() {
        if (currentCollector instanceof InternalProfileCollector) {
            return ((InternalProfileCollector) currentCollector).getWrappedCollector().getClass();
        }
        return currentCollector.getClass();
    }

    public void setTopDocsCallable(Callable<TopDocs> callable) {
        topDocsCallable = callable;
    }


}


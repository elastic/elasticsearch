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

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import static org.elasticsearch.action.search.SearchPhaseController.mergeTopDocs;
import static org.elasticsearch.action.search.SearchResponse.Clusters;

/**
 * Merges multiple search responses into one. Used in cross-cluster search when reduction is performed locally on each cluster.
 * The CCS coordinating node sends one search request per remote cluster involved and gets one search response back from each one of them.
 * Such responses contain all the info to be able to perform an additional reduction and return results back to the user.
 * Preconditions are that only non final reduction has been performed on each cluster, meaning that buckets have not been pruned locally
 * and pipeline aggregations have not yet been executed. Also, from+size search hits need to be requested to each cluster and such results
 * have all already been fetched downstream.
 * This approach consists of a different trade-off compared to ordinary cross-cluster search where we fan out to all the shards, no matter
 * whether they belong to the local or the remote cluster. Assuming that there commonly is network latency when communicating with remote
 * clusters, limiting the number of requests to one per cluster is beneficial, and outweighs the downside of fetching many more hits than
 * needed downstream and returning bigger responses to the coordinating node.
 * Known limitations:
 * - scroll requests are not supported
 * - field collapsing is supported, but whenever inner_hits are requested, they will be retrieved by each cluster locally after the fetch
 * phase, through the {@link ExpandSearchPhase}. Such inner_hits are not merged together as part of hits reduction.
 */
//TODO it may make sense to integrate the remote clusters responses as a shard response in the initial search phase and ignore hits coming
//from the remote clusters in the fetch phase. This would be identical to the removed QueryAndFetch strategy except that only the remote
//cluster response would have the fetch results.
final class SearchResponseMerger {
    private final int from;
    private final int size;
    private final int trackTotalHitsUpTo;
    private final SearchTimeProvider searchTimeProvider;
    private final Function<Boolean, ReduceContext> reduceContextFunction;
    private final List<SearchResponse> searchResponses = new CopyOnWriteArrayList<>();

    SearchResponseMerger(int from, int size, int trackTotalHitsUpTo, SearchTimeProvider searchTimeProvider,
                         Function<Boolean, ReduceContext> reduceContextFunction) {
        this.from = from;
        this.size = size;
        this.trackTotalHitsUpTo = trackTotalHitsUpTo;
        this.searchTimeProvider = Objects.requireNonNull(searchTimeProvider);
        this.reduceContextFunction = Objects.requireNonNull(reduceContextFunction);
    }

    /**
     * Add a search response to the list of responses to be merged together into one.
     * Merges currently happen at once when all responses are available and {@link #getMergedResponse(Clusters)} )} is called.
     * That may change in the future as it's possible to introduce incremental merges as responses come in if necessary.
     */
    void add(SearchResponse searchResponse) {
        searchResponses.add(searchResponse);
    }

    /**
     * Returns the merged response. To be called once all responses have been added through {@link #add(SearchResponse)}
     * so that all responses are merged into a single one.
     */
    SearchResponse getMergedResponse(Clusters clusters) {
        assert searchResponses.size() > 1;
        int totalShards = 0;
        int skippedShards = 0;
        int successfulShards = 0;
        //the current reduce phase counts as one
        int numReducePhases = 1;
        List<ShardSearchFailure> failures = new ArrayList<>();
        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        List<InternalAggregations> aggs = new ArrayList<>();
        Map<ShardId, Integer> shards = new TreeMap<>();
        List<TopDocs> topDocsList = new ArrayList<>(searchResponses.size());
        Map<String, List<Suggest.Suggestion>> groupedSuggestions = new HashMap<>();
        Boolean trackTotalHits = null;

        TopDocsStats topDocsStats = new TopDocsStats(trackTotalHitsUpTo);

        for (SearchResponse searchResponse : searchResponses) {
            totalShards += searchResponse.getTotalShards();
            skippedShards += searchResponse.getSkippedShards();
            successfulShards += searchResponse.getSuccessfulShards();
            numReducePhases += searchResponse.getNumReducePhases();

            Collections.addAll(failures, searchResponse.getShardFailures());

            profileResults.putAll(searchResponse.getProfileResults());

            if (searchResponse.getAggregations() != null) {
                InternalAggregations internalAggs = (InternalAggregations) searchResponse.getAggregations();
                aggs.add(internalAggs);
            }

            Suggest suggest = searchResponse.getSuggest();
            if (suggest != null) {
                for (Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> entries : suggest) {
                    List<Suggest.Suggestion> suggestionList = groupedSuggestions.computeIfAbsent(entries.getName(), s -> new ArrayList<>());
                    suggestionList.add(entries);
                }
            }

            SearchHits searchHits = searchResponse.getHits();

            final TotalHits totalHits;
            if (searchHits.getTotalHits() == null) {
                //in case we didn't track total hits, we get null from each cluster, but we need to set 0 eq to the TopDocs
                totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
                assert trackTotalHits == null || trackTotalHits == false;
                trackTotalHits = false;
            } else {
                totalHits = searchHits.getTotalHits();
                assert trackTotalHits == null || trackTotalHits;
                trackTotalHits = true;
            }
            TopDocs topDocs = searchHitsToTopDocs(searchHits, totalHits, shards);
            topDocsStats.add(new TopDocsAndMaxScore(topDocs, searchHits.getMaxScore()),
                searchResponse.isTimedOut(), searchResponse.isTerminatedEarly());
            topDocsList.add(topDocs);
        }

        //after going through all the hits and collecting all their distinct shards, we can assign shardIndex and set it to the ScoreDocs
        setShardIndex(shards, topDocsList);
        TopDocs topDocs = mergeTopDocs(topDocsList, size, from);
        SearchHits mergedSearchHits = topDocsToSearchHits(topDocs, topDocsStats);
        Suggest suggest = groupedSuggestions.isEmpty() ? null : new Suggest(Suggest.reduce(groupedSuggestions));
        InternalAggregations reducedAggs = InternalAggregations.reduce(aggs, reduceContextFunction.apply(true));
        ShardSearchFailure[] shardFailures = failures.toArray(ShardSearchFailure.EMPTY_ARRAY);
        //make failures ordering consistent with ordinary search and CCS
        Arrays.sort(shardFailures, FAILURES_COMPARATOR);
        InternalSearchResponse response = new InternalSearchResponse(mergedSearchHits, reducedAggs, suggest,
            new SearchProfileShardResults(profileResults), topDocsStats.timedOut, topDocsStats.terminatedEarly, numReducePhases);
        long tookInMillis = searchTimeProvider.buildTookInMillis();
        return new SearchResponse(response, null, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters);
    }

    private static final Comparator<ShardSearchFailure> FAILURES_COMPARATOR = new Comparator<ShardSearchFailure>() {
        @Override
        public int compare(ShardSearchFailure o1, ShardSearchFailure o2) {
            ShardId shardId1 = extractShardId(o1);
            ShardId shardId2 = extractShardId(o2);
            if (shardId1 == null && shardId2 == null) {
                return 0;
            }
            if (shardId1 == null) {
                return -1;
            }
            if (shardId2 == null) {
                return 1;
            }
            return shardId1.compareTo(shardId2);
        }

        private ShardId extractShardId(ShardSearchFailure failure) {
            SearchShardTarget shard = failure.shard();
            if (shard != null) {
                return shard.getShardId();
            }
            Throwable cause = failure.getCause();
            if (cause instanceof ElasticsearchException) {
                ElasticsearchException e = (ElasticsearchException) cause;
                return e.getShardId();
            }
            return null;
        }
    };

    private static TopDocs searchHitsToTopDocs(SearchHits searchHits, TotalHits totalHits, Map<ShardId, Integer> shards) {
        SearchHit[] hits = searchHits.getHits();
        ScoreDoc[] scoreDocs = new ScoreDoc[hits.length];
        final TopDocs topDocs;
        if (searchHits.getSortFields() != null) {
            if (searchHits.getCollapseField() != null) {
                assert searchHits.getCollapseValues() != null;
                topDocs = new CollapseTopFieldDocs(searchHits.getCollapseField(), totalHits, scoreDocs,
                    searchHits.getSortFields(), searchHits.getCollapseValues());
            } else {
                topDocs = new TopFieldDocs(totalHits, scoreDocs, searchHits.getSortFields());
            }
        } else {
            topDocs = new TopDocs(totalHits, scoreDocs);
        }

        for (int i = 0; i < hits.length; i++) {
            SearchHit hit = hits[i];
            ShardId shardId = hit.getShard().getShardId();
            shards.putIfAbsent(shardId, null);
            final SortField[] sortFields = searchHits.getSortFields();
            final Object[] sortValues;
            if (sortFields == null) {
                sortValues = null;
            } else {
                if (sortFields.length == 1 && sortFields[0].getType() == SortField.Type.SCORE) {
                    sortValues = new Object[]{hit.getScore()};
                } else {
                    sortValues = hit.getRawSortValues();
                }
            }
            scoreDocs[i] = new FieldDocAndSearchHit(hit.docId(), hit.getScore(), sortValues, hit);
        }
        return topDocs;
    }

    private static void setShardIndex(Map<ShardId, Integer> shards, List<TopDocs> topDocsList) {
        int shardIndex = 0;
        for (Map.Entry<ShardId, Integer> shard : shards.entrySet()) {
            shard.setValue(shardIndex++);
        }
        //and go through all the scoreDocs from each cluster and set their corresponding shardIndex
        for (TopDocs topDocs : topDocsList) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                FieldDocAndSearchHit fieldDocAndSearchHit = (FieldDocAndSearchHit) scoreDoc;
                //When hits come from the indices with same names on multiple clusters and same shard identifier, we rely on such indices
                //to have a different uuid across multiple clusters. That's how they will get a different shardIndex.
                ShardId shardId = fieldDocAndSearchHit.searchHit.getShard().getShardId();
                fieldDocAndSearchHit.shardIndex = shards.get(shardId);
            }
        }
    }

    private static SearchHits topDocsToSearchHits(TopDocs topDocs, TopDocsStats topDocsStats) {
        SearchHit[] searchHits = new SearchHit[topDocs.scoreDocs.length];
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            FieldDocAndSearchHit scoreDoc = (FieldDocAndSearchHit)topDocs.scoreDocs[i];
            searchHits[i] = scoreDoc.searchHit;
        }

        SortField[] sortFields = null;
        String collapseField = null;
        Object[] collapseValues = null;
        if (topDocs instanceof TopFieldDocs) {
            sortFields = ((TopFieldDocs)topDocs).fields;
            if (topDocs instanceof CollapseTopFieldDocs) {
                CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs)topDocs;
                collapseField = collapseTopFieldDocs.field;
                collapseValues = collapseTopFieldDocs.collapseValues;
            }
        }
        return new SearchHits(searchHits, topDocsStats.getTotalHits(), topDocsStats.getMaxScore(),
            sortFields, collapseField, collapseValues);
    }

    private static final class FieldDocAndSearchHit extends FieldDoc {
        private final SearchHit searchHit;

        //to simplify things, we use a FieldDoc all the time, even when only a ScoreDoc is needed, in which case fields are null.
        FieldDocAndSearchHit(int doc, float score, Object[] fields, SearchHit searchHit) {
            super(doc, score, fields);
            this.searchHit = searchHit;
        }
    }
}

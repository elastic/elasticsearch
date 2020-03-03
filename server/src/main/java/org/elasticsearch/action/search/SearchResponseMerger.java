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
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

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
    final int from;
    final int size;
    final int trackTotalHitsUpTo;
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
        assert searchResponse.getScrollId() == null : "merging scroll results is not supported";
        searchResponses.add(searchResponse);
    }

    int numResponses() {
        return searchResponses.size();
    }

    /**
     * Returns the merged response. To be called once all responses have been added through {@link #add(SearchResponse)}
     * so that all responses are merged into a single one.
     */
    SearchResponse getMergedResponse(Clusters clusters) {
        //if the search is only across remote clusters, none of them are available, and all of them have skip_unavailable set to true,
        //we end up calling merge without anything to merge, we just return an empty search response
        if (searchResponses.size() == 0) {
            return SearchResponse.empty(searchTimeProvider::buildTookInMillis, clusters);
        }
        int totalShards = 0;
        int skippedShards = 0;
        int successfulShards = 0;
        //the current reduce phase counts as one
        int numReducePhases = 1;
        List<ShardSearchFailure> failures = new ArrayList<>();
        Map<String, ProfileShardResult> profileResults = new HashMap<>();
        List<InternalAggregations> aggs = new ArrayList<>();
        Map<ShardIdAndClusterAlias, Integer> shards = new TreeMap<>();
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
                List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
                for (CompletionSuggestion completionSuggestion : completionSuggestions) {
                    for (CompletionSuggestion.Entry options : completionSuggestion) {
                        for (CompletionSuggestion.Entry.Option option : options) {
                            SearchShardTarget shard = option.getHit().getShard();
                            ShardIdAndClusterAlias shardId = new ShardIdAndClusterAlias(shard.getShardId(), shard.getClusterAlias());
                            shards.putIfAbsent(shardId, null);
                        }
                    }
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
            if (searchHits.getHits().length > 0) {
                //there is no point in adding empty search hits and merging them with the others. Also, empty search hits always come
                //without sort fields and collapse info, despite sort by field and/or field collapsing was requested, which causes
                //issues reconstructing the proper TopDocs instance and breaks mergeTopDocs which expects the same type for each result.
                topDocsList.add(topDocs);
            }
        }

        //after going through all the hits and collecting all their distinct shards, we assign shardIndex and set it to the ScoreDocs
        setTopDocsShardIndex(shards, topDocsList);
        TopDocs topDocs = mergeTopDocs(topDocsList, size, from);
        SearchHits mergedSearchHits = topDocsToSearchHits(topDocs, topDocsStats);
        setSuggestShardIndex(shards, groupedSuggestions);
        Suggest suggest = groupedSuggestions.isEmpty() ? null : new Suggest(Suggest.reduce(groupedSuggestions));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, reduceContextFunction.apply(true));
        ShardSearchFailure[] shardFailures = failures.toArray(ShardSearchFailure.EMPTY_ARRAY);
        SearchProfileShardResults profileShardResults = profileResults.isEmpty() ? null : new SearchProfileShardResults(profileResults);
        //make failures ordering consistent between ordinary search and CCS by looking at the shard they come from
        Arrays.sort(shardFailures, FAILURES_COMPARATOR);
        InternalSearchResponse response = new InternalSearchResponse(mergedSearchHits, reducedAggs, suggest, profileShardResults,
            topDocsStats.timedOut, topDocsStats.terminatedEarly, numReducePhases);
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
            int shardIdCompare = shardId1.compareTo(shardId2);
            //we could assume that the same shard id cannot come back from multiple clusters as even with same index name and shard index,
            //the index uuid does not match. But the same cluster can be registered multiple times with different aliases, in which case
            //we may get failures from the same index, yet with a different cluster alias in their shard target.
            if (shardIdCompare != 0) {
                return shardIdCompare;
            }
            String clusterAlias1 = o1.shard() == null ? null : o1.shard().getClusterAlias();
            String clusterAlias2 = o2.shard() == null ? null : o2.shard().getClusterAlias();
            if (clusterAlias1 == null && clusterAlias2 == null) {
                return 0;
            }
            if (clusterAlias1 == null) {
                return -1;
            }
            if (clusterAlias2 == null) {
                return 1;
            }
            return clusterAlias1.compareTo(clusterAlias2);
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

    private static TopDocs searchHitsToTopDocs(SearchHits searchHits, TotalHits totalHits, Map<ShardIdAndClusterAlias, Integer> shards) {
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
            SearchShardTarget shard = hit.getShard();
            ShardIdAndClusterAlias shardId = new ShardIdAndClusterAlias(shard.getShardId(), shard.getClusterAlias());
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

    private static void setTopDocsShardIndex(Map<ShardIdAndClusterAlias, Integer> shards, List<TopDocs> topDocsList) {
        assignShardIndex(shards);
        //go through all the scoreDocs from each cluster and set their corresponding shardIndex
        for (TopDocs topDocs : topDocsList) {
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                FieldDocAndSearchHit fieldDocAndSearchHit = (FieldDocAndSearchHit) scoreDoc;
                SearchShardTarget shard = fieldDocAndSearchHit.searchHit.getShard();
                ShardIdAndClusterAlias shardId = new ShardIdAndClusterAlias(shard.getShardId(), shard.getClusterAlias());
                assert shards.containsKey(shardId);
                fieldDocAndSearchHit.shardIndex = shards.get(shardId);
            }
        }
    }

    private static void setSuggestShardIndex(Map<ShardIdAndClusterAlias, Integer> shards,
                                             Map<String, List<Suggest.Suggestion>> groupedSuggestions) {
        assignShardIndex(shards);
        for (List<Suggest.Suggestion> suggestions : groupedSuggestions.values()) {
            for (Suggest.Suggestion suggestion : suggestions) {
                if (suggestion instanceof CompletionSuggestion) {
                    CompletionSuggestion completionSuggestion = (CompletionSuggestion) suggestion;
                    for (CompletionSuggestion.Entry options : completionSuggestion) {
                        for (CompletionSuggestion.Entry.Option option : options) {
                            SearchShardTarget shard = option.getHit().getShard();
                            ShardIdAndClusterAlias shardId = new ShardIdAndClusterAlias(shard.getShardId(), shard.getClusterAlias());
                            assert shards.containsKey(shardId);
                            option.setShardIndex(shards.get(shardId));
                        }
                    }
                }
            }
        }
    }

    private static void assignShardIndex(Map<ShardIdAndClusterAlias, Integer> shards) {
        //assign a different shardIndex to each shard, based on their shardId natural ordering and their cluster alias
        int shardIndex = 0;
        for (Map.Entry<ShardIdAndClusterAlias, Integer> shard : shards.entrySet()) {
            shard.setValue(shardIndex++);
        }
    }

    private static SearchHits topDocsToSearchHits(TopDocs topDocs, TopDocsStats topDocsStats) {
        SearchHit[] searchHits;
        if (topDocs == null) {
            //merged TopDocs is null whenever all clusters have returned empty hits
            searchHits = new SearchHit[0];
        } else {
            searchHits = new SearchHit[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                FieldDocAndSearchHit scoreDoc = (FieldDocAndSearchHit)topDocs.scoreDocs[i];
                searchHits[i] = scoreDoc.searchHit;
            }
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

    /**
     * This class is used instead of plain {@link ShardId} to support the scenario where the same remote cluster is registered twice using
     * different aliases. In that case searching across the same cluster twice would make an assertion in lucene fail
     * (see TopDocs#tieBreakLessThan line 86). Generally, indices with same names on different clusters have different index uuids which
     * make their ShardIds different, which is not the case if the index is really the same one from the same cluster, in which case we
     * need to look at the cluster alias and make sure to assign a different shardIndex based on that.
     */
    private static final class ShardIdAndClusterAlias implements Comparable<ShardIdAndClusterAlias> {
        private final ShardId shardId;
        private final String clusterAlias;

        ShardIdAndClusterAlias(ShardId shardId, String clusterAlias) {
            this.shardId = shardId;
            assert clusterAlias != null : "clusterAlias is null";
            this.clusterAlias = clusterAlias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardIdAndClusterAlias that = (ShardIdAndClusterAlias) o;
            return shardId.equals(that.shardId) &&
                clusterAlias.equals(that.clusterAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, clusterAlias);
        }

        @Override
        public int compareTo(ShardIdAndClusterAlias o) {
            int shardIdCompareTo = shardId.compareTo(o.shardId);
            if (shardIdCompareTo != 0) {
                return shardIdCompareTo;
            }
            return clusterAlias.compareTo(o.clusterAlias);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardPhase;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RankFeatureShardPhaseTests extends ESTestCase {

    private SearchContext getSearchContext() {
        return new TestSearchContext((SearchExecutionContext) null) {

            private FetchSearchResult fetchResult;
            private RankFeatureResult rankFeatureResult;
            private FetchFieldsContext fetchFieldsContext;
            private StoredFieldsContext storedFieldsContext;

            @Override
            public FetchSearchResult fetchResult() {
                return fetchResult;
            }

            @Override
            public void addFetchResult() {
                this.fetchResult = new FetchSearchResult();
                this.addReleasable(fetchResult::decRef);
            }

            @Override
            public RankFeatureResult rankFeatureResult() {
                return rankFeatureResult;
            }

            @Override
            public void addRankFeatureResult() {
                this.rankFeatureResult = new RankFeatureResult();
                this.addReleasable(rankFeatureResult::decRef);
            }

            @Override
            public SearchContext fetchFieldsContext(FetchFieldsContext fetchFieldsContext) {
                this.fetchFieldsContext = fetchFieldsContext;
                return this;
            }

            @Override
            public FetchFieldsContext fetchFieldsContext() {
                return fetchFieldsContext;
            }

            @Override
            public SearchContext storedFieldsContext(StoredFieldsContext storedFieldsContext) {
                this.storedFieldsContext = storedFieldsContext;
                return this;
            }

            @Override
            public StoredFieldsContext storedFieldsContext() {
                return storedFieldsContext;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }
        };
    }

    private RankBuilder getRankBuilder(final String field) {
        return new RankBuilder(DEFAULT_RANK_WINDOW_SIZE) {
            @Override
            protected void doWriteTo(StreamOutput out) throws IOException {
                // no-op
            }

            @Override
            protected void doXContent(XContentBuilder builder, Params params) throws IOException {
                // no-op
            }

            @Override
            public boolean isCompoundBuilder() {
                return false;
            }

            @Override
            public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
                // no-op
                return baseExplanation;
            }

            // no work to be done on the query phase
            @Override
            public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                return null;
            }

            // no work to be done on the query phase
            @Override
            public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                return null;
            }

            @Override
            public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                return new RankFeaturePhaseRankShardContext(field) {
                    @Override
                    public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                        RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                        for (int i = 0; i < hits.getHits().length; i++) {
                            SearchHit hit = hits.getHits()[i];
                            rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                            rankFeatureDocs[i].featureData(hit.getFields().get(field).getValue());
                            rankFeatureDocs[i].rank = i + 1;
                        }
                        return new RankFeatureShardResult(rankFeatureDocs);
                    }
                };
            }

            // no work to be done on the coordinator node for the rank feature phase
            @Override
            public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
                return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE) {
                    @Override
                    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                        throw new AssertionError("not expected");
                    }
                };
            }

            @Override
            protected boolean doEquals(RankBuilder other) {
                return false;
            }

            @Override
            protected int doHashCode() {
                return 0;
            }

            @Override
            public String getWriteableName() {
                return "rank_builder_rank_feature_shard_phase_enabled";
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersions.V_8_15_0;
            }
        };
    }

    public void testPrepareForFetch() {

        final String fieldName = "some_field";
        int numDocs = randomIntBetween(10, 30);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(getRankBuilder(fieldName));

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        try (SearchContext searchContext = spy(getSearchContext())) {
            when(searchContext.isCancelled()).thenReturn(false);
            when(searchContext.request()).thenReturn(searchRequest);

            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            rankFeatureShardPhase.prepareForFetch(searchContext, request);

            assertNotNull(searchContext.fetchFieldsContext());
            assertEquals(searchContext.fetchFieldsContext().fields().size(), 1);
            assertEquals(searchContext.fetchFieldsContext().fields().get(0).field, fieldName);
            assertNotNull(searchContext.storedFieldsContext());
            assertNull(searchContext.storedFieldsContext().fieldNames());
            assertFalse(searchContext.storedFieldsContext().fetchFields());
            assertNotNull(searchContext.fetchResult());
        }
    }

    public void testPrepareForFetchNoRankFeatureContext() {
        int numDocs = randomIntBetween(10, 30);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(null);

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        try (SearchContext searchContext = spy(getSearchContext())) {
            when(searchContext.isCancelled()).thenReturn(false);
            when(searchContext.request()).thenReturn(searchRequest);

            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            rankFeatureShardPhase.prepareForFetch(searchContext, request);

            assertNull(searchContext.fetchFieldsContext());
            assertNull(searchContext.fetchResult());
        }
    }

    public void testPrepareForFetchWhileTaskIsCancelled() {

        final String fieldName = "some_field";
        int numDocs = randomIntBetween(10, 30);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(getRankBuilder(fieldName));

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        try (SearchContext searchContext = spy(getSearchContext())) {
            when(searchContext.isCancelled()).thenReturn(true);
            when(searchContext.request()).thenReturn(searchRequest);

            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            expectThrows(TaskCancelledException.class, () -> rankFeatureShardPhase.prepareForFetch(searchContext, request));
        }
    }

    public void testProcessFetch() {
        final String fieldName = "some_field";
        int numDocs = randomIntBetween(15, 30);
        Map<Integer, String> expectedFieldData = Map.of(4, "doc_4_aardvark", 9, "doc_9_aardvark", numDocs - 1, "last_doc_aardvark");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(getRankBuilder(fieldName));

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        SearchShardTarget shardTarget = new SearchShardTarget(
            "node_id",
            new ShardId(new Index("some_index", UUID.randomUUID().toString()), 0),
            null
        );
        SearchHits searchHits = null;
        try (SearchContext searchContext = spy(getSearchContext())) {
            searchContext.addFetchResult();
            SearchHit[] hits = new SearchHit[3];
            hits[0] = SearchHit.unpooled(4);
            hits[0].setDocumentField(fieldName, new DocumentField(fieldName, Collections.singletonList(expectedFieldData.get(4))));

            hits[1] = SearchHit.unpooled(9);
            hits[1].setDocumentField(fieldName, new DocumentField(fieldName, Collections.singletonList(expectedFieldData.get(9))));

            hits[2] = SearchHit.unpooled(numDocs - 1);
            hits[2].setDocumentField(
                fieldName,
                new DocumentField(fieldName, Collections.singletonList(expectedFieldData.get(numDocs - 1)))
            );
            searchHits = SearchHits.unpooled(hits, new TotalHits(3, TotalHits.Relation.EQUAL_TO), 1.0f);
            searchContext.fetchResult().shardResult(searchHits, null);
            when(searchContext.isCancelled()).thenReturn(false);
            when(searchContext.request()).thenReturn(searchRequest);
            when(searchContext.shardTarget()).thenReturn(shardTarget);
            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            // this is called as part of the search context initialization
            // with the ResultsType.RANK_FEATURE type
            searchContext.addRankFeatureResult();
            rankFeatureShardPhase.processFetch(searchContext);

            assertNotNull(searchContext.rankFeatureResult());
            assertNotNull(searchContext.rankFeatureResult().rankFeatureResult());
            for (RankFeatureDoc rankFeatureDoc : searchContext.rankFeatureResult().rankFeatureResult().shardResult().rankFeatureDocs) {
                assertTrue(expectedFieldData.containsKey(rankFeatureDoc.doc));
                assertEquals(rankFeatureDoc.featureData, expectedFieldData.get(rankFeatureDoc.doc));
            }
        } finally {
            if (searchHits != null) {
                searchHits.decRef();
            }
        }
    }

    public void testProcessFetchEmptyHits() {
        final String fieldName = "some_field";
        int numDocs = randomIntBetween(10, 30);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(getRankBuilder(fieldName));

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        SearchShardTarget shardTarget = new SearchShardTarget(
            "node_id",
            new ShardId(new Index("some_index", UUID.randomUUID().toString()), 0),
            null
        );

        SearchHits searchHits = null;
        try (SearchContext searchContext = spy(getSearchContext())) {
            searchContext.addFetchResult();
            SearchHit[] hits = new SearchHit[0];
            searchHits = SearchHits.unpooled(hits, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f);
            searchContext.fetchResult().shardResult(searchHits, null);
            when(searchContext.isCancelled()).thenReturn(false);
            when(searchContext.request()).thenReturn(searchRequest);
            when(searchContext.shardTarget()).thenReturn(shardTarget);
            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            // this is called as part of the search context initialization
            // with the ResultsType.RANK_FEATURE type
            searchContext.addRankFeatureResult();
            rankFeatureShardPhase.processFetch(searchContext);

            assertNotNull(searchContext.rankFeatureResult());
            assertNotNull(searchContext.rankFeatureResult().rankFeatureResult());
            assertEquals(searchContext.rankFeatureResult().rankFeatureResult().shardResult().rankFeatureDocs.length, 0);
        } finally {
            if (searchHits != null) {
                searchHits.decRef();
            }
        }
    }

    public void testProcessFetchWhileTaskIsCancelled() {

        final String fieldName = "some_field";
        int numDocs = randomIntBetween(10, 30);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.rankBuilder(getRankBuilder(fieldName));

        ShardSearchRequest searchRequest = mock(ShardSearchRequest.class);
        when(searchRequest.source()).thenReturn(searchSourceBuilder);

        SearchShardTarget shardTarget = new SearchShardTarget(
            "node_id",
            new ShardId(new Index("some_index", UUID.randomUUID().toString()), 0),
            null
        );

        SearchHits searchHits = null;
        try (SearchContext searchContext = spy(getSearchContext())) {
            searchContext.addFetchResult();
            SearchHit[] hits = new SearchHit[0];
            searchHits = SearchHits.unpooled(hits, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 1.0f);
            searchContext.fetchResult().shardResult(searchHits, null);
            when(searchContext.isCancelled()).thenReturn(true);
            when(searchContext.request()).thenReturn(searchRequest);
            when(searchContext.shardTarget()).thenReturn(shardTarget);
            RankFeatureShardRequest request = mock(RankFeatureShardRequest.class);
            when(request.getDocIds()).thenReturn(new int[] { 4, 9, numDocs - 1 });

            RankFeatureShardPhase rankFeatureShardPhase = new RankFeatureShardPhase();
            // this is called as part of the search context initialization
            // with the ResultsType.RANK_FEATURE type
            searchContext.addRankFeatureResult();
            expectThrows(TaskCancelledException.class, () -> rankFeatureShardPhase.processFetch(searchContext));
        } finally {
            if (searchHits != null) {
                searchHits.decRef();
            }
        }
    }
}

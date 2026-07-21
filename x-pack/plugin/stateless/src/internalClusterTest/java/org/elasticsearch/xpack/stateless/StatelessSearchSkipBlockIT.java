/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchShardsAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class StatelessSearchSkipBlockIT extends AbstractStatelessPluginIntegTestCase {

    private final int numShards = randomIntBetween(1, 3);
    private final int numReplicas = randomIntBetween(1, 2);

    public void testSearchWhenIndexSearchShardsAreNotUp() throws Exception {
        // If a new index does not have search shards ready when a search request comes in,
        // the response should have 0 hits as the index was skipped.
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of hits.
        startMasterOnlyNode();
        startIndexNodes(numShards);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas);
        // Wait for 0 shards, else this will hang until search shards are added
        assertAcked(prepareCreate(indexName, indexSettings).setWaitForActiveShards(0));

        int numDocs = randomIntBetween(1, 10);
        indexDocuments(indexName, numDocs);

        var searchRequest = prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery());
        assertHitCount(searchRequest, 0);

        startSearchNodes(randomIntBetween(1, numShards * numReplicas));
        assertBusy(() -> {
            try {
                assertHitCount(searchRequest, numDocs);
            } catch (SearchPhaseExecutionException e) {
                // A SearchPhaseExecutionException may imply a search shard is not yet available.
                // Throwing an AssertionError allows us to retry in the assertBusy loop.
                throw new AssertionError(e);
            }
        });
    }

    public void testMultiSearchWhenIndexSearchShardsAreNotUp() throws Exception {
        // If a new index does not have search shards ready when a multisearch request comes in,
        // the response should have 0 hits as the index was skipped.
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of hits.
        startMasterOnlyNode();
        startIndexNodes(numShards);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas);
        // Wait for 0 shards, else this will hang until search shards are added
        assertAcked(prepareCreate(indexName, indexSettings).setWaitForActiveShards(0));

        int numDocs = randomIntBetween(1, 10);
        indexDocuments(indexName, numDocs);

        var multiSearchRequest = client().prepareMultiSearch()
            .add(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()))
            .add(prepareSearch().setQuery(QueryBuilders.termQuery("field", "blah")));
        assertResponse(multiSearchRequest, response -> {
            assertHitCount(Objects.requireNonNull(response.getResponses()[0].getResponse()), 0);
            assertHitCount(Objects.requireNonNull(response.getResponses()[1].getResponse()), 0);
        });

        startSearchNodes(randomIntBetween(1, numShards * numReplicas));
        assertBusy(() -> assertResponse(multiSearchRequest, response -> {
            try {
                assertHitCount(Objects.requireNonNull(response.getResponses()[0].getResponse()), numDocs);
                assertHitCount(Objects.requireNonNull(response.getResponses()[1].getResponse()), 0);
            } catch (NullPointerException npe) {
                throw new AssertionError(npe);
            }
        }));
    }

    public void testSearchShardsWhenIndexSearchShardsAreNotUp() throws Exception {
        // If a new index does not have search shards ready when a search shards request comes in,
        // there should be no shards in the response.
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of shards in the response.
        startMasterOnlyNode();
        startIndexNodes(numShards);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas);
        // Wait for 0 shards, else this will hang until search shards are added
        assertAcked(prepareCreate(indexName, indexSettings).setWaitForActiveShards(0));

        int numDocs = randomIntBetween(1, 10);
        indexDocuments(indexName, numDocs);

        var response = client().execute(
            TransportSearchShardsAction.TYPE,
            new SearchShardsRequest(new String[] { indexName }, IndicesOptions.DEFAULT, new MatchAllQueryBuilder(), null, null, true, null)
        ).actionGet();
        assertThat(response.getGroups().size(), equalTo(0));

        startSearchNodes(randomIntBetween(1, numShards * numReplicas));
        assertBusy(() -> {
            var subseqResponse = client().execute(
                TransportSearchShardsAction.TYPE,
                new SearchShardsRequest(
                    new String[] { indexName },
                    IndicesOptions.DEFAULT,
                    new MatchAllQueryBuilder(),
                    null,
                    null,
                    true,
                    null
                )
            ).actionGet();
            assertThat(subseqResponse.getGroups().size(), equalTo(numShards));
        });
    }

    public void testOpenPITWhenIndexSearchShardsAreNotUp() {
        // If a new index does not have search shards ready when an open PIT request comes in,
        // the response should have 0 hits as the index was skipped.
        // Once at least one search shard is ready, a subsequent get PIT request should still have 0 hits, even if docs were indexed.
        startMasterOnlyNode();
        startIndexNodes(numShards);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas);
        // Wait for 0 shards, else this will hang until search shards are added
        assertAcked(prepareCreate(indexName, indexSettings).setWaitForActiveShards(0));

        BytesReference pitId = null;
        try {
            boolean allowPartialSearchResults = randomBoolean();
            OpenPointInTimeRequest openPITRequest = new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(10))
                .allowPartialSearchResults(allowPartialSearchResults);

            pitId = client().execute(TransportOpenPointInTimeAction.TYPE, openPITRequest).actionGet().getPointInTimeId();
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(10)))
            );
            assertHitCount(client().search(searchRequest), 0);

            startSearchNodes(randomIntBetween(1, numShards * numReplicas));
            int numDocs = randomIntBetween(1, 10);
            indexDocuments(indexName, numDocs).actionGet();

            // The PIT should 'remember' the index was skipped, even when it's no longer blocked
            assertHitCount(client().search(searchRequest), 0);
        } finally {
            if (pitId != null) {
                client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
            }
        }
    }

    private ActionFuture<BulkResponse> indexDocuments(String indexName, int numDocs) {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = 0; i < numDocs; i++) {
            bulkRequestBuilder.add(prepareIndex(indexName).setId(String.valueOf(i)).setSource("field", "value"));
        }
        return bulkRequestBuilder.execute();
    }
}

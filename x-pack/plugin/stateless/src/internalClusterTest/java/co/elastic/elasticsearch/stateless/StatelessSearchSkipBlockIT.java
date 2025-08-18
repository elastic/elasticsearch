/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
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
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessSearchSkipBlockIT extends AbstractStatelessIntegTestCase {

    private final int numShards = randomIntBetween(1, 3);
    private final int numReplicas = randomIntBetween(1, 2);

    public void testSearchWhenIndexSearchShardsAreNotUp() throws Exception {
        // If a new index does not have search shards ready when a search request comes in:
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is true, the response should have 0 hits as the index was skipped
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is false, the response should be a 503
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of hits
        boolean useBlock = randomBoolean();
        startMasterOnlyNode(Settings.builder().put(MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME, useBlock).build());
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
        if (useBlock) {
            assertHitCount(searchRequest, 0);
        } else {
            assertFailures(searchRequest, RestStatus.SERVICE_UNAVAILABLE, containsString("NoShardAvailableActionException"));
        }

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
        // If a new index does not have search shards ready when a multisearch request comes in:
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is true, the response should have 0 hits as the index was skipped
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is false, the responses should be null
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of hits
        boolean useBlock = randomBoolean();
        startMasterOnlyNode(Settings.builder().put(MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME, useBlock).build());
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
            if (useBlock) {
                assertHitCount(Objects.requireNonNull(response.getResponses()[0].getResponse()), 0);
                assertHitCount(Objects.requireNonNull(response.getResponses()[1].getResponse()), 0);
            } else {
                assertNull(response.getResponses()[0].getResponse());
                assertNull(response.getResponses()[1].getResponse());
            }
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
        // If a new index does not have search shards ready when a search shards request comes in:
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is true, there should be no shards in the response
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is false, there should be the correct number of shards in the response
        // Once at least one search shard is ready, a subsequent request should succeed with the correct number of shards in the response
        boolean useBlock = randomBoolean();
        startMasterOnlyNode(Settings.builder().put(MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME, useBlock).build());
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
        if (useBlock) {
            assertThat(response.getGroups().size(), equalTo(0));
        } else {
            assertThat(response.getGroups().size(), equalTo(numShards));
        }

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
        // If a new index does not have search shards ready when an open PIT request comes in:
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is true, the response should have 0 hits as the index was skipped
        // Once at least one search shard is ready, a subsequent get PIT request should still have 0 hits, even if docs were indexed
        // - if USE_INDEX_REFRESH_BLOCK_SETTING_NAME is false, the response should be a 503
        boolean useBlock = randomBoolean();
        startMasterOnlyNode(Settings.builder().put(MetadataCreateIndexService.USE_INDEX_REFRESH_BLOCK_SETTING_NAME, useBlock).build());
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
            if (useBlock) {
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
            } else {
                Throwable openPITThrowable = assertThrows(
                    SearchPhaseExecutionException.class,
                    () -> client().execute(TransportOpenPointInTimeAction.TYPE, openPITRequest).actionGet()
                );
                if (allowPartialSearchResults) {
                    assertThat(openPITThrowable.getCause(), instanceOf(NoShardAvailableActionException.class));
                } else {
                    assertThat(openPITThrowable.getCause(), instanceOf(SearchPhaseExecutionException.class));
                    assertThat(openPITThrowable.getCause().getMessage(), containsString("Search rejected due to missing shards"));
                }
            }
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

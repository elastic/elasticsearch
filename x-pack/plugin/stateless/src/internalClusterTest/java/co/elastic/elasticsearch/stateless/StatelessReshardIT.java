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

import co.elastic.elasticsearch.stateless.reshard.ReshardIndexRequest;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessReshardIT extends AbstractStatelessIntegTestCase {

    public void testReshardWillRouteDocumentsToNewShard() {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        indexDocs(indexName, 100);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        int oldShardDocs = 0;
        while (true) {
            indexDocs(indexName, 1);
            if (getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 1) > 0L) {
                break;
            }
            oldShardDocs++;
        }

        IndicesStatsResponse postReshardStatsResponse = client().admin().indices().prepareStats(indexName).execute().actionGet();
        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(getIndexCount(postReshardStatsResponse, 0), equalTo(oldShardDocs + 100L));
        assertThat(getIndexCount(postReshardStatsResponse, 1), equalTo(1L));

        indexDocs(indexName, 100);

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), 201L + oldShardDocs);
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(2)
        );
    }

    public void testReshardSearchShardWillNotBeAllocatedUntilIndexingShard() {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();
        startSearchNode();

        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1).build()
        );
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        ReshardIndexRequest request = new ReshardIndexRequest(indexName, ActiveShardCount.NONE);
        client(indexNode).execute(TransportReshardAction.TYPE, request).actionGet();

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(2)
        );

        // Briefly pause to give time for allocation to have theoretically occurred
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));

        final var indexingShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(true);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, indexingShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        final var searchShardExplain = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex(indexName)
            .setShard(1)
            .setPrimary(false);
        assertThat(
            client().execute(TransportClusterAllocationExplainAction.TYPE, searchShardExplain)
                .actionGet()
                .getExplanation()
                .getUnassignedInfo()
                .reason(),
            equalTo(UnassignedInfo.Reason.RESHARD_ADDED)
        );

        // We have to clear the setting to prevent teardown issues with the cluster being red
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), (String) null))
            .get();
    }

    @AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-11036")
    public void testReshardWithIndexClose() throws Exception {
        String indexNode = startMasterAndIndexNode();
        String searchNode = startSearchNode();

        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(1)
        );

        indexDocs(indexName, 100);

        assertThat(getIndexCount(client().admin().indices().prepareStats(indexName).execute().actionGet(), 0), equalTo(100L));

        // assertAcked(indicesAdmin().prepareClose(indexName));
        assertBusy(() -> closeIndices(indexName));
        client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        GetSettingsResponse postReshardSettingsResponse = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .get();

        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(postReshardSettingsResponse.getIndexToSettings().get(indexName)),
            equalTo(2)
        );
    }

    private static long getIndexCount(IndicesStatsResponse statsResponse, int shardId) {
        ShardStats primaryStats = Arrays.stream(statsResponse.getShards())
            .filter(shardStat -> shardStat.getShardRouting().primary() && shardStat.getShardRouting().id() == shardId)
            .findAny()
            .get();
        return primaryStats.getStats().indexing.getTotal().getIndexCount();
    }

    private static void closeIndices(final String... indices) {
        closeIndices(indicesAdmin().prepareClose(indices));
    }

    private static void closeIndices(final CloseIndexRequestBuilder requestBuilder) {
        final CloseIndexResponse response = requestBuilder.get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isShardsAcknowledged(), is(true));

        final String[] indices = requestBuilder.request().indices();
        if (indices != null) {
            assertThat(response.getIndices().size(), equalTo(indices.length));
            for (String index : indices) {
                CloseIndexResponse.IndexResult indexResult = response.getIndices()
                    .stream()
                    .filter(result -> index.equals(result.getIndex().getName()))
                    .findFirst()
                    .get();
                assertThat(indexResult, notNullValue());
                assertThat(indexResult.hasFailures(), is(false));
                assertThat(indexResult.getException(), nullValue());
                assertThat(indexResult.getShards(), notNullValue());
                Arrays.stream(indexResult.getShards()).forEach(shardResult -> {
                    assertThat(shardResult.hasFailures(), is(false));
                    assertThat(shardResult.getFailures(), notNullValue());
                    assertThat(shardResult.getFailures().length, equalTo(0));
                });
            }
        } else {
            assertThat(response.getIndices().size(), equalTo(0));
        }
    }
}

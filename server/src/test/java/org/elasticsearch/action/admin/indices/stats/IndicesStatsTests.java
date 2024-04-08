/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class IndicesStatsTests extends ESSingleNodeTestCase {

    public void testSegmentStatsEmptyIndex() {
        createIndex("test");
        IndicesStatsResponse rsp = indicesAdmin().prepareStats("test").get();
        SegmentsStats stats = rsp.getTotal().getSegments();
        assertEquals(0, stats.getCount());
    }

    public void testSegmentStats() throws Exception {
        IndexModule.Type storeType = IndexModule.defaultStoreType(true);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .field("doc_values", true)
            .field("store", true)
            .endObject()
            .startObject("bar")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .startObject("baz")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(mapping)
                .setSettings(Settings.builder().put("index.store.type", storeType.getSettingsKey()))
        );
        ensureGreen("test");
        prepareIndex("test").setId("1").setSource("foo", "bar", "bar", "baz", "baz", 42).get();
        indicesAdmin().prepareRefresh("test").get();

        IndicesStatsResponse rsp = indicesAdmin().prepareStats("test").get();
        SegmentsStats stats = rsp.getIndex("test").getTotal().getSegments();
        assertThat(stats.getCount(), greaterThan(0L));

        // now check multiple segments stats are merged together
        prepareIndex("test").setId("2").setSource("foo", "bar", "bar", "baz", "baz", 43).get();
        indicesAdmin().prepareRefresh("test").get();

        rsp = indicesAdmin().prepareStats("test").get();
        SegmentsStats stats2 = rsp.getIndex("test").getTotal().getSegments();
        assertThat(stats2.getCount(), greaterThan(stats.getCount()));
    }

    public void testCommitStats() throws Exception {
        createIndex("test");
        ensureGreen("test");

        IndicesStatsResponse rsp = indicesAdmin().prepareStats("test").get();
        for (ShardStats shardStats : rsp.getIndex("test").getShards()) {
            final CommitStats commitStats = shardStats.getCommitStats();
            assertNotNull(commitStats);
            assertThat(commitStats.getGeneration(), greaterThan(0L));
            assertThat(commitStats.getId(), notNullValue());
            assertThat(commitStats.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
        }
    }

    public void testRefreshListeners() throws Exception {
        // Create an index without automatic refreshes
        createIndex("test", Settings.builder().put("refresh_interval", -1).build());

        // Index a document asynchronously so the request will only return when document is refreshed
        ActionFuture<DocWriteResponse> index = prepareIndex("test").setId("test")
            .setSource("test", "test")
            .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
            .execute();

        // Wait for the refresh listener to appear in the stats. Wait a long time because NFS tests can be quite slow!
        logger.info("starting to wait");
        long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (true) {
            IndicesStatsResponse stats = indicesAdmin().prepareStats("test").clear().setRefresh(true).setDocs(true).get();
            CommonStats common = stats.getIndices().get("test").getTotal();
            // There shouldn't be a doc. If there is then we did *something* weird.
            assertEquals(0, common.docs.getCount());
            if (1 == common.refresh.getListeners()) {
                break;
            }
            if (end - System.nanoTime() < 0) {
                logger.info("timed out");
                fail("didn't get a refresh listener in time: " + Strings.toString(common));
            }
        }

        // Refresh the index and wait for the request to come back
        indicesAdmin().prepareRefresh("test").get();
        index.get();

        // The document should appear in the statistics and the refresh listener should be gone
        IndicesStatsResponse stats = indicesAdmin().prepareStats("test").clear().setRefresh(true).setDocs(true).get();
        CommonStats common = stats.getIndices().get("test").getTotal();
        assertEquals(1, common.docs.getCount());
        assertEquals(0, common.refresh.getListeners());
    }

    public void testUuidOnRootStatsIndices() {
        String uuid = createIndex("test").indexUUID();
        IndicesStatsResponse rsp = indicesAdmin().prepareStats().get();
        assertEquals(uuid, rsp.getIndex("test").getUuid());
    }

    public void testIndexHealth() {
        String greenIndex = "green-index";
        createIndex(greenIndex);
        String yellowIndex = "yellow-index";
        createIndex(yellowIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());

        IndicesStatsResponse rsp = indicesAdmin().prepareStats().all().get();

        IndexStats greenIndexStats = rsp.getIndex(greenIndex);
        assertEquals(ClusterHealthStatus.GREEN, greenIndexStats.getHealth());

        IndexStats yellowIndexStats = rsp.getIndex(yellowIndex);
        assertEquals(ClusterHealthStatus.YELLOW, yellowIndexStats.getHealth());
    }

    public void testIndexState() throws ExecutionException, InterruptedException {
        String openIndex = "open-index";
        createIndex(openIndex);
        String closeIndex = "close-index";
        createIndex(closeIndex);

        indicesAdmin().close(new CloseIndexRequest(closeIndex)).get();

        IndicesStatsResponse rsp = indicesAdmin().prepareStats().setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_CLOSED).get();
        IndexStats openIndexStats = rsp.getIndex(openIndex);
        assertEquals(IndexMetadata.State.OPEN, openIndexStats.getState());

        IndexStats closeIndexStats = rsp.getIndex(closeIndex);
        assertEquals(IndexMetadata.State.CLOSE, closeIndexStats.getState());
    }

    /**
     * Gives access to package private IndicesStatsResponse constructor for test purpose.
     **/
    public static IndicesStatsResponse newIndicesStatsResponse(
        ShardStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new IndicesStatsResponse(
            shards,
            totalShards,
            successfulShards,
            failedShards,
            shardFailures,
            clusterState.getMetadata(),
            clusterState.routingTable()
        );
    }
}

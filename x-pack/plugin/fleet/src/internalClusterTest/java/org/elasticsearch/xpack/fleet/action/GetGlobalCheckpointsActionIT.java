/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.fleet.Fleet;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GetGlobalCheckpointsActionIT extends ESIntegTestCase {

    public static final TimeValue TEN_SECONDS = TimeValue.timeValueSeconds(10);
    public static final long[] EMPTY_ARRAY = new long[0];

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(Fleet.class, LocalStateCompositeXPackPlugin.class, IndexLifecycle.class).collect(Collectors.toList());
    }

    public void testGetGlobalCheckpoints() throws Exception {
        int shards = between(1, 5);
        String indexName = "test_index";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(
                indexSettings(shards, 1)
                    // ESIntegTestCase randomizes durability settings. The global checkpoint only advances after a fsync, hence we
                    // must run with REQUEST durability
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
            )
            .get();

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            false,
            false,
            EMPTY_ARRAY,
            TimeValue.parseTimeValue(randomTimeValue(), "test")
        );
        final GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).get();
        long[] expected = new long[shards];
        Arrays.fill(expected, -1);
        assertArrayEquals(expected, response.globalCheckpoints());

        final int totalDocuments = shards * 3;
        for (int i = 0; i < totalDocuments; ++i) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final GetGlobalCheckpointsAction.Request request2 = new GetGlobalCheckpointsAction.Request(
            indexName,
            false,
            false,
            EMPTY_ARRAY,
            TimeValue.parseTimeValue(randomTimeValue(), "test")
        );
        final GetGlobalCheckpointsAction.Response response2 = client().execute(GetGlobalCheckpointsAction.INSTANCE, request2).get();

        assertEquals(totalDocuments, Arrays.stream(response2.globalCheckpoints()).map(s -> s + 1).sum());

        indicesAdmin().prepareRefresh(indexName).get();

        final IndicesStatsResponse statsResponse = indicesAdmin().prepareStats(indexName).get();
        long[] fromStats = Arrays.stream(statsResponse.getShards())
            .filter(i -> i.getShardRouting().primary())
            .sorted(Comparator.comparingInt(value -> value.getShardRouting().id()))
            .mapToLong(s -> s.getSeqNoStats().getGlobalCheckpoint())
            .toArray();
        assertArrayEquals(fromStats, response2.globalCheckpoints());
    }

    public void testPollGlobalCheckpointAdvancement() throws Exception {
        String indexName = "test_index";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(1, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            false,
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        final GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).get();
        assertEquals(-1, response.globalCheckpoints()[0]);

        final int totalDocuments = between(25, 50);
        new Thread(() -> {
            for (int i = 0; i < totalDocuments; ++i) {
                client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).execute();
            }
        }).start();

        final GetGlobalCheckpointsAction.Request request2 = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            false,
            new long[] { totalDocuments - 2 },
            TimeValue.timeValueSeconds(30)
        );
        long start = System.nanoTime();
        final GetGlobalCheckpointsAction.Response response2 = client().execute(GetGlobalCheckpointsAction.INSTANCE, request2).get();
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();

        assertThat(elapsed, lessThan(30L));
        assertFalse(response.timedOut());
        assertEquals(totalDocuments - 1, response2.globalCheckpoints()[0]);

    }

    public void testPollGlobalCheckpointAdvancementTimeout() {
        String indexName = "test_index";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();

        final int totalDocuments = 30;
        for (int i = 0; i < totalDocuments; ++i) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            false,
            new long[] { 29 },
            TimeValue.timeValueMillis(between(1, 100))
        );
        long start = System.nanoTime();
        GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet();
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();
        assertThat(elapsed, lessThan(30L));
        assertTrue(response.timedOut());
        assertEquals(29L, response.globalCheckpoints()[0]);
    }

    public void testMustProvideCorrectNumberOfShards() {
        String indexName = "test_index";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();

        final long[] incorrectArrayLength = new long[2];
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            false,
            incorrectArrayLength,
            TEN_SECONDS
        );
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            equalTo("number of checkpoints must equal number of shards. [shard count: 1, checkpoint count: 2]")
        );
    }

    public void testWaitForAdvanceOnlySupportsOneShard() {
        String indexName = "test_index";
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(3, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            false,
            new long[3],
            TEN_SECONDS
        );
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), equalTo("wait_for_advance only supports indices with one shard. [shard count: 3]"));
    }

    public void testIndexDoesNotExistNoWait() {
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            "non-existent",
            false,
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );

        long start = System.nanoTime();
        ElasticsearchException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();
        assertThat(elapsed, lessThanOrEqualTo(TEN_SECONDS.seconds()));
    }

    public void testWaitOnIndexTimeout() {
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            "non-existent",
            true,
            true,
            EMPTY_ARRAY,
            TimeValue.timeValueMillis(between(1, 100))
        );
        ElasticsearchException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
    }

    public void testWaitOnIndexCreated() throws Exception {
        String indexName = "not-yet-existing";
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            true,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        long start = System.nanoTime();
        ActionFuture<GetGlobalCheckpointsAction.Response> future = client().execute(GetGlobalCheckpointsAction.INSTANCE, request);
        Thread.sleep(randomIntBetween(10, 100));
        indicesAdmin().prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();
        client().prepareIndex(indexName).setId(Integer.toString(0)).setSource("{}", XContentType.JSON).get();

        GetGlobalCheckpointsAction.Response response = future.actionGet();
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();
        assertThat(elapsed, lessThanOrEqualTo(TEN_SECONDS.seconds()));
        assertThat(response.globalCheckpoints()[0], equalTo(0L));
        assertFalse(response.timedOut());
    }

    public void testPrimaryShardsNotReadyNoWait() {
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            "not-assigned",
            false,
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        indicesAdmin().prepareCreate("not-assigned")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "node", "none")
            )
            .get();

        UnavailableShardsException exception = expectThrows(
            UnavailableShardsException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
        assertEquals("Primary shards were not active [shards=1, active=0]", exception.getMessage());
    }

    public void testWaitOnPrimaryShardsReadyTimeout() {
        TimeValue timeout = TimeValue.timeValueMillis(between(1, 100));
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            "not-assigned",
            true,
            true,
            EMPTY_ARRAY,
            timeout
        );
        indicesAdmin().prepareCreate("not-assigned")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "node", "none")
            )
            .get();

        UnavailableShardsException exception = expectThrows(
            UnavailableShardsException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
        assertEquals("Primary shards were not active within timeout [timeout=" + timeout + ", shards=1, active=0]", exception.getMessage());
    }

    public void testWaitOnPrimaryShardsReady() throws Exception {
        String indexName = "not-assigned";
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            true,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        indicesAdmin().prepareCreate(indexName)
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "node", "none")
            )
            .get();

        long start = System.nanoTime();
        ActionFuture<GetGlobalCheckpointsAction.Response> future = client().execute(GetGlobalCheckpointsAction.INSTANCE, request);
        Thread.sleep(randomIntBetween(10, 100));
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "node", ""), indexName);
        client().prepareIndex(indexName).setId(Integer.toString(0)).setSource("{}", XContentType.JSON).get();

        GetGlobalCheckpointsAction.Response response = future.actionGet();
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();
        assertThat(elapsed, lessThanOrEqualTo(TEN_SECONDS.seconds()));
        assertThat(response.globalCheckpoints()[0], equalTo(0L));
        assertFalse(response.timedOut());
    }

    public void testWaitOnPrimaryShardThrottled() throws Exception {
        updateClusterSettings(Settings.builder().put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 0));

        String indexName = "throttled";
        indicesAdmin().prepareCreate(indexName)
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
            .get();

        long start = System.nanoTime();
        var future = client().execute(
            GetGlobalCheckpointsAction.INSTANCE,
            new GetGlobalCheckpointsAction.Request(indexName, true, true, EMPTY_ARRAY, TEN_SECONDS)
        );
        Thread.sleep(randomIntBetween(10, 100));

        updateClusterSettings(Settings.builder().putNull(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey()));
        client().prepareIndex(indexName).setId(Integer.toString(0)).setSource("{}", XContentType.JSON).get();

        var response = future.actionGet();
        long elapsed = TimeValue.timeValueNanos(System.nanoTime() - start).seconds();
        assertThat(elapsed, lessThanOrEqualTo(TEN_SECONDS.seconds()));
        assertThat(response.globalCheckpoints()[0], equalTo(0L));
        assertFalse(response.timedOut());
    }
}

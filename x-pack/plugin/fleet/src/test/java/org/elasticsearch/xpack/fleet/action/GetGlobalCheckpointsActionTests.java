/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.fleet.Fleet;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class GetGlobalCheckpointsActionTests extends ESIntegTestCase {

    public static final TimeValue TEN_SECONDS = TimeValue.timeValueSeconds(10);
    public static final long[] EMPTY_ARRAY = new long[0];

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.of(Fleet.class).collect(Collectors.toList());
    }

    public void testGetGlobalCheckpoints() throws Exception {
        int shards = randomInt(4) + 1;
        String indexName = "test_index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put("index.number_of_shards", shards)
                    .put("index.number_of_replicas", 0)
            )
            .get();

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        final GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).get();
        long[] expected = new long[shards];
        for (int i = 0; i < shards; ++i) {
            expected[i] = -1;
        }
        assertArrayEquals(expected, response.globalCheckpoints());

        final int totalDocuments = shards * 3;
        for (int i = 0; i < totalDocuments; ++i) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        assertBusy(() -> {
            final GetGlobalCheckpointsAction.Request request2 = new GetGlobalCheckpointsAction.Request(
                indexName,
                false,
                EMPTY_ARRAY,
                TEN_SECONDS
            );
            final GetGlobalCheckpointsAction.Response response2 = client().execute(GetGlobalCheckpointsAction.INSTANCE, request2).get();

            assertEquals(totalDocuments, Arrays.stream(response2.globalCheckpoints()).map(s -> s + 1).sum());

            final IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).get();
            long[] fromStats = Arrays.stream(statsResponse.getShards())
                .filter(i -> i.getShardRouting().primary())
                .sorted(Comparator.comparingInt(value -> value.getShardRouting().id()))
                .mapToLong(s -> s.getSeqNoStats().getGlobalCheckpoint())
                .toArray();
            assertArrayEquals(fromStats, response2.globalCheckpoints());
        });
    }

    public void testPollGlobalCheckpointAdvancement() throws Exception {
        String indexName = "test_index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            )
            .get();

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        final GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).get();
        assertEquals(-1, response.globalCheckpoints()[0]);

        final int totalDocuments = 30;
        for (int i = 0; i < totalDocuments; ++i) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).execute();
        }

        final GetGlobalCheckpointsAction.Request request2 = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            new long[] { 28 },
            TEN_SECONDS
        );
        final GetGlobalCheckpointsAction.Response response2 = client().execute(GetGlobalCheckpointsAction.INSTANCE, request2).get();
        assertEquals(29L, response2.globalCheckpoints()[0]);
    }

    public void testPollGlobalCheckpointAdvancementTimeout() throws Exception {
        String indexName = "test_index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
            )
            .get();

        final int totalDocuments = 30;
        for (int i = 0; i < totalDocuments; ++i) {
            client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("{}", XContentType.JSON).execute();
        }

        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
            new long[] { 29 },
            TimeValue.timeValueMillis(100)
        );
        GetGlobalCheckpointsAction.Response response = client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet();
        assertTrue(response.timedOut());
    }

    public void testMustProvideCorrectNumberOfShards() throws Exception {
        String indexName = "test_index";
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                Settings.builder()
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
            )
            .get();

        final long[] incorrectArrayLength = new long[2];
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            indexName,
            true,
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
            equalTo("current_checkpoints must equal number of shards. [shard count: 3, current_checkpoints: 2]")
        );
    }

    public void testIndexDoesNotExist() throws Exception {
        final GetGlobalCheckpointsAction.Request request = new GetGlobalCheckpointsAction.Request(
            "non-existent",
            false,
            EMPTY_ARRAY,
            TEN_SECONDS
        );
        ElasticsearchException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(GetGlobalCheckpointsAction.INSTANCE, request).actionGet()
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

/**
 * Test suite for checkpointing using transform getcheckpoint API
 */
public class TransformGetCheckpointIT extends TransformSingleNodeTestCase {

    public void testGetCheckpoint() throws Exception {
        final String indexNamePrefix = "test_index-";
        final int shards = randomIntBetween(1, 5);
        var indices = indices(indexNamePrefix, randomIntBetween(1, 5));

        for (var index : indices) {
            indicesAdmin().prepareCreate(index).setSettings(indexSettings(shards, 1)).get();
        }

        final GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { indexNamePrefix + "*" },
            IndicesOptions.LENIENT_EXPAND_OPEN,
            null,
            null,
            TimeValue.timeValueSeconds(5)
        );

        final GetCheckpointAction.Response response = client().execute(GetCheckpointAction.INSTANCE, request).get();
        assertEquals(indices.size(), response.getCheckpoints().size());

        // empty indices should report -1 as sequence id
        assertFalse(
            response.getCheckpoints().entrySet().stream().anyMatch(entry -> Arrays.stream(entry.getValue()).anyMatch(l -> l != -1L))
        );

        final int docsToCreatePerShard = randomIntBetween(0, 10);
        for (int d = 0; d < docsToCreatePerShard; ++d) {
            for (var index : indices) {
                for (int j = 0; j < shards; ++j) {
                    prepareIndex(index).setSource("{" + "\"field\":" + j + "}", XContentType.JSON).get();
                }
            }
        }

        indicesAdmin().refresh(new RefreshRequest(indexNamePrefix + "*"));

        final GetCheckpointAction.Response response2 = client().execute(GetCheckpointAction.INSTANCE, request).get();
        assertEquals(indices.size(), response2.getCheckpoints().size());

        // check the sum, counting starts with 0, so we have to take docsToCreatePerShard - 1
        long checkpointSum = response2.getCheckpoints().values().stream().map(l -> Arrays.stream(l).sum()).mapToLong(Long::valueOf).sum();
        assertEquals(
            "Expected "
                + (docsToCreatePerShard - 1) * shards * indices.size()
                + " as sum of "
                + response2.getCheckpoints()
                    .entrySet()
                    .stream()
                    .map(e -> e.getKey() + ": {" + Strings.arrayToCommaDelimitedString(Arrays.stream(e.getValue()).boxed().toArray()) + "}")
                    .collect(Collectors.joining(",")),
            (docsToCreatePerShard - 1) * shards * indices.size(),
            checkpointSum
        );

        final IndicesStatsResponse statsResponse = indicesAdmin().prepareStats(indexNamePrefix + "*").get();

        assertEquals(
            "Checkpoint API and indices stats don't match",
            Arrays.stream(statsResponse.getShards())
                .filter(i -> i.getShardRouting().primary())
                .sorted(Comparator.comparingInt(value -> value.getShardRouting().id()))
                .mapToLong(s -> s.getSeqNoStats().getGlobalCheckpoint())
                .filter(Objects::nonNull)
                .sum(),
            checkpointSum
        );
        deleteIndices(indices);
    }

    public void testGetCheckpointWithQueryThatFiltersOutEverything() throws Exception {
        final String indexNamePrefix = "test_index-";
        var indices = indices(indexNamePrefix, randomIntBetween(1, 5));
        final int shards = randomIntBetween(1, 5);
        final int docsToCreatePerShard = randomIntBetween(0, 10);

        for (int i = 0; i < indices.size(); ++i) {
            var index = indices.get(i);
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(shards, 1))
                .setMapping("field", "type=long", "@timestamp", "type=date")
                .get();
            for (int j = 0; j < shards; ++j) {
                for (int d = 0; d < docsToCreatePerShard; ++d) {
                    client().prepareIndex(index)
                        .setSource(Strings.format("{ \"field\":%d, \"@timestamp\": %d }", j, 10_000_000 + d + i + j), XContentType.JSON)
                        .get();
                }
            }
        }
        indicesAdmin().refresh(new RefreshRequest(indexNamePrefix + "*"));

        final GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { indexNamePrefix + "*" },
            IndicesOptions.LENIENT_EXPAND_OPEN,
            // This query does not match any documents
            QueryBuilders.rangeQuery("@timestamp").gte(20_000_000),
            null,
            TimeValue.timeValueSeconds(5)
        );

        final GetCheckpointAction.Response response = client().execute(GetCheckpointAction.INSTANCE, request).get();
        assertThat("Response was: " + response.getCheckpoints(), response.getCheckpoints(), is(anEmptyMap()));
        deleteIndices(indices);
    }

    public void testGetCheckpointWithMissingIndex() throws Exception {
        GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { "test_index_missing" },
            IndicesOptions.LENIENT_EXPAND_OPEN,
            null,
            null,
            TimeValue.timeValueSeconds(5)
        );

        GetCheckpointAction.Response response = client().execute(GetCheckpointAction.INSTANCE, request).get();
        assertThat("Response was: " + response.getCheckpoints(), response.getCheckpoints(), is(anEmptyMap()));

        request = new GetCheckpointAction.Request(
            new String[] { "test_index_missing-*" },
            IndicesOptions.LENIENT_EXPAND_OPEN,
            null,
            null,
            TimeValue.timeValueSeconds(5)
        );

        response = client().execute(GetCheckpointAction.INSTANCE, request).get();
        assertThat("Response was: " + response.getCheckpoints(), response.getCheckpoints(), is(anEmptyMap()));
    }

    public void testGetCheckpointTimeoutExceeded() throws Exception {
        final String indexNamePrefix = "test_index-";
        var indices = indices(indexNamePrefix, 100);
        final int shards = 5;

        for (var index : indices) {
            indicesAdmin().prepareCreate(index).setSettings(indexSettings(shards, 0)).get();
        }

        final GetCheckpointAction.Request request = new GetCheckpointAction.Request(
            new String[] { indexNamePrefix + "*" },
            IndicesOptions.LENIENT_EXPAND_OPEN,
            null,
            null,
            TimeValue.ZERO
        );

        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<Exception> finalException = new SetOnce<>();
        client().execute(GetCheckpointAction.INSTANCE, request, ActionListener.wrap(r -> latch.countDown(), e -> {
            finalException.set(e);
            latch.countDown();
        }));
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        Exception e = finalException.get();
        if (e != null) {
            assertThat(e, is(instanceOf(ElasticsearchTimeoutException.class)));
            assertThat(
                "Message was: " + e.getMessage(),
                e.getMessage(),
                startsWith("Transform checkpointing timed out on node [node_s_0] after [0ms]")
            );
        } else {
            // Due to system clock usage, the timeout does not always occur where it should.
            // We cannot mock the clock so we just have to live with it.
        }
        deleteIndices(indices);
    }

    private List<String> indices(String prefix, int numberOfIndices) {
        return IntStream.range(0, numberOfIndices).mapToObj(i -> prefix + i).toList();
    }

    private void deleteIndices(List<String> indices) {
        try {
            indicesAdmin().prepareDelete(indices.toArray(new String[0])).get();
        } catch (Exception e) {
            // we can fail to clean up the indices, but this wouldn't impact other tests since the node gets torn down anyway
            // the index delete is to help the node tear down go smoother
        }
    }
}

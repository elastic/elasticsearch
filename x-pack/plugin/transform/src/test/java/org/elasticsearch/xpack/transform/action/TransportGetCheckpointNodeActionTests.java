/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction;
import org.elasticsearch.xpack.transform.transforms.scheduling.FakeClock;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetCheckpointNodeActionTests extends ESTestCase {

    private static final String NODE_NAME = "dummy-node";

    private IndicesService indicesService;
    private CancellableTask task;
    private FakeClock clock;
    private Set<ShardId> shards;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = new ClusterService(
            Settings.builder().put("node.name", NODE_NAME).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null,
            (TaskManager) null
        );
        IndexShard indexShardA0 = mock(IndexShard.class);
        when(indexShardA0.seqNoStats()).thenReturn(new SeqNoStats(3_000, 2_000, 3_000));
        IndexShard indexShardA1 = mock(IndexShard.class);
        when(indexShardA1.seqNoStats()).thenReturn(new SeqNoStats(3_000, 2_000, 3_001));
        IndexShard indexShardB0 = mock(IndexShard.class);
        when(indexShardB0.seqNoStats()).thenReturn(new SeqNoStats(3_000, 2_000, 4_000));
        IndexShard indexShardB1 = mock(IndexShard.class);
        when(indexShardB1.seqNoStats()).thenReturn(new SeqNoStats(3_000, 2_000, 4_001));
        Settings commonIndexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, 1_000_000)
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexService indexServiceA = mock(IndexService.class);
        when(indexServiceA.getIndexSettings()).thenReturn(
            new IndexSettings(IndexMetadata.builder("my-index-A").settings(commonIndexSettings).build(), Settings.EMPTY)
        );
        when(indexServiceA.getShard(0)).thenReturn(indexShardA0);
        when(indexServiceA.getShard(1)).thenReturn(indexShardA1);
        IndexService indexServiceB = mock(IndexService.class);
        when(indexServiceB.getIndexSettings()).thenReturn(
            new IndexSettings(IndexMetadata.builder("my-index-B").settings(commonIndexSettings).build(), Settings.EMPTY)
        );
        when(indexServiceB.getShard(0)).thenReturn(indexShardB0);
        when(indexServiceB.getShard(1)).thenReturn(indexShardB1);
        indicesService = mock(IndicesService.class);
        when(indicesService.clusterService()).thenReturn(clusterService);
        when(indicesService.indexServiceSafe(new Index("my-index-A", "A"))).thenReturn(indexServiceA);
        when(indicesService.indexServiceSafe(new Index("my-index-B", "B"))).thenReturn(indexServiceB);

        task = new CancellableTask(123, "type", "action", "description", new TaskId("dummy-node:456"), Map.of());
        clock = new FakeClock(Instant.now());
        shards = Set.of(
            new ShardId(new Index("my-index-A", "A"), 0),
            new ShardId(new Index("my-index-A", "A"), 1),
            new ShardId(new Index("my-index-B", "B"), 0),
            new ShardId(new Index("my-index-B", "B"), 1)
        );
    }

    public void testGetGlobalCheckpointsWithNoTimeout() throws InterruptedException {
        testGetGlobalCheckpointsSuccess(null);
    }

    public void testGetGlobalCheckpointsWithHighTimeout() throws InterruptedException {
        testGetGlobalCheckpointsSuccess(TimeValue.timeValueMinutes(1));
    }

    private void testGetGlobalCheckpointsSuccess(TimeValue timeout) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<GetCheckpointNodeAction.Response> responseHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        TransportGetCheckpointNodeAction.getGlobalCheckpoints(indicesService, task, shards, timeout, clock, ActionListener.wrap(r -> {
            responseHolder.set(r);
            latch.countDown();
        }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));
        latch.await(10, TimeUnit.SECONDS);

        Map<String, long[]> checkpoints = responseHolder.get().getCheckpoints();
        assertThat(checkpoints.keySet(), containsInAnyOrder("my-index-A", "my-index-B"));
        assertThat(LongStream.of(checkpoints.get("my-index-A")).boxed().collect(Collectors.toList()), contains(3000L, 3001L));
        assertThat(LongStream.of(checkpoints.get("my-index-B")).boxed().collect(Collectors.toList()), contains(4000L, 4001L));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    public void testGetGlobalCheckpointsFailureDueToTaskCancelled() throws InterruptedException {
        TaskCancelHelper.cancel(task, "due to apocalypse");

        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<GetCheckpointNodeAction.Response> responseHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        TransportGetCheckpointNodeAction.getGlobalCheckpoints(indicesService, task, shards, null, clock, ActionListener.wrap(r -> {
            responseHolder.set(r);
            latch.countDown();
        }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));
        latch.await(10, TimeUnit.SECONDS);

        assertThat("Response was: " + responseHolder.get(), responseHolder.get(), is(nullValue()));
        assertThat(exceptionHolder.get().getMessage(), is(equalTo("task cancelled [due to apocalypse]")));
    }

    public void testGetGlobalCheckpointsFailureDueToTimeout() throws InterruptedException {
        // Move the current time past the timeout.
        clock.advanceTimeBy(Duration.ofSeconds(10));

        CountDownLatch latch = new CountDownLatch(1);
        SetOnce<GetCheckpointNodeAction.Response> responseHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        TransportGetCheckpointNodeAction.getGlobalCheckpoints(
            indicesService,
            task,
            shards,
            TimeValue.timeValueSeconds(5),
            clock,
            ActionListener.wrap(r -> {
                responseHolder.set(r);
                latch.countDown();
            }, e -> {
                exceptionHolder.set(e);
                latch.countDown();
            })
        );
        latch.await(10, TimeUnit.SECONDS);

        assertThat("Response was: " + responseHolder.get(), responseHolder.get(), is(nullValue()));
        assertThat(
            exceptionHolder.get().getMessage(),
            is(equalTo("Transform checkpointing timed out on node [dummy-node] after [5s] having processed [0] of [4] shards"))
        );
    }
}

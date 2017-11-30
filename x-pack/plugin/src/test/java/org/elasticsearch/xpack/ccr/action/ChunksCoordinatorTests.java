/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ccr.action.ShardFollowTasksExecutor.ChunksCoordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ChunksCoordinatorTests extends ESTestCase {

    public void testCreateChunks() {
        Client client = mock(Client.class);
        Executor ccrExecutor = mock(Executor.class);
        ShardId leaderShardId = new ShardId("index1", "index1", 0);
        ShardId followShardId = new ShardId("index2", "index1", 0);

        ChunksCoordinator coordinator = new ChunksCoordinator(client, ccrExecutor, 1024, leaderShardId, followShardId, e -> {});
        coordinator.createChucks(0, 1024);
        List<long[]> result = new ArrayList<>(coordinator.getChunks());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1024L));

        coordinator.getChunks().clear();
        coordinator.createChucks(0, 2048);
        result = new ArrayList<>(coordinator.getChunks());
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1024L));
        assertThat(result.get(1)[0], equalTo(1025L));
        assertThat(result.get(1)[1], equalTo(2048L));

        coordinator.getChunks().clear();
        coordinator.createChucks(0, 4096);
        result = new ArrayList<>(coordinator.getChunks());
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0)[0], equalTo(0L));
        assertThat(result.get(0)[1], equalTo(1024L));
        assertThat(result.get(1)[0], equalTo(1025L));
        assertThat(result.get(1)[1], equalTo(2048L));
        assertThat(result.get(2)[0], equalTo(2049L));
        assertThat(result.get(2)[1], equalTo(3072L));
        assertThat(result.get(3)[0], equalTo(3073L));
        assertThat(result.get(3)[1], equalTo(4096L));

        coordinator.getChunks().clear();
        coordinator.createChucks(4096, 8196);
        result = new ArrayList<>(coordinator.getChunks());
        assertThat(result.size(), equalTo(5));
        assertThat(result.get(0)[0], equalTo(4096L));
        assertThat(result.get(0)[1], equalTo(5120L));
        assertThat(result.get(1)[0], equalTo(5121L));
        assertThat(result.get(1)[1], equalTo(6144L));
        assertThat(result.get(2)[0], equalTo(6145L));
        assertThat(result.get(2)[1], equalTo(7168L));
        assertThat(result.get(3)[0], equalTo(7169L));
        assertThat(result.get(3)[1], equalTo(8192L));
        assertThat(result.get(4)[0], equalTo(8193L));
        assertThat(result.get(4)[1], equalTo(8196L));
    }

}

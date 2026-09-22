/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.List;

import static org.elasticsearch.xpack.stateless.StatelessPlugin.PREWARM_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.StatelessPlugin.SHARD_READ_THREAD_POOL;
import static org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor.MAX_CONCURRENCY_SETTING;
import static org.elasticsearch.xpack.stateless.commits.BCCHeaderReadExecutor.maxConcurrency;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class BCCHeaderReadExecutorTests extends ESTestCase {

    public void testDerivesLimitFromTheLargerPool() {
        // enough processors for the prewarm pool to exceed the shard read pool
        assertThat(maxConcurrency(-1, 32, 10), equalTo(32));
        // small enough that the shard read pool is the larger of the two
        assertThat(maxConcurrency(-1, 2, 4), equalTo(4));
        assertThat(maxConcurrency(-1, 10, 10), equalTo(10));
    }

    public void testNeverDropsBelowTheShardReadPoolSize() {
        int shardReadMaxThreads = randomIntBetween(1, 28);
        int prewarmMaxThreads = randomIntBetween(1, 32);
        assertThat(maxConcurrency(-1, prewarmMaxThreads, shardReadMaxThreads), greaterThanOrEqualTo(shardReadMaxThreads));
    }

    public void testOverrideWins() {
        assertThat(maxConcurrency(1, 32, 10), equalTo(1));
        assertThat(maxConcurrency(64, 32, 10), equalTo(64));
    }

    public void testNonPositiveOverrideMeansDerive() {
        // -1 is the documented "unset" value; 0 would be rejected by the task runner, so it must not be passed through
        assertThat(maxConcurrency(-1, 32, 10), equalTo(32));
        assertThat(maxConcurrency(0, 32, 10), equalTo(32));
    }

    public void testReadsTheOverrideFromNodeSettings() {
        final var nodeSettings = Settings.builder()
            .put(StatelessPlugin.STATELESS_ENABLED.getKey(), true)
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), List.of(DiscoveryNodeRole.INDEX_ROLE.roleName()))
            .build();
        final var executorBuilders = new StatelessPlugin(nodeSettings).getExecutorBuilders(nodeSettings).toArray(new ExecutorBuilder<?>[0]);

        final var threadPool = new TestThreadPool(getTestName(), nodeSettings, executorBuilders);
        try {
            assertThat(MAX_CONCURRENCY_SETTING.get(nodeSettings), equalTo(-1));
            assertThat(
                maxConcurrency(nodeSettings, threadPool),
                equalTo(Math.max(threadPool.info(PREWARM_THREAD_POOL).getMax(), threadPool.info(SHARD_READ_THREAD_POOL).getMax()))
            );

            final int override = randomIntBetween(1, 64);
            final var overridden = Settings.builder().put(nodeSettings).put(MAX_CONCURRENCY_SETTING.getKey(), override).build();
            assertThat(maxConcurrency(overridden, threadPool), equalTo(override));
        } finally {
            terminate(threadPool);
        }
    }
}

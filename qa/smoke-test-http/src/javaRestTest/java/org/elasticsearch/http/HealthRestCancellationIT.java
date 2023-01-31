/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class HealthRestCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), BlockingHealthPlugin.class);
    }

    public void testHealthRestCancellation() throws Exception {
        runTest(new Request(HttpGet.METHOD_NAME, "/_health_report"));
    }

    private void runTest(Request request) throws Exception {
        List<Semaphore> blocks = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            for (BlockingHealthPlugin plugin : pluginsService.filterPlugins(BlockingHealthPlugin.class)) {
                blocks.add(plugin.operationBlock);
            }
        }

        assertThat(blocks, not(empty()));
        final List<Releasable> releasables = new ArrayList<>();
        try {
            for (final Semaphore operationBlock : blocks) {
                operationBlock.acquire();
                releasables.add(operationBlock::release);
            }

            final PlainActionFuture<Response> future = new PlainActionFuture<>();
            logger.info("--> sending request");
            final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

            awaitTaskWithPrefix(GetHealthAction.NAME);

            logger.info("--> waiting for at least one task to hit a block");
            assertBusy(() -> assertTrue(blocks.stream().anyMatch(Semaphore::hasQueuedThreads)));

            logger.info("--> cancelling request");
            cancellable.cancel();
            expectThrows(CancellationException.class, future::actionGet);

            assertAllCancellableTasksAreCancelled(GetHealthAction.NAME);
        } finally {
            Releasables.close(releasables);
        }

        assertAllTasksHaveFinished(GetHealthAction.NAME);
    }

    public static class BlockingHealthPlugin extends Plugin implements HealthPlugin {

        final Semaphore operationBlock = new Semaphore(1);

        @Override
        public Collection<HealthIndicatorService> getHealthIndicatorServices() {
            return List.of(new BlockingHealthIndicator(operationBlock));
        }
    }

    private static class BlockingHealthIndicator implements HealthIndicatorService {

        private final Semaphore operationBlock;

        BlockingHealthIndicator(Semaphore operationBlock) {
            this.operationBlock = operationBlock;
        }

        @Override
        public String name() {
            return BlockingHealthPlugin.class.getSimpleName();
        }

        @Override
        public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
            try {
                operationBlock.acquire();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            operationBlock.release();

            return new HealthIndicatorResult(
                name(),
                HealthStatus.GREEN,
                "testing",
                SimpleHealthIndicatorDetails.EMPTY,
                List.of(),
                List.of()
            );
        }
    }
}

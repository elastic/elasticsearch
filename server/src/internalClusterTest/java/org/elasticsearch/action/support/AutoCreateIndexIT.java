/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

public class AutoCreateIndexIT extends ESIntegTestCase {
    public void testBatchingWithDeprecationWarnings() throws Exception {
        final var masterNodeClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var barrier = new CyclicBarrier(2);
        masterNodeClusterService.createTaskQueue("block", Priority.NORMAL, batchExecutionContext -> {
            barrier.await(10, TimeUnit.SECONDS);
            barrier.await(10, TimeUnit.SECONDS);
            batchExecutionContext.taskContexts().forEach(c -> c.success(() -> {}));
            return batchExecutionContext.initialState();
        }).submitTask("block", e -> { assert false : e; }, null);

        barrier.await(10, TimeUnit.SECONDS);

        final var countDownLatch = new CountDownLatch(2);

        final var client = client();
        client.prepareIndex("no-dot").setSource("{}", XContentType.JSON).execute(new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                try {
                    final var warningHeaders = client.threadPool().getThreadContext().getResponseHeaders().get("Warning");
                    if (warningHeaders != null) {
                        assertThat(
                            warningHeaders,
                            not(
                                hasItems(
                                    containsString("index names starting with a dot are reserved for hidden indices and system indices")
                                )
                            )
                        );
                    }
                } finally {
                    countDownLatch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                countDownLatch.countDown();
                assert false : e;
            }
        });

        client.prepareIndex(".has-dot").setSource("{}", XContentType.JSON).execute(new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                try {
                    final var warningHeaders = client.threadPool().getThreadContext().getResponseHeaders().get("Warning");
                    assertNotNull(warningHeaders);
                    assertThat(
                        warningHeaders,
                        hasItems(containsString("index names starting with a dot are reserved for hidden indices and system indices"))
                    );
                } finally {
                    countDownLatch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                countDownLatch.countDown();
                assert false : e;
            }
        });

        assertBusy(
            () -> assertThat(
                masterNodeClusterService.getMasterService()
                    .pendingTasks()
                    .stream()
                    .map(pendingClusterTask -> pendingClusterTask.getSource().string())
                    .toList(),
                hasItems("auto create [no-dot]", "auto create [.has-dot]")
            )
        );

        barrier.await(10, TimeUnit.SECONDS);
        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
    }
}

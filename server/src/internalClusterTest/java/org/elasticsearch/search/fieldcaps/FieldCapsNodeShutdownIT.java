/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fieldcaps;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.empty;

/**
 * Tests that graceful shutdown of a data node waits for a node-level field caps task that it runs for a coordinator on another node.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class FieldCapsNodeShutdownIT extends ESIntegTestCase {

    private static final String INDEX = "field-caps-shutdown";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FieldCapabilitiesIT.BlockingOnRewriteQueryPlugin.class);
    }

    @After
    public void unblockFieldCaps() {
        FieldCapabilitiesIT.BlockingOnRewriteQueryBuilder.unblockOnRewrite();
    }

    @TestLogging(
        reason = "verify that shutdown observed the node-level field caps task",
        value = "org.elasticsearch.node.ShutdownPrepareService:DEBUG"
    )
    public void testShutdownWaitsForNodeLevelFieldCaps() throws Exception {
        final String coordinator = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(1, 0).put("index.routing.allocation.include._name", dataNode)));
        ensureGreen(INDEX);

        FieldCapabilitiesIT.BlockingOnRewriteQueryBuilder.blockOnRewrite();
        final var request = new FieldCapabilitiesRequest();
        request.indices(INDEX);
        request.fields("*");
        request.indexFilter(new FieldCapabilitiesIT.BlockingOnRewriteQueryBuilder());
        final ActionFuture<FieldCapabilitiesResponse> fieldCaps = client(coordinator).execute(
            TransportFieldCapabilitiesAction.TYPE,
            request
        );

        final var taskManager = internalCluster().getInstance(TransportService.class, dataNode).getTaskManager();
        assertBusy(
            () -> assertTrue(
                taskManager.getTasks()
                    .values()
                    .stream()
                    .anyMatch(task -> TransportFieldCapabilitiesAction.ACTION_NODE_NAME.equals(task.getAction()))
            )
        );

        try (var mockLog = MockLog.capture(ShutdownPrepareService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "wait for the node-level field caps task",
                    ShutdownPrepareService.class.getCanonicalName(),
                    Level.DEBUG,
                    "waiting for *indices:data/read/field_caps[n]*"
                )
            );
            final PlainActionFuture<Void> shutdown = prepareForShutdownAsync(dataNode);
            mockLog.awaitAllExpectationsMatched();
            expectThrows(TimeoutException.class, () -> shutdown.get(1, TimeUnit.SECONDS));

            FieldCapabilitiesIT.BlockingOnRewriteQueryBuilder.unblockOnRewrite();
            final FieldCapabilitiesResponse response = safeGet(fieldCaps);
            assertThat(response.getFailures(), empty());
            assertThat(response.getIndices(), arrayContaining(INDEX));
            safeGet(shutdown);
        }
        internalCluster().stopNode(dataNode);
    }

    private PlainActionFuture<Void> prepareForShutdownAsync(String nodeName) {
        final var shutdownPrepareService = internalCluster().getInstance(ShutdownPrepareService.class, nodeName);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        new Thread(() -> {
            try {
                shutdownPrepareService.prepareForShutdown();
                future.onResponse(null);
            } catch (Exception e) {
                future.onFailure(e);
            }
        }, "prepare-for-shutdown-" + nodeName).start();
        return future;
    }
}

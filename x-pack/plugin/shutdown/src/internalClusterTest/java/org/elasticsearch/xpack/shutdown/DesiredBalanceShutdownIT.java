/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DesiredBalanceShutdownIT extends ESIntegTestCase {

    private static final String INDEX = "test-index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ShutdownPlugin.class);
    }

    public void testDesiredBalanceWithShutdown() throws Exception {

        final var oldNodeName = internalCluster().startNode();
        final var oldNodeId = internalCluster().getInstance(ClusterService.class, oldNodeName).localNode().getId();

        createIndex(
            INDEX,
            indexSettings(between(1, 5), 0).put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", oldNodeName).build()
        );
        ensureGreen(INDEX);

        internalCluster().restartNode(internalCluster().startNode(), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String newNodeName) {

                logger.info("--> excluding index from [{}] and concurrently starting replacement with [{}]", oldNodeName, newNodeName);

                final PlainActionFuture<AcknowledgedResponse> excludeFuture = new PlainActionFuture<>();
                client().admin()
                    .indices()
                    .prepareUpdateSettings(INDEX)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", oldNodeName)
                            .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name")
                    )
                    .execute(excludeFuture);

                assertAcked(
                    client().execute(
                        PutShutdownNodeAction.INSTANCE,
                        new PutShutdownNodeAction.Request(
                            oldNodeId,
                            SingleNodeShutdownMetadata.Type.REPLACE,
                            "test",
                            null,
                            newNodeName,
                            null
                        )
                    ).actionGet(10, TimeUnit.SECONDS)
                );

                excludeFuture.actionGet(10, TimeUnit.SECONDS);

                return Settings.EMPTY;
            }
        });

        logger.info("--> waiting for replacement to complete");

        assertBusy(() -> {
            final var getShutdownResponse = client().execute(GetShutdownStatusAction.INSTANCE, new GetShutdownStatusAction.Request())
                .actionGet(10, TimeUnit.SECONDS);
            assertTrue(
                Strings.toString(getShutdownResponse, true, true),
                getShutdownResponse.getShutdownStatuses()
                    .stream()
                    .allMatch(s -> s.overallStatus() == SingleNodeShutdownMetadata.Status.COMPLETE)
            );
        }, 120, TimeUnit.SECONDS);
    }
}

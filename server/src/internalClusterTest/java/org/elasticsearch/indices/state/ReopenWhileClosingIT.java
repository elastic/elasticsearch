/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.state;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Glob;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.metadata.MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ReopenWhileClosingIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public void testReopenDuringClose() throws Exception {
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(randomIntBetween(2, 3));
        final String indexName = "test";
        createIndexWithDocs(indexName, dataOnlyNodes);

        ensureYellowAndNoInitializingShards(indexName);

        final CountDownLatch block = new CountDownLatch(1);
        final Releasable releaseBlock = interceptVerifyShardBeforeCloseActions(indexName, block::countDown);

        ActionFuture<CloseIndexResponse> closeIndexResponse = indicesAdmin().prepareClose(indexName).execute();
        assertTrue("Waiting for index to have a closing blocked", block.await(60, TimeUnit.SECONDS));
        assertIndexIsBlocked(indexName);
        assertFalse(closeIndexResponse.isDone());

        assertAcked(indicesAdmin().prepareOpen(indexName));

        releaseBlock.close();
        assertFalse(closeIndexResponse.get().isAcknowledged());
        assertIndexIsOpened(indexName);
    }

    public void testReopenDuringCloseOnMultipleIndices() throws Exception {
        List<String> dataOnlyNodes = internalCluster().startDataOnlyNodes(randomIntBetween(2, 3));
        final List<String> indices = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            indices.add("index-" + i);
            createIndexWithDocs(indices.get(i), dataOnlyNodes);
        }

        ensureYellowAndNoInitializingShards(indices.toArray(Strings.EMPTY_ARRAY));

        final CountDownLatch block = new CountDownLatch(1);
        final Releasable releaseBlock = interceptVerifyShardBeforeCloseActions(randomFrom(indices), block::countDown);

        ActionFuture<CloseIndexResponse> closeIndexResponse = indicesAdmin().prepareClose("index-*").execute();
        assertTrue("Waiting for index to have a closing blocked", block.await(60, TimeUnit.SECONDS));
        assertFalse(closeIndexResponse.isDone());
        indices.forEach(ReopenWhileClosingIT::assertIndexIsBlocked);

        final List<String> reopenedIndices = randomSubsetOf(randomIntBetween(1, indices.size()), indices);
        assertAcked(indicesAdmin().prepareOpen(reopenedIndices.toArray(Strings.EMPTY_ARRAY)));

        releaseBlock.close();
        assertFalse(closeIndexResponse.get().isAcknowledged());

        indices.forEach(index -> {
            if (reopenedIndices.contains(index)) {
                assertIndexIsOpened(index);
            } else {
                assertIndexIsClosed(index);
            }
        });
    }

    private void createIndexWithDocs(final String indexName, final Collection<String> dataOnlyNodes) {
        createIndex(
            indexName,
            Settings.builder().put(indexSettings()).put("index.routing.allocation.include._name", String.join(",", dataOnlyNodes)).build()
        );
        final int nbDocs = scaledRandomIntBetween(1, 100);
        for (int i = 0; i < nbDocs; i++) {
            indexDoc(indexName, String.valueOf(i), "num", i);
        }
        assertIndexIsOpened(indexName);
    }

    /**
     * Intercepts and blocks the {@link TransportVerifyShardBeforeCloseAction} executed for the given index pattern.
     */
    private Releasable interceptVerifyShardBeforeCloseActions(final String indexPattern, final Runnable onIntercept) {
        final MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            internalCluster().getMasterName()
        );
        final ListenableFuture<Void> release = new ListenableFuture<>();
        for (DiscoveryNode node : internalCluster().clusterService().state().getNodes()) {
            mockTransportService.addSendBehavior(
                internalCluster().getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (action.startsWith(TransportVerifyShardBeforeCloseAction.NAME)) {
                        if (request instanceof TransportVerifyShardBeforeCloseAction.ShardRequest shardRequest) {
                            final String index = shardRequest.shardId().getIndexName();
                            if (Glob.globMatch(indexPattern, index)) {
                                logger.info("request {} intercepted for index {}", requestId, index);
                                onIntercept.run();
                                release.addListener(ActionListener.running(() -> {
                                    logger.info("request {} released for index {}", requestId, index);
                                    try {
                                        connection.sendRequest(requestId, action, request, options);
                                    } catch (IOException e) {
                                        throw new AssertionError(e);
                                    }
                                }));
                                return;
                            }
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }
        return Releasables.releaseOnce(() -> release.onResponse(null));
    }

    private static void assertIndexIsBlocked(final String... indices) {
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (String index : indices) {
            assertThat(clusterState.metadata().indices().get(index).getState(), is(IndexMetadata.State.OPEN));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(
                "Index " + index + " must have only 1 block with [id=" + INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks()
                    .indices()
                    .getOrDefault(index, emptySet())
                    .stream()
                    .filter(clusterBlock -> clusterBlock.id() == INDEX_CLOSED_BLOCK_ID)
                    .count(),
                equalTo(1L)
            );
        }
    }
}

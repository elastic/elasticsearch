/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A rename that gives an index's old name to another index while a bulk for it is in flight. The bulk resolved its target to an
 * index, by UUID, when it was accepted, and must keep addressing that index afterwards, whatever its name has become and whichever
 * index now has the old one: its documents land in the renamed index, and the blocks that apply to it are those of that index.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RenameIndexSwapIT extends ESIntegTestCase {

    private static final int DOCS = 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(HoldShardBulkPlugin.class);
    }

    /**
     * Holds the first shard-level bulk it sees, where it starts executing on the coordinating node, and hands the test a way to let it
     * proceed later. An action filter, not a transport hook, so nothing blocks a transport thread while the test changes the cluster.
     */
    public static class HoldShardBulkPlugin extends Plugin implements ActionPlugin {
        static final AtomicReference<String> indexUuidToHold = new AtomicReference<>();
        static final AtomicReference<Runnable> proceed = new AtomicReference<>();
        static final AtomicReference<CountDownLatch> held = new AtomicReference<>();

        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    final String uuid = indexUuidToHold.get();
                    if (uuid != null
                        && TransportShardBulkAction.ACTION_NAME.equals(action)
                        && request instanceof BulkShardRequest bulkShardRequest
                        && bulkShardRequest.shardId().getIndex().getUUID().equals(uuid)
                        && proceed.compareAndSet(null, () -> chain.proceed(task, action, request, listener))) {
                        held.get().countDown();
                        return;
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }

    /**
     * The documents of a held bulk land in the index the bulk was accepted for, now named "c", and none in the index that took
     * over its old name "a".
     */
    public void testHeldBulkAcrossSwapLandsInRenamedIndex() throws Exception {
        final BulkResponse response = bulkHeldAcrossSwap(false);
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        for (BulkItemResponse item : response.getItems()) {
            // not "a", which now names the other index
            assertEquals("c", item.getIndex());
        }
        refresh("c", "a");
        assertHitCount(prepareSearch("c").setSize(0), DOCS);
        assertHitCount(prepareSearch("a").setSize(0), 0);
    }

    /**
     * A write block put on the renamed index "c" after the swap refuses the held bulk. The bulk was routed under the name "a", which
     * now belongs to an index without that block, so checking blocks by name would let the bulk through.
     */
    public void testHeldBulkAcrossSwapHonoursBlockOfRenamedIndex() throws Exception {
        final BulkResponse response = bulkHeldAcrossSwap(true);
        assertTrue(response.hasFailures());
        for (BulkItemResponse item : response.getItems()) {
            assertTrue(item.isFailed());
            assertThat(item.getFailure().getCause(), instanceOf(ClusterBlockException.class));
        }
        refresh("c", "a");
        assertHitCount(prepareSearch("c").setSize(0), 0);
        assertHitCount(prepareSearch("a").setSize(0), 0);
    }

    /**
     * Control: a bulk held and released with nothing in between completes, so a failure of the tests above is the rename's.
     */
    public void testHeldBulkCompletesWithoutRename() throws Exception {
        final Cluster cluster = startClusterWithTwoIndices();
        final ActionFuture<BulkResponse> future = sendBulkAndHold(cluster);
        HoldShardBulkPlugin.proceed.get().run();
        final BulkResponse response = future.actionGet(SAFE_AWAIT_TIMEOUT);
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh("a");
        assertHitCount(prepareSearch("a").setSize(0), DOCS);
    }

    private record Cluster(String coordinator, String uuidA, String uuidB) {}

    /**
     * Sends a bulk for "a", holds it, swaps the names "a" and "b" through a temporary "c" in one aliases request, optionally blocks
     * writes on the renamed index, then releases the bulk and returns its response.
     */
    private BulkResponse bulkHeldAcrossSwap(boolean blockRenamedIndex) {
        final Cluster cluster = startClusterWithTwoIndices();
        final ActionFuture<BulkResponse> future = sendBulkAndHold(cluster);

        final IndicesAliasesRequest swap = new IndicesAliasesRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAliasAction(
            IndicesAliasesRequest.AliasActions.renameIndex().index("a").newIndex("c")
        ).addAliasAction(IndicesAliasesRequest.AliasActions.renameIndex().index("b").newIndex("a"));
        assertAcked(client().execute(TransportIndicesAliasesAction.TYPE, swap).actionGet());
        assertEquals(cluster.uuidA(), resolveIndex("c").getUUID());
        assertEquals(cluster.uuidB(), resolveIndex("a").getUUID());
        if (blockRenamedIndex) {
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "c");
        }

        HoldShardBulkPlugin.proceed.get().run();
        return future.actionGet(SAFE_AWAIT_TIMEOUT);
    }

    /**
     * A master, a coordinating data node and a data node holding the primaries of "a" and "b".
     */
    private Cluster startClusterWithTwoIndices() {
        internalCluster().startMasterOnlyNode();
        final String coordinator = internalCluster().startDataOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        final Settings settings = indexSettings(1, 0).put("index.routing.allocation.require._name", primaryNode).build();
        createIndex("a", settings, false);
        createIndex("b", settings, false);
        ensureGreen("a", "b");
        return new Cluster(coordinator, resolveIndex("a").getUUID(), resolveIndex("b").getUUID());
    }

    private ActionFuture<BulkResponse> sendBulkAndHold(Cluster cluster) {
        HoldShardBulkPlugin.proceed.set(null);
        HoldShardBulkPlugin.held.set(new CountDownLatch(1));
        HoldShardBulkPlugin.indexUuidToHold.set(cluster.uuidA());
        final BulkRequestBuilder bulk = client(cluster.coordinator()).prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulk.add(client(cluster.coordinator()).prepareIndex("a").setId(String.valueOf(i)).setSource("f", i));
        }
        final ActionFuture<BulkResponse> future = bulk.execute();
        safeAwait(HoldShardBulkPlugin.held.get());
        return future;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.admin.indices.dangling.DanglingIndexInfo;
import org.elasticsearch.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.elasticsearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.elasticsearch.action.admin.indices.dangling.list.NodeListDanglingIndicesResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexGraveyard.SETTING_MAX_TOMBSTONES;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * This class tests how dangling indices are handled, in terms of how they
 * are discovered, and how they can be accessed and manipulated through the
 * API.
 *
 * <p>See also <code>DanglingIndicesRestIT</code> in the <code>qa:smoke-test-http</code>
 * project.
 *
 * @see org.elasticsearch.action.admin.indices.dangling
 */
@ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class DanglingIndicesIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "test-idx-1";
    private static final String OTHER_INDEX_NAME = INDEX_NAME + "-other";

    private Settings buildSettings(int maxTombstones, boolean writeDanglingIndices) {
        return Settings.builder()
            // Limit the indices kept in the graveyard. This can be set to zero, so that
            // when we delete an index, it's definitely considered to be dangling.
            .put(SETTING_MAX_TOMBSTONES.getKey(), maxTombstones)
            .put(IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), writeDanglingIndices)
            .build();
    }

    private void ensurePendingDanglingIndicesWritten() throws Exception {
        assertBusy(
            () -> internalCluster().getInstances(IndicesService.class)
                .forEach(indicesService -> assertTrue(indicesService.allPendingDanglingIndicesWritten()))
        );
    }

    /**
     * Check that when dangling indices are not written, then they cannot be listed by the API
     */
    public void testDanglingIndicesCannotBeListedWhenNotWritten() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, false));

        createDanglingIndices(INDEX_NAME);

        // Since index recovery is async, we can't prove index recovery will never occur, just that it doesn't occur within some reasonable
        // amount of time
        assertFalse(
            "Did not expect dangling index " + INDEX_NAME + " to be listed",
            waitUntil(() -> listDanglingIndices().isEmpty() == false, 5, TimeUnit.SECONDS)
        );
    }

    /**
     * Check that when dangling indices are discovered, then they can be listed.
     */
    public void testDanglingIndicesCanBeListed() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);

        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();
        assertThat("Didn't get responses from all nodes", nodeResponses, hasSize(3));

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            if (nodeResponse.getNode().getName().equals(stoppedNodeName)) {
                assertThat("Expected node that was stopped to have one dangling index", nodeResponse.getDanglingIndices(), hasSize(1));

                final DanglingIndexInfo danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                assertThat(danglingIndexInfo.getIndexName(), equalTo(INDEX_NAME));
            } else {
                assertThat("Expected node that was never stopped to have no dangling indices", nodeResponse.getDanglingIndices(), empty());
            }
        }
    }

    /**
     * Check that dangling indices can be imported.
     */
    public void testDanglingIndicesCanBeImported() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);

        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest(danglingIndexUUID, true);
        clusterAdmin().importDanglingIndex(request).get();

        assertTrue("Expected dangling index " + INDEX_NAME + " to be recovered", indexExists(INDEX_NAME));
    }

    /**
     * Check that the when sending an import-dangling-indices request, the specified UUIDs are validated as
     * being dangling.
     */
    public void testDanglingIndicesMustExistToBeImported() {
        internalCluster().startNodes(1, buildSettings(0, true));

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest("NonExistentUUID", true);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterAdmin().importDanglingIndex(request).actionGet()
        );

        assertThat(e.getMessage(), containsString("No dangling index found for UUID [NonExistentUUID]"));
    }

    /**
     * Check that a dangling index can only be imported if "accept_data_loss" is set to true.
     */
    public void testMustAcceptDataLossToImportDanglingIndex() throws Exception {
        internalCluster().startNodes(3, buildSettings(0, true));

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        final ImportDanglingIndexRequest request = new ImportDanglingIndexRequest(danglingIndexUUID, false);

        Exception e = expectThrows(Exception.class, () -> clusterAdmin().importDanglingIndex(request).actionGet());

        assertThat(e.getMessage(), containsString("accept_data_loss must be set to true"));
    }

    /**
     * Check that dangling indices can be deleted. Since this requires that
     * we add an entry to the index graveyard, the graveyard size must be
     * greater than 1. To test deletes, we set the index graveyard size to
     * 1, then create two indices and delete them both while one node in
     * the cluster is stopped. The deletion of the second pushes the deletion
     * of the first out of the graveyard. When the stopped node is resumed,
     * only the second index will be found into the graveyard and the the
     * other will be considered dangling, and can therefore be listed and
     * deleted through the API
     */
    public void testDanglingIndexCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, true);
        internalCluster().startNodes(3, settings);

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        clusterAdmin().deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndexUUID, true)).actionGet();

        // The dangling index that we deleted ought to have been removed from disk. Check by
        // creating and deleting another index, which creates a new tombstone entry, which should
        // not cause the deleted dangling index to be considered "live" again, just because its
        // tombstone has been pushed out of the graveyard.
        createIndex("additional");
        assertAcked(indicesAdmin().prepareDelete("additional"));
        assertThat(listDanglingIndices(), is(empty()));
    }

    /**
     * Check that when a index is found to be dangling on more than one node, it can be deleted.
     */
    public void testDanglingIndexOverMultipleNodesCanBeDeleted() throws Exception {
        final Settings settings = buildSettings(1, true);
        internalCluster().startNodes(3, settings);

        createIndices(INDEX_NAME, OTHER_INDEX_NAME);

        ensurePendingDanglingIndicesWritten();

        // Restart 2 nodes, deleting the indices in their absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

                    @Override
                    public Settings onNodeStopped(String nodeName) throws Exception {
                        internalCluster().validateClusterFormed();
                        assertAcked(indicesAdmin().prepareDelete(INDEX_NAME));
                        assertAcked(indicesAdmin().prepareDelete(OTHER_INDEX_NAME));
                        return super.onNodeStopped(nodeName);
                    }
                });

                return super.onNodeStopped(nodeName);
            }
        });

        final AtomicReference<List<DanglingIndexInfo>> danglingIndices = new AtomicReference<>();

        final List<DanglingIndexInfo> results = listDanglingIndices();

        // Both the stopped nodes should have found a dangling index.
        assertThat(results, hasSize(2));
        danglingIndices.set(results);

        // Try to delete the index - this request should succeed
        client().admin()
            .cluster()
            .deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndices.get().get(0).getIndexUUID(), true))
            .actionGet();

        // The dangling index that we deleted ought to have been removed from disk. Check by
        // creating and deleting another index, which creates a new tombstone entry, which should
        // not cause the deleted dangling index to be considered "live" again, just because its
        // tombstone has been pushed out of the graveyard.
        createIndex("additional");
        assertAcked(indicesAdmin().prepareDelete("additional"));
        assertBusy(() -> assertThat(listDanglingIndices(), empty()));
    }

    /**
     * Check that when deleting a dangling index, it is required that the "accept_data_loss" flag is set.
     */
    public void testDeleteDanglingIndicesRequiresDataLossFlagToBeTrue() throws Exception {
        final Settings settings = buildSettings(1, true);
        internalCluster().startNodes(3, settings);

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        Exception e = expectThrows(
            Exception.class,
            () -> clusterAdmin().deleteDanglingIndex(new DeleteDanglingIndexRequest(danglingIndexUUID, false)).actionGet()
        );

        assertThat(e.getMessage(), containsString("accept_data_loss must be set to true"));
    }

    /**
     * Check that import-and-delete of a dangling index doesn't have a race condition that bypasses the graveyard and permits a re-import.
     */
    public void testDanglingIndicesImportedAndDeletedCannotBeReimported() throws Exception {

        final Settings settings = buildSettings(1, true);
        internalCluster().startNodes(3, settings);

        final String stoppedNodeName = createDanglingIndices(INDEX_NAME, OTHER_INDEX_NAME);
        final String danglingIndexUUID = findDanglingIndexForNode(stoppedNodeName, INDEX_NAME);

        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicBoolean isImporting = new AtomicBoolean(true);
        final Thread[] importThreads = new Thread[2];
        for (int i = 0; i < importThreads.length; i++) {
            importThreads[i] = new Thread(() -> {
                try {
                    startLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }

                while (isImporting.get()) {
                    try {
                        clusterAdmin().importDanglingIndex(new ImportDanglingIndexRequest(danglingIndexUUID, true)).get();
                    } catch (Exception e) {
                        // failures are expected
                    }
                }
            });
            importThreads[i].start();
        }

        startLatch.countDown();

        final TimeValue timeout = TimeValue.timeValueSeconds(10);
        final long endTimeMillis = System.currentTimeMillis() + timeout.millis();
        while (isImporting.get() && System.currentTimeMillis() < endTimeMillis) {
            try {
                indicesAdmin().prepareDelete(INDEX_NAME).get(timeout);
                isImporting.set(false);
            } catch (Exception e) {
                // failures are expected
            }
        }

        try {
            if (isImporting.get()) {
                isImporting.set(false);
                try {
                    indicesAdmin().prepareDelete(INDEX_NAME).get(timeout);
                } catch (Exception e) {
                    throw new AssertionError("delete index never succeeded", e);
                }
                throw new AssertionError("delete index succeeded but too late");
            }
        } finally {
            for (final Thread importThread : importThreads) {
                importThread.join();
            }
        }

        final Metadata metadata = clusterAdmin().prepareState().clear().setMetadata(true).get().getState().metadata();
        assertTrue(metadata.indexGraveyard().toString(), metadata.indexGraveyard().containsIndex(new Index(INDEX_NAME, danglingIndexUUID)));
        assertNull(Strings.toString(metadata, true, true), metadata.index(INDEX_NAME));
    }

    /**
     * Helper that fetches the current list of dangling indices.
     */
    private List<DanglingIndexInfo> listDanglingIndices() {
        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();

        final List<DanglingIndexInfo> results = new ArrayList<>();

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            results.addAll(nodeResponse.getDanglingIndices());
        }

        return results;
    }

    /**
     * Simple helper that creates one or more indices, and importantly,
     * checks that they are green before proceeding. This is important
     * because the tests in this class stop and restart nodes, assuming
     * that each index has a primary or replica shard on every node, and if
     * a node is stopped prematurely, this assumption is broken.
     */
    private void createIndices(String... indices) {
        assert indices.length > 0;
        for (String index : indices) {
            createIndex(index, Settings.builder().put("number_of_replicas", 2).put("routing.allocation.total_shards_per_node", 1).build());
        }
        ensureGreen(indices);
    }

    /**
     * Creates a number of dangling indices by first creating then, then stopping a data node
     * and deleting the indices while the node is stopped.
     * @param indices the indices to create and delete
     * @return the name of the stopped node
     */
    private String createDanglingIndices(String... indices) throws Exception {
        createIndices(indices);

        ensurePendingDanglingIndicesWritten();

        AtomicReference<String> stoppedNodeName = new AtomicReference<>();

        final int nodes = internalCluster().size();
        // Restart node, deleting the indices in its absence, so that there is a dangling index to recover
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {

            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                internalCluster().validateClusterFormed();
                stoppedNodeName.set(nodeName);
                for (String index : indices) {
                    assertAcked(indicesAdmin().prepareDelete(index));
                }
                return super.onNodeStopped(nodeName);
            }
        });
        ensureStableCluster(nodes);

        return stoppedNodeName.get();
    }

    private String findDanglingIndexForNode(String stoppedNodeName, String indexName) {
        String danglingIndexUUID = null;

        final ListDanglingIndicesResponse response = client().admin()
            .cluster()
            .listDanglingIndices(new ListDanglingIndicesRequest())
            .actionGet();
        assertThat(response.status(), equalTo(RestStatus.OK));

        final List<NodeListDanglingIndicesResponse> nodeResponses = response.getNodes();

        for (NodeListDanglingIndicesResponse nodeResponse : nodeResponses) {
            if (nodeResponse.getNode().getName().equals(stoppedNodeName)) {
                final DanglingIndexInfo danglingIndexInfo = nodeResponse.getDanglingIndices().get(0);
                assertThat(danglingIndexInfo.getIndexName(), equalTo(indexName));

                danglingIndexUUID = danglingIndexInfo.getIndexUUID();
                break;
            }
        }

        assertNotNull("Failed to find a dangling index UUID for node [" + stoppedNodeName + "]", danglingIndexUUID);

        return danglingIndexUUID;
    }
}

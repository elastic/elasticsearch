/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.SuppressFileSystems("*")
public class MultiProjectIndexRefreshBlockIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    public void testRefreshBlockRemovedIfZeroReplicas() throws Exception {
        startMasterAndIndexNode(
            Settings.builder()
                .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(StatelessPlugin.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), true)
                .build()
        );

        var project1 = createProject();
        var project2 = createProject();
        var client1 = client().projectClient(project1);
        var client2 = client().projectClient(project2);

        try {
            createIndexWithBlock(client1, "index-1");
            createIndexWithBlock(client2, "index-2");

            assertBusy(() -> {
                var blocks = clusterBlocks();
                assertThat(blocks.hasIndexBlock(project1, "index-1", IndexMetadata.INDEX_REFRESH_BLOCK), is(true));
                assertThat(blocks.hasIndexBlock(project2, "index-2", IndexMetadata.INDEX_REFRESH_BLOCK), is(true));
            });

            client1.admin()
                .indices()
                .prepareUpdateSettings("index-1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .get();

            assertBusy(() -> {
                var blocks = clusterBlocks();
                assertThat(blocks.hasIndexBlock(project1, "index-1", IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
                assertThat(blocks.hasIndexBlock(project2, "index-2", IndexMetadata.INDEX_REFRESH_BLOCK), is(true));
            });

            client2.admin()
                .indices()
                .prepareUpdateSettings("index-2")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .get();

            assertBusy(() -> {
                var blocks = clusterBlocks();
                assertThat(blocks.hasIndexBlock(project1, "index-1", IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
                assertThat(blocks.hasIndexBlock(project2, "index-2", IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
            });
        } finally {
            removeProject(project1);
            removeProject(project2);
        }
    }

    public void testRefreshBlockRemovedAfterSearchNodeAdded() throws Exception {
        startMasterAndIndexNode(
            Settings.builder()
                .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(StatelessPlugin.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), true)
                .build()
        );

        var project1 = createProject();
        var project2 = createProject();
        var client1 = client().projectClient(project1);
        var client2 = client().projectClient(project2);

        try {
            int p1Count = randomIntBetween(1, 5);
            int p2Count = randomIntBetween(1, 5);
            for (int i = 0; i < p1Count; i++) {
                createIndexWithBlock(client1, "p1-index-" + i);
            }
            for (int i = 0; i < p2Count; i++) {
                createIndexWithBlock(client2, "p2-index-" + i);
            }

            assertBusy(() -> {
                var blocks = clusterBlocks();
                for (int i = 0; i < p1Count; i++) {
                    assertThat(blocks.hasIndexBlock(project1, "p1-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(true));
                }
                for (int i = 0; i < p2Count; i++) {
                    assertThat(blocks.hasIndexBlock(project2, "p2-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(true));
                }
            });

            startSearchNode();

            assertBusy(() -> {
                var blocks = clusterBlocks();
                for (int i = 0; i < p1Count; i++) {
                    assertThat(blocks.hasIndexBlock(project1, "p1-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
                }
                for (int i = 0; i < p2Count; i++) {
                    assertThat(blocks.hasIndexBlock(project2, "p2-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
                }
            });
        } finally {
            removeProject(project1);
            removeProject(project2);
        }
    }

    public void testRefreshBlockRemovedAfterDelay() throws Exception {
        startMasterAndIndexNode(
            Settings.builder()
                .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(StatelessPlugin.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), true)
                .build()
        );

        var project1 = createProject();
        var project2 = createProject();
        var client1 = client().projectClient(project1);
        var client2 = client().projectClient(project2);

        try {
            int p1Count = randomIntBetween(1, 5);
            int p2Count = randomIntBetween(1, 5);
            final Set<Index> allBlockedIndices = new HashSet<>();
            for (int i = 0; i < p1Count; i++) {
                var indexName = "p1-index-" + i;
                createIndexWithBlock(client1, indexName);
                allBlockedIndices.add(resolveIndex(client1, indexName));
            }
            for (int i = 0; i < p2Count; i++) {
                var indexName = "p2-index-" + i;
                createIndexWithBlock(client2, indexName);
                allBlockedIndices.add(resolveIndex(client2, indexName));
            }

            assertBusy(() -> {
                var service = internalCluster().getCurrentMasterNodeInstance(RemoveRefreshClusterBlockService.class);
                assertThat(service.blockedIndices(), equalTo(allBlockedIndices));
            });

            updateClusterSettings(
                Settings.builder().put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueSeconds(1L))
            );

            assertBusy(() -> {
                var service = internalCluster().getCurrentMasterNodeInstance(RemoveRefreshClusterBlockService.class);
                assertThat(service.blockedIndices(), hasSize(0));
            });

            var blocks = clusterBlocks();
            for (int i = 0; i < p1Count; i++) {
                assertThat(blocks.hasIndexBlock(project1, "p1-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
            }
            for (int i = 0; i < p2Count; i++) {
                assertThat(blocks.hasIndexBlock(project2, "p2-index-" + i, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
            }
        } finally {
            removeProject(project1);
            removeProject(project2);
        }
    }

    private ProjectId createProject() throws Exception {
        var projectId = randomUniqueProjectId();
        putProject(projectId);
        return projectId;
    }

    private static void createIndexWithBlock(Client projectClient, String indexName) {
        assertAcked(
            projectClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings(1, 1))
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
    }

    private static Index resolveIndex(Client projectClient, String index) {
        GetIndexResponse response = projectClient.admin().indices().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(index).get();
        assertTrue("index " + index + " not found", response.getSettings().containsKey(index));
        String uuid = response.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    private static ClusterBlocks clusterBlocks() {
        return client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).get().getState().blocks();
    }
}

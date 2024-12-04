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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BlockRefreshUponIndexCreationIT extends AbstractStatelessIntegTestCase {

    public void testIndexCreatedWithRefreshBlock() {
        startMasterAndIndexNode(useRefreshBlockSetting(true));

        int nbReplicas = randomIntBetween(0, 3);
        if (0 < nbReplicas) {
            startSearchNodes(nbReplicas);
        }

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, nbReplicas)));
        ensureGreen(indexName);

        var blocks = clusterBlocks();
        assertThat(blocks.hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(0 < nbReplicas));
    }

    public void testIndexCreatedWithRefreshBlockDisabled() {
        startMasterAndIndexNode(useRefreshBlockSetting(false));

        int nbReplicas = randomIntBetween(0, 3);
        if (0 < nbReplicas) {
            startSearchNodes(nbReplicas);
        }

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, nbReplicas)));
        ensureGreen(indexName);

        var blocks = clusterBlocks();
        assertThat(blocks.hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(false));
    }

    public void testRefreshBlockRemovedAfterRestart() throws Exception {
        var masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .put(useRefreshBlockSetting(true))
                .build()
        );
        startIndexNode();

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1)).setWaitForActiveShards(ActiveShardCount.NONE));
        ensureYellow(indexName);

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), is(true));

        internalCluster().restartNode(masterNode);
        ensureYellow(indexName);

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
    }

    private static ClusterBlocks clusterBlocks() {
        return client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).get().getState().blocks();
    }

    private static Settings useRefreshBlockSetting(boolean value) {
        return Settings.builder().put(Stateless.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), value).build();
    }
}

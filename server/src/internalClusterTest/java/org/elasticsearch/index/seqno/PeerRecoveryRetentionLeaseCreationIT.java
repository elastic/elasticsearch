/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.seqno;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.VersionUtils;

import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class PeerRecoveryRetentionLeaseCreationIT extends ESIntegTestCase {
    private static final String INDEX_NAME = "index";

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testCanRecoverFromStoreWithoutPeerRecoveryRetentionLease() throws Exception {
        /*
         * In a full cluster restart from a version without peer-recovery retention leases, the leases on disk will not include a lease for
         * the local node. The same sort of thing can happen in weird situations involving dangling indices. This test ensures that a
         * primary that is recovering from store creates a lease for itself.
         */

        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();

        assertAcked(prepareCreate(INDEX_NAME).setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))));
        ensureGreen(INDEX_NAME);


        IndicesService service = internalCluster().getInstance(IndicesService.class, dataNode);
        String uuid = client().admin().indices().getIndex(new GetIndexRequest().indices(INDEX_NAME)).actionGet().getSetting(INDEX_NAME,
            IndexMetadata.SETTING_INDEX_UUID);
        Path path = service.indexService(new Index(INDEX_NAME, uuid)).getShard(0).shardPath().getShardStatePath();

        long version = between(1, 1000);
        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                RetentionLeases.FORMAT.writeAndCleanup(new RetentionLeases(1, version, List.of()), path);
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen(INDEX_NAME);
        final RetentionLeases retentionLeases = getRetentionLeases();
        final String nodeId = client().admin().cluster().prepareNodesInfo(dataNode).clear().get().getNodes().get(0).getNode().getId();
        assertTrue("expected lease for [" + nodeId + "] in " + retentionLeases,
            retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(nodeId)));
        // verify that we touched the right file.
        assertThat(retentionLeases.version(), equalTo(version + 1));
    }

    public RetentionLeases getRetentionLeases() {
        return client().admin().indices().prepareStats(INDEX_NAME).get().getShards()[0]
            .getRetentionLeaseStats().retentionLeases();
    }

}

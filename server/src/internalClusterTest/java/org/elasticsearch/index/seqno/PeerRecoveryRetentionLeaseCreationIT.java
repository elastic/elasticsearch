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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetadata;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.VersionUtils;

import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class PeerRecoveryRetentionLeaseCreationIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/48701")
    public void testCanRecoverFromStoreWithoutPeerRecoveryRetentionLease() throws Exception {
        /*
         * In a full cluster restart from a version without peer-recovery retention leases, the leases on disk will not include a lease for
         * the local node. The same sort of thing can happen in weird situations involving dangling indices. This test ensures that a
         * primary that is recovering from store creates a lease for itself.
         */

        internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final Path[] nodeDataPaths = internalCluster().getInstance(NodeEnvironment.class, dataNode).nodeDataPaths();

        assertAcked(prepareCreate("index").setSettings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexMetadata.SETTING_VERSION_CREATED,
                VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))));
        ensureGreen("index");

        // Change the node ID so that the persisted retention lease no longer applies.
        final String oldNodeId = client().admin().cluster().prepareNodesInfo(dataNode).clear().get().getNodes().get(0).getNode().getId();
        final String newNodeId = randomValueOtherThan(oldNodeId, () -> UUIDs.randomBase64UUID(random()));

        internalCluster().restartNode(dataNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                final NodeMetadata nodeMetadata = new NodeMetadata(newNodeId, Version.CURRENT);
                NodeMetadata.FORMAT.writeAndCleanup(nodeMetadata, nodeDataPaths);
                return Settings.EMPTY;
            }
        });

        ensureGreen("index");
        assertThat(client().admin().cluster().prepareNodesInfo(dataNode).clear().get().getNodes().get(0).getNode().getId(),
            equalTo(newNodeId));
        final RetentionLeases retentionLeases = client().admin().indices().prepareStats("index").get().getShards()[0]
            .getRetentionLeaseStats().retentionLeases();
        assertTrue("expected lease for [" + newNodeId + "] in " + retentionLeases,
            retentionLeases.contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(newNodeId)));
    }

}

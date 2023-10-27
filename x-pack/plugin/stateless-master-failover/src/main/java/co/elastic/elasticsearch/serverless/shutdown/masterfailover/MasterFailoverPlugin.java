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

package co.elastic.elasticsearch.serverless.shutdown.masterfailover;

import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ShutdownAwarePlugin;

import java.util.Collection;
import java.util.List;

public class MasterFailoverPlugin extends Plugin implements ShutdownAwarePlugin {

    private static final Logger logger = LogManager.getLogger(MasterFailoverPlugin.class);

    ClusterService clusterService;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterService = services.clusterService();
        return List.of();
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        // called on the elected master (in a cluster state listener) so we only need to check our own node ID
        return nodeId.equals(clusterService.state().nodes().getLocalNodeId()) == false;
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {
        // master handles shutdown signal via StatelessElectionStrategy#nodeMayWinElection
    }
}

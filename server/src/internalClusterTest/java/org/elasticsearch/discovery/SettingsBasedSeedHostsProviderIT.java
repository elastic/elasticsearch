/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportInfo;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SettingsBasedSeedHostsProviderIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));

        // super.nodeSettings enables file-based discovery, but here we disable it again so we can test the static list:
        if (randomBoolean()) {
            builder.putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey());
        } else {
            builder.remove(DISCOVERY_SEED_PROVIDERS_SETTING.getKey());
        }

        // super.nodeSettings sets this to an empty list, which disables any search for other nodes, but here we want this to happen:
        builder.remove(DISCOVERY_SEED_HOSTS_SETTING.getKey());

        return builder.build();
    }

    public void testClusterFormsWithSingleSeedHostInSettings() {
        final String seedNodeName = internalCluster().startNode();
        final NodesInfoResponse nodesInfoResponse = client(seedNodeName).admin()
            .cluster()
            .nodesInfo(new NodesInfoRequest("_local"))
            .actionGet();
        final String seedNodeAddress = nodesInfoResponse.getNodes()
            .get(0)
            .getInfo(TransportInfo.class)
            .getAddress()
            .publishAddress()
            .toString();
        logger.info("--> using seed node address {}", seedNodeAddress);

        int extraNodes = randomIntBetween(1, 5);
        internalCluster().startNodes(
            extraNodes,
            Settings.builder().putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), seedNodeAddress).build()
        );

        ensureStableCluster(extraNodes + 1);
    }
}

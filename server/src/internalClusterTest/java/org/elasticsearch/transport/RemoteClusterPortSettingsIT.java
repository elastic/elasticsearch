/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.hamcrest.Matchers.containsString;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RemoteClusterPortSettingsIT extends ESIntegTestCase {

    public void testDirectlyConfiguringTransportProfileForRemoteClusterWillFailToStartTheNode() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        internalCluster().setBootstrapMasterNodeIndex(0);

        final Settings.Builder builder = Settings.builder()
            .put(randomBoolean() ? masterNode() : dataOnlyNode())
            .put("discovery.initial_state_timeout", "1s")
            .put("remote_cluster_server.enabled", true);

        // Test that the same error message is always reported for direct usage of the _remote_cluster profile
        switch (randomIntBetween(0, 2)) {
            case 0 -> builder.put("transport.profiles._remote_cluster.tcp.keep_alive", true);
            case 1 -> builder.put("transport.profiles._remote_cluster.port", 9900);
            default -> builder.put("transport.profiles._remote_cluster.port", 9900)
                .put("transport.profiles._remote_cluster.tcp.keep_alive", true);
        }

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> internalCluster().startNode(builder));
        assertThat(
            e.getMessage(),
            containsString(
                "Remote Access settings should not be configured using the [_remote_cluster] profile. "
                    + "Use the [remote_cluster.] settings instead."
            )
        );
    }

}

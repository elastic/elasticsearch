/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse;
import org.elasticsearch.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.junit.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.transport.ProxyConnectionStrategy.PROXY_ADDRESS;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE;
import static org.elasticsearch.transport.SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS;

public class CrossClusterSearchSkipUnavailableIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER_NAME = "remote-cluster";
    private static final boolean REMOTE_CLUSTER_SKIP_UNAVAILABLE_SETTING = true;

    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_NAME);
    }

    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_NAME, REMOTE_CLUSTER_SKIP_UNAVAILABLE_SETTING);
    }

    /**
     * Reproduce <a href="https://github.com/elastic/elasticsearch/issues/107125">support issue</a>
     */
    public void testSkipUnavailableDoesNotChangeOnConfigurationFailure() throws Exception {
        // 1. Configure a remote cluster with skip_unavailable set to true
        assertBusyRemoteClusterConnectionStatus(true);

        // 2. Verify the configuration, note that skip_unavailable is true.
        assertSkipUnavailableIs(REMOTE_CLUSTER_SKIP_UNAVAILABLE_SETTING);

        // 3. Introduce an error by setting an incorrect remote cluster address:
        safeGet(
            client().execute(
                ClusterUpdateSettingsAction.INSTANCE,
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder()
                        .put(PROXY_ADDRESS.getConcreteSettingForNamespace(REMOTE_CLUSTER_NAME).getKey(), "hostname.is.invalid:1234")
                        // ensure we switch to proxy mode if we were in seed mode
                        .putNull(REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(REMOTE_CLUSTER_NAME).getKey())
                        .put("cluster.remote." + REMOTE_CLUSTER_NAME + ".mode", "proxy")
                )
            )
        );

        // 4. Observe that the remote connection fails and skip_unavailable is automatically set to false.
        assertBusyRemoteClusterConnectionStatus(false);
        assertSkipUnavailableIs(REMOTE_CLUSTER_SKIP_UNAVAILABLE_SETTING); // <-- The issue says this is supposed to be false now?
    }

    private void assertBusyRemoteClusterConnectionStatus(boolean expectConnected) throws Exception {
        RemoteInfoResponse remoteInfoResponse = safeGet(client().execute(TransportRemoteInfoAction.TYPE, new RemoteInfoRequest()));
        assertBusy(() -> {
            RemoteConnectionInfo connectionInfo = remoteInfoResponse.getInfos()
                .stream()
                .filter(rci -> REMOTE_CLUSTER_NAME.equals(rci.getClusterAlias()))
                .findAny()
                .orElseThrow(() -> new AssertionError("remote cluster alias not found"));
            assertEquals(expectConnected, connectionInfo.isConnected());
        });
    }

    private void assertSkipUnavailableIs(boolean expectedValue) {
        ClusterGetSettingsAction.Response settingsResponse = safeGet(
            client().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request())
        );
        Settings settings = settingsResponse.settings();
        logger.info(settings);
        boolean skipUnavailable = REMOTE_CLUSTER_SKIP_UNAVAILABLE.getConcreteSettingForNamespace(REMOTE_CLUSTER_NAME).get(settings);
        Assert.assertEquals(expectedValue, skipUnavailable);
    }
}

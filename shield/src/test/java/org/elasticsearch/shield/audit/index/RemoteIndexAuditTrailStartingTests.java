/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.hamcrest.Matchers.is;

/**
 * This test checks to ensure that the IndexAuditTrail starts properly when indexing to a remote cluster
 */
public class RemoteIndexAuditTrailStartingTests extends ShieldIntegTestCase {

    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + SUITE_CLUSTER_NODE_PREFIX;

    private InternalTestCluster remoteCluster;

    private final boolean useSSL = randomBoolean();
    private final boolean localAudit = randomBoolean();
    private final String outputs = randomFrom("index", "logfile", "index,logfile");

    @Override
    public boolean sslTransportEnabled() {
        return useSSL;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("shield.audit.enabled", localAudit)
                .put("shield.audit.outputs", outputs)
                .build();
    }

    @Before
    public void startRemoteCluster() throws IOException {
        final List<String> addresses = new ArrayList<>();
        // get addresses for current cluster
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        final String clusterName = response.getClusterNameAsString();
        for (NodeInfo nodeInfo : response.getNodes()) {
            InetSocketTransportAddress address = (InetSocketTransportAddress) nodeInfo.getTransport().address().publishAddress();
            addresses.add(address.address().getHostString() + ":" + address.address().getPort());
        }

        // create another cluster
        String cluster2Name = clusterName(Scope.SUITE.name(), randomLong());

        // Setup a second test cluster with randomization for number of nodes, shield enabled, and SSL
        final int numNodes = randomIntBetween(2, 3);
        ShieldSettingsSource cluster2SettingsSource = new ShieldSettingsSource(numNodes, useSSL, systemKey(), createTempDir(), Scope.SUITE) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        .put("shield.audit.enabled", true)
                        .put("shield.audit.outputs", randomFrom("index", "index,logfile"))
                        .putArray("shield.audit.index.client.hosts", addresses.toArray(new String[addresses.size()]))
                        .put("shield.audit.index.client.cluster.name", clusterName)
                        .put("shield.audit.index.client.shield.user", ShieldSettingsSource.DEFAULT_USER_NAME + ":" + ShieldSettingsSource.DEFAULT_PASSWORD);

                if (useSSL) {
                    for (Map.Entry<String, String> entry : getClientSSLSettings().getAsMap().entrySet()) {
                        builder.put("shield.audit.index.client." + entry.getKey(), entry.getValue());
                    }
                }
                return builder.build();
            }
        };
        remoteCluster = new InternalTestCluster("network", randomLong(), createTempDir(), numNodes, numNodes, cluster2Name, cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, true);
        remoteCluster.beforeTest(getRandom(), 0.5);
    }

    @After
    public void stopRemoteCluster() throws Exception {
        if (remoteCluster != null) {
            try {
                remoteCluster.wipe();
            } finally {
                remoteCluster.afterTest();
            }
            remoteCluster.close();
        }
    }

    @Test
    public void testThatRemoteAuditInstancesAreStarted() throws Exception {
        Iterable<IndexAuditTrail> auditTrails = remoteCluster.getInstances(IndexAuditTrail.class);
        for (final IndexAuditTrail auditTrail : auditTrails) {
            awaitBusy(new Predicate<Void>() {
                @Override
                public boolean apply(Void aVoid) {
                    return auditTrail.state() == IndexAuditTrail.State.STARTED;
                }
            }, 2L, TimeUnit.SECONDS);
            assertThat(auditTrail.state(), is(IndexAuditTrail.State.STARTED));
        }
    }

}

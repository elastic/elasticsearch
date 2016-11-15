/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.is;

/**
 * This test checks to ensure that the IndexAuditTrail starts properly when indexing to a remote cluster. The cluster
 * started by the integration tests is indexed into by the remote cluster started before the test.
 *
 * The cluster started by the integrations tests may also index into itself...
 */
@ClusterScope(scope = Scope.TEST)
@TestLogging("org.elasticsearch.xpack.security.audit.index:TRACE")
public class RemoteIndexAuditTrailStartingTests extends SecurityIntegTestCase {

    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + TEST_CLUSTER_NODE_PREFIX;

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
                .put("xpack.security.audit.enabled", localAudit)
                .put("xpack.security.audit.outputs", outputs)
                .build();
    }

    @Override
    protected Set<String> excludeTemplates() {
        return Collections.singleton(IndexAuditTrail.INDEX_TEMPLATE_NAME);
    }

    @Override
    public void beforeIndexDeletion() {
        // For this test, this is a NO-OP because the index audit trail will continue to capture events and index after
        // the tests have completed. The default implementation of this method expects that nothing is performing operations
        // after the test has completed
    }

    @Before
    public void startRemoteCluster() throws IOException, InterruptedException {
        final List<String> addresses = new ArrayList<>();
        // get addresses for current cluster
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        final String clusterName = response.getClusterName().value();
        for (NodeInfo nodeInfo : response.getNodes()) {
            TransportAddress address = nodeInfo.getTransport().address().publishAddress();
            addresses.add(address.address().getHostString() + ":" + address.address().getPort());
        }

        // create another cluster
        String cluster2Name = clusterName(Scope.TEST.name(), randomLong());

        // Setup a second test cluster with randomization for number of nodes, security enabled, and SSL
        final int numNodes = randomIntBetween(2, 3);
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(numNodes, useSSL, systemKey(), createTempDir(), Scope.TEST) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        .put("xpack.security.audit.enabled", true)
                        .put("xpack.security.audit.outputs", randomFrom("index", "index,logfile"))
                        .putArray("xpack.security.audit.index.client.hosts", addresses.toArray(new String[addresses.size()]))
                        .put("xpack.security.audit.index.client.cluster.name", clusterName)
                        .put("xpack.security.audit.index.client.xpack.security.user", DEFAULT_USER_NAME + ":" + DEFAULT_PASSWORD);

                for (Map.Entry<String, String> entry : getClientSSLSettings().getAsMap().entrySet()) {
                    builder.put("xpack.security.audit.index.client." + entry.getKey(), entry.getValue());
                }
                return builder.build();
            }
        };
        remoteCluster = new InternalTestCluster(randomLong(), createTempDir(), false, true, numNodes, numNodes,
                cluster2Name, cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(), getClientWrapper());
        remoteCluster.beforeTest(random(), 0.5);
        assertNoTimeout(remoteCluster.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get());
    }

    @After
    public void stopRemoteCluster() throws Exception {
        if (remoteCluster != null) {
            remoteCluster.getInstance(AuditTrailService.class).getAuditTrails().stream()
                    .filter(t -> t.name().equals(IndexAuditTrail.NAME))
                    .forEach((auditTrail) -> ((IndexAuditTrail) auditTrail).stop());

            try {
                remoteCluster.wipe(Collections.<String>emptySet());
            } finally {
                remoteCluster.afterTest();
            }
            remoteCluster.close();
        }

        // stop the index audit trail so that the shards aren't locked causing the test to fail
        internalCluster().getInstance(AuditTrailService.class).getAuditTrails().stream()
                .filter(t -> t.name().equals(IndexAuditTrail.NAME))
                .forEach((auditTrail) -> ((IndexAuditTrail) auditTrail).stop());
    }

    public void testThatRemoteAuditInstancesAreStarted() throws Exception {
        AuditTrailService auditTrailService = remoteCluster.getInstance(AuditTrailService.class);
        Optional<AuditTrail> auditTrail = auditTrailService.getAuditTrails().stream()
            .filter(t -> t.name().equals(IndexAuditTrail.NAME)).findFirst();
        assertTrue(auditTrail.isPresent());
        IndexAuditTrail indexAuditTrail = (IndexAuditTrail)auditTrail.get();

        awaitBusy(() -> indexAuditTrail.state() == IndexAuditTrail.State.STARTED);
        assertThat(indexAuditTrail.state(), is(IndexAuditTrail.State.STARTED));
    }
}

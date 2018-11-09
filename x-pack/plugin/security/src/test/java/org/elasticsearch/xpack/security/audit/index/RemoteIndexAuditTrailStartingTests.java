/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

/**
 * This test checks to ensure that the IndexAuditTrail starts properly when indexing to a remote cluster. The cluster
 * started by the integration tests is indexed into by the remote cluster started before the test.
 *
 * The cluster started by the integrations tests may also index into itself...
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, transportClientRatio = 0.0, supportsDedicatedMasters = false)
@TestLogging("org.elasticsearch.xpack.security.audit.index:TRACE")
public class RemoteIndexAuditTrailStartingTests extends SecurityIntegTestCase {

    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + TEST_CLUSTER_NODE_PREFIX;

    private InternalTestCluster remoteCluster;

    private final boolean sslEnabled = randomBoolean();
    private final boolean localAudit = randomBoolean();
    private final String outputs = randomFrom("index", "logfile", "index,logfile");

    @Override
    public boolean transportSSLEnabled() {
        return sslEnabled;
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
        return Sets.newHashSet(SecurityIndexManager.SECURITY_TEMPLATE_NAME, IndexAuditTrail.INDEX_TEMPLATE_NAME);
    }

    @Override
    protected int numberOfShards() {
        return 1; // limit ourselves to a single shard in order to avoid timeout issues with large numbers of shards in tests
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

        // Setup a second test cluster with a single node, security enabled, and SSL
        final int numNodes = 1;
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(sslEnabled, createTempDir(), Scope.TEST) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        // Disable native ML autodetect_process as the c++ controller won't be available
//                        .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                        .put("xpack.security.audit.enabled", true)
                        .put("xpack.security.audit.outputs", randomFrom("index", "index,logfile"))
                        .putList("xpack.security.audit.index.client.hosts", addresses.toArray(new String[addresses.size()]))
                        .put("xpack.security.audit.index.client.cluster.name", clusterName)
                        .put("xpack.security.audit.index.client.xpack.security.user",
                             TEST_USER_NAME + ":" + SecuritySettingsSourceField.TEST_PASSWORD)
                        .put("xpack.security.audit.index.settings.index.number_of_shards", 1)
                        .put("xpack.security.audit.index.settings.index.number_of_replicas", 0);

                addClientSSLSettings(builder, "xpack.security.audit.index.client.");
                builder.put("xpack.security.audit.index.client.xpack.security.transport.ssl.enabled", sslEnabled);
                return builder.build();
            }
        };
        remoteCluster = new InternalTestCluster(randomLong(), createTempDir(), false, true, numNodes, numNodes,
                cluster2Name, cluster2SettingsSource, 0, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(), getClientWrapper());
        remoteCluster.beforeTest(random(), 0.0);
        assertNoTimeout(remoteCluster.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get());
    }

    @After
    public void stopRemoteCluster() throws Exception {
        List<Closeable> toStop = new ArrayList<>();
        // stop the index audit trail so that the shards aren't locked causing the test to fail
        toStop.add(() -> StreamSupport.stream(internalCluster().getInstances(AuditTrailService.class).spliterator(), false)
                .map(s -> s.getAuditTrails()).flatMap(List::stream)
                .filter(t -> t.name().equals(IndexAuditTrail.NAME))
                .forEach((auditTrail) -> ((IndexAuditTrail) auditTrail).stop()));
        // first stop both audit trails otherwise we keep on indexing
        if (remoteCluster != null) {
            toStop.add(() -> StreamSupport.stream(remoteCluster.getInstances(AuditTrailService.class).spliterator(), false)
                    .map(s -> s.getAuditTrails()).flatMap(List::stream)
                    .filter(t -> t.name().equals(IndexAuditTrail.NAME))
                    .forEach((auditTrail) -> ((IndexAuditTrail) auditTrail).stop()));
            toStop.add(() -> remoteCluster.wipe(excludeTemplates()));
            toStop.add(remoteCluster::afterTest);
            toStop.add(remoteCluster);
        }


        IOUtils.close(toStop);
    }

    public void testThatRemoteAuditInstancesAreStarted() throws Exception {
        logger.info("Test configuration: ssl=[{}] localAudit=[{}][{}]", sslEnabled, localAudit, outputs);
        // we ensure that all instances present are started otherwise we will have issues
        // and race with the shutdown logic
        for (InternalTestCluster cluster : Arrays.asList(remoteCluster, internalCluster())) {
            for (AuditTrailService auditTrailService : cluster.getInstances(AuditTrailService.class)) {
                Optional<AuditTrail> auditTrail = auditTrailService.getAuditTrails().stream()
                        .filter(t -> t.name().equals(IndexAuditTrail.NAME)).findAny();
                if (cluster == remoteCluster || (localAudit && outputs.contains("index"))) {
                    // remote cluster must be present and only if we do local audit and output to an index we are good on the local one
                    // as well.
                    assertTrue(auditTrail.isPresent());
                }
                if (auditTrail.isPresent()) {
                    IndexAuditTrail indexAuditTrail = (IndexAuditTrail) auditTrail.get();
                    assertBusy(() -> assertSame("trail not started remoteCluster: " + (remoteCluster == cluster),
                            indexAuditTrail.state(), IndexAuditTrail.State.STARTED));
                }
            }
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.client.SecurityClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests security with tribe nodes
 */
public class SecurityTribeIT extends NativeRealmIntegTestCase {

    private static final String SECOND_CLUSTER_NODE_PREFIX = "node_cluster2_";
    private static InternalTestCluster cluster2;
    private static boolean useGeneratedSSL;

    private Node tribeNode;
    private Client tribeClient;

    @BeforeClass
    public static void setupSSL() {
        useGeneratedSSL = randomBoolean();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (cluster2 == null) {
            SecuritySettingsSource cluster2SettingsSource =
                    new SecuritySettingsSource(defaultMaxNumberOfNodes(), useGeneratedSSL, systemKey(), createTempDir(), Scope.SUITE);
            cluster2 = new InternalTestCluster(randomLong(), createTempDir(), true, true, 1, 2,
                    UUIDs.randomBase64UUID(random()), cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(),
                    getClientWrapper());
            cluster2.beforeTest(random(), 0.1);
            cluster2.ensureAtLeastNumDataNodes(2);
        }
    }

    @Override
    public boolean useGeneratedSSLConfig() {
        return useGeneratedSSL;
    }

    @AfterClass
    public static void tearDownSecondCluster() {
        if (cluster2 != null) {
            try {
                cluster2.close();
            } finally {
                cluster2 = null;
            }
        }
    }

    /**
     * We intentionally do not override {@link ESIntegTestCase#tearDown()} as doing so causes the ensure cluster size check to timeout
     */
    @After
    public void tearDownTribeNodeAndWipeCluster() throws Exception {
        if (cluster2 != null) {
            try {
                cluster2.wipe(Collections.<String>emptySet());
                try {
                    // this is a hack to clean up the .security index since only the XPack user or superusers can delete it
                    cluster2.getInstance(InternalClient.class)
                            .admin().indices().prepareDelete(SecurityTemplateService.SECURITY_INDEX_NAME).get();
                } catch (IndexNotFoundException e) {
                    // ignore it since not all tests create this index...
                }

                // Clear the realm cache for all realms since we use a SUITE scoped cluster
                SecurityClient client = securityClient(cluster2.client());
                client.prepareClearRealmCache().get();
            } finally {
                cluster2.afterTest();
            }
        }

        if (tribeNode != null) {
            tribeNode.close();
            tribeNode = null;
        }
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    private void setupTribeNode(Settings settings) throws NodeValidationException, InterruptedException {
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(1, useGeneratedSSL, systemKey(), createTempDir(), Scope.TEST);
        Map<String,String> asMap = new HashMap<>(cluster2SettingsSource.nodeSettings(0).getAsMap());
        asMap.remove(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey());
        Settings.Builder tribe1Defaults = Settings.builder();
        Settings.Builder tribe2Defaults = Settings.builder();
        for (Map.Entry<String, String> entry : asMap.entrySet()) {
            if (entry.getKey().startsWith("path.")) {
                continue;
            } else if (entry.getKey().equals("transport.tcp.port")) {
                continue;
            }
            tribe1Defaults.put("tribe.t1." + entry.getKey(), entry.getValue());
            tribe2Defaults.put("tribe.t2." + entry.getKey(), entry.getValue());
        }

        Settings merged = Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(asMap)
                .put("tribe.t1.cluster.name", internalCluster().getClusterName())
                .put("tribe.t2.cluster.name", cluster2.getClusterName())
                .put("tribe.blocks.write", false)
                .put("tribe.on_conflict", "prefer_t1")
                .put(tribe1Defaults.build())
                .put(tribe2Defaults.build())
                .put(settings)
                .put("node.name", "tribe_node") // make sure we can identify threads from this node
                .build();

        final List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>(nodePlugins());
        classpathPlugins.addAll(getMockPlugins());
        tribeNode = new MockNode(merged, classpathPlugins).start();
        tribeClient = getClientWrapper().apply(tribeNode.client());
        ClusterService tribeClusterService = tribeNode.injector().getInstance(ClusterService.class);
        ClusterState clusterState = tribeClusterService.state();
        ClusterStateObserver observer = new ClusterStateObserver(clusterState, tribeClusterService, null,
                logger, new ThreadContext(settings));
        final int cluster1Nodes = internalCluster().size();
        final int cluster2Nodes = cluster2.size();
        logger.info("waiting for [{}] nodes to be added to the tribe cluster state", cluster1Nodes + cluster2Nodes + 2);
        final Predicate<ClusterState> nodeCountPredicate = state -> state.nodes().getSize() == cluster1Nodes + cluster2Nodes + 3;
        if (nodeCountPredicate.test(clusterState) == false) {
            CountDownLatch latch = new CountDownLatch(1);
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    latch.countDown();
                }

                @Override
                public void onClusterServiceClose() {
                    fail("tribe cluster service closed");
                    latch.countDown();
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    fail("timed out waiting for nodes to be added to tribe's cluster state");
                    latch.countDown();
                }
            }, nodeCountPredicate);
            latch.await();
        }
    }

    public void testThatTribeCanAuthenticateElasticUser() throws Exception {
        setupTribeNode(Settings.EMPTY);
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecuredString("changeme".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeCanAuthenticateElasticUserWithChangedPassword() throws Exception {
        setupTribeNode(Settings.EMPTY);
        Client clusterClient = randomBoolean() ? client() : cluster2.client();
        securityClient(clusterClient).prepareChangePassword("elastic", "password".toCharArray()).get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecuredString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeClustersHaveDifferentPasswords() throws Exception {
        setupTribeNode(Settings.EMPTY);
        securityClient().prepareChangePassword("elastic", "password".toCharArray()).get();
        securityClient(cluster2.client()).prepareChangePassword("elastic", "password2".toCharArray()).get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecuredString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testUsersInBothTribes() throws Exception {
        final String preferredTribe = randomBoolean() ? "t1" : "t2";
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomUsers = scaledRandomIntBetween(3, 8);
        final Client cluster1Client = client();
        final Client cluster2Client = cluster2.client();
        List<String> shouldBeSuccessfulUsers = new ArrayList<>();
        List<String> shouldFailUsers = new ArrayList<>();
        final Client preferredClient = "t1".equals(preferredTribe) ? cluster1Client : cluster2Client;
        // always ensure the index exists on all of the clusters in this test
        assertAcked(internalClient().admin().indices().prepareCreate(SecurityTemplateService.SECURITY_INDEX_NAME).get());
        assertAcked(cluster2.getInstance(InternalClient.class).admin().indices()
                .prepareCreate(SecurityTemplateService.SECURITY_INDEX_NAME).get());
        for (int i = 0; i < randomUsers; i++) {
            final String username = "user" + i;
            Client clusterClient = randomBoolean() ? cluster1Client : cluster2Client;

            PutUserResponse response =
                    securityClient(clusterClient).preparePutUser(username, "password".toCharArray(), "superuser").get();
            assertTrue(response.created());

            // if it was the first client, we should expect authentication to succeed
            if (preferredClient == clusterClient) {
                shouldBeSuccessfulUsers.add(username);
            } else {
                shouldFailUsers.add(username);
            }
        }

        assertTribeNodeHasAllIndices();
        for (String username : shouldBeSuccessfulUsers) {
            ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString("password".toCharArray()))))
                    .admin().cluster().prepareHealth().get();
            assertNoTimeout(response);
        }

        for (String username : shouldFailUsers) {
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
                    tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                            UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString("password".toCharArray()))))
                            .admin().cluster().prepareHealth().get());
            assertThat(e.getMessage(), containsString("authenticate"));
        }
    }

    public void testUsersInNonPreferredClusterOnly() throws Exception {
        final String preferredTribe = randomBoolean() ? "t1" : "t2";
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomUsers = scaledRandomIntBetween(3, 8);
        final Client cluster1Client = client();
        final Client cluster2Client = cluster2.client();
        List<String> shouldBeSuccessfulUsers = new ArrayList<>();

        // only create users in the non preferred client
        final Client nonPreferredClient = "t1".equals(preferredTribe) ? cluster2Client : cluster1Client;
        for (int i = 0; i < randomUsers; i++) {
            final String username = "user" + i;
            PutUserResponse response =
                    securityClient(nonPreferredClient).preparePutUser(username, "password".toCharArray(), "superuser").get();
            assertTrue(response.created());
                shouldBeSuccessfulUsers.add(username);
        }

        assertTribeNodeHasAllIndices();
        for (String username : shouldBeSuccessfulUsers) {
            ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(username, new SecuredString("password".toCharArray()))))
                    .admin().cluster().prepareHealth().get();
            assertNoTimeout(response);
        }
    }

    public void testUserModificationUsingTribeNodeAreDisabled() throws Exception {
        setupTribeNode(Settings.EMPTY);
        SecurityClient securityClient = securityClient(tribeClient);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.preparePutUser("joe", "password".toCharArray()).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareSetEnabled("elastic", randomBoolean()).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.prepareChangePassword("elastic", "password".toCharArray()).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareDeleteUser("joe").get());
        assertThat(e.getMessage(), containsString("users may not be deleted using a tribe node"));
    }

    public void testRetrieveRolesOnTribeNode() throws Exception {
        final String preferredTribe = randomBoolean() ? "t1" : "t2";
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomRoles = scaledRandomIntBetween(3, 8);
        final Client cluster1Client = client();
        final Client cluster2Client = cluster2.client();
        List<String> shouldBeSuccessfulRoles = new ArrayList<>();
        List<String> shouldFailRoles = new ArrayList<>();
        final Client preferredClient = "t1".equals(preferredTribe) ? cluster1Client : cluster2Client;
        // always ensure the index exists on all of the clusters in this test
        assertAcked(internalClient().admin().indices().prepareCreate(SecurityTemplateService.SECURITY_INDEX_NAME).get());
        assertAcked(cluster2.getInstance(InternalClient.class).admin().indices()
                .prepareCreate(SecurityTemplateService.SECURITY_INDEX_NAME).get());

        for (int i = 0; i < randomRoles; i++) {
            final String rolename = "role" + i;
            Client clusterClient = randomBoolean() ? cluster1Client : cluster2Client;

            PutRoleResponse response = securityClient(clusterClient).preparePutRole(rolename).cluster("monitor").get();
            assertTrue(response.isCreated());

            // if it was the first client, we should expect authentication to succeed
            if (preferredClient == clusterClient) {
                shouldBeSuccessfulRoles.add(rolename);
            } else {
                shouldFailRoles.add(rolename);
            }
        }

        assertTribeNodeHasAllIndices();
        SecurityClient securityClient = securityClient(tribeClient);
        for (String rolename : shouldBeSuccessfulRoles) {
            GetRolesResponse response = securityClient.prepareGetRoles(rolename).get();
            assertTrue(response.hasRoles());
            assertEquals(1, response.roles().length);
            assertThat(response.roles()[0].getClusterPrivileges(), arrayContaining("monitor"));
        }

        for (String rolename : shouldFailRoles) {
            GetRolesResponse response = securityClient.prepareGetRoles(rolename).get();
            assertFalse(response.hasRoles());
        }
    }

    public void testRetrieveRolesOnNonPreferredClusterOnly() throws Exception {
        final String preferredTribe = randomBoolean() ? "t1" : "t2";
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomRoles = scaledRandomIntBetween(3, 8);
        final Client cluster1Client = client();
        final Client cluster2Client = cluster2.client();
        List<String> shouldBeSuccessfulRoles = new ArrayList<>();
        final Client nonPreferredClient = "t1".equals(preferredTribe) ? cluster2Client : cluster1Client;

        for (int i = 0; i < randomRoles; i++) {
            final String rolename = "role" + i;
            PutRoleResponse response = securityClient(nonPreferredClient).preparePutRole(rolename).cluster("monitor").get();
            assertTrue(response.isCreated());
            shouldBeSuccessfulRoles.add(rolename);
        }

        assertTribeNodeHasAllIndices();
        SecurityClient securityClient = securityClient(tribeClient);
        for (String rolename : shouldBeSuccessfulRoles) {
            GetRolesResponse response = securityClient.prepareGetRoles(rolename).get();
            assertTrue(response.hasRoles());
            assertEquals(1, response.roles().length);
            assertThat(response.roles()[0].getClusterPrivileges(), arrayContaining("monitor"));
        }
    }

    public void testRoleModificationUsingTribeNodeAreDisabled() throws Exception {
        setupTribeNode(Settings.EMPTY);
        SecurityClient securityClient = securityClient(getClientWrapper().apply(tribeClient));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.preparePutRole("role").cluster("all").get());
        assertThat(e.getMessage(), containsString("roles may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareDeleteRole("role").get());
        assertThat(e.getMessage(), containsString("roles may not be deleted using a tribe node"));
    }

    private void assertTribeNodeHasAllIndices() throws Exception {
        assertBusy(() -> {
            Set<String> indices = new HashSet<>();
            client().admin().cluster().prepareState().setMetaData(true).get()
                    .getState().getMetaData().getIndices().keysIt().forEachRemaining(indices::add);
            cluster2.client().admin().cluster().prepareState().setMetaData(true).get()
                    .getState().getMetaData().getIndices().keysIt().forEachRemaining(indices::add);

            ClusterState state = tribeClient.admin().cluster().prepareState().setRoutingTable(true).setMetaData(true).get().getState();
            assertThat(state.getMetaData().getIndices().size(), equalTo(indices.size()));
            for (String index : indices) {
                assertTrue(state.getMetaData().hasIndex(index));
                assertTrue(state.getRoutingTable().hasIndex(index));
                assertTrue(state.getRoutingTable().index(index).allPrimaryShardsActive());
            }
        });
    }
}

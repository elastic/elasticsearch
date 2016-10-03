/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
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
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests security with tribe nodes
 */
public class SecurityTribeIT extends NativeRealmIntegTestCase {

    private static final String SECOND_CLUSTER_NODE_PREFIX = "node_cluster2_";
    private static InternalTestCluster cluster2;
    private static boolean useSSL;

    private Node tribeNode;
    private Client tribeClient;

    @BeforeClass
    public static void setupSSL() {
        useSSL = randomBoolean();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (cluster2 == null) {
            SecuritySettingsSource cluster2SettingsSource =
                    new SecuritySettingsSource(defaultMaxNumberOfNodes(), useSSL, systemKey(), createTempDir(), Scope.SUITE);
            cluster2 = new InternalTestCluster(randomLong(), createTempDir(), true, 1, 2,
                    UUIDs.randomBase64UUID(random()), cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(),
                    getClientWrapper());
            cluster2.beforeTest(random(), 0.1);
            cluster2.ensureAtLeastNumDataNodes(2);
        }
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
    public boolean sslTransportEnabled() {
        return useSSL;
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    private void setupTribeNode(Settings settings) throws NodeValidationException {
        SecuritySettingsSource cluster2SettingsSource = new SecuritySettingsSource(1, useSSL, systemKey(), createTempDir(), Scope.TEST);
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
        // give each tribe it's unicast hosts to connect to
        tribe1Defaults.putArray("tribe.t1." + UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(),
                getUnicastHosts(internalCluster().client()));
        tribe2Defaults.putArray("tribe.t2." + UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey(),
                getUnicastHosts(cluster2.client()));

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

        tribeNode = new MockNode(merged, nodePlugins()).start();
        tribeClient = tribeNode.client();
    }

    private String[] getUnicastHosts(Client client) {
        ArrayList<String> unicastHosts = new ArrayList<>();
        NodesInfoResponse nodeInfos = client.admin().cluster().prepareNodesInfo().clear().setTransport(true).get();
        for (NodeInfo info : nodeInfos.getNodes()) {
            TransportAddress address = info.getTransport().getAddress().publishAddress();
            unicastHosts.add(address.getAddress() + ":" + address.getPort());
        }
        return unicastHosts.toArray(new String[unicastHosts.size()]);
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

        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecuredString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeClustersHaveDifferentPasswords() throws Exception {
        setupTribeNode(Settings.EMPTY);
        securityClient().prepareChangePassword("elastic", "password".toCharArray()).get();
        securityClient(cluster2.client()).prepareChangePassword("elastic", "password2".toCharArray()).get();

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

    public void testUserModificationUsingTribeNodeAreDisabled() throws Exception {
        setupTribeNode(Settings.EMPTY);
        SecurityClient securityClient = securityClient(getClientWrapper().apply(tribeClient));
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

        SecurityClient securityClient = securityClient(getClientWrapper().apply(tribeClient));
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

    public void testRoleModificationUsingTribeNodeAreDisabled() throws Exception {
        setupTribeNode(Settings.EMPTY);
        SecurityClient securityClient = securityClient(getClientWrapper().apply(tribeClient));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.preparePutRole("role").cluster("all").get());
        assertThat(e.getMessage(), containsString("roles may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareDeleteRole("role").get());
        assertThat(e.getMessage(), containsString("roles may not be deleted using a tribe node"));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.tribe.TribePlugin;
import org.elasticsearch.tribe.TribeService;
import org.elasticsearch.xpack.core.security.action.role.GetRolesResponse;
import org.elasticsearch.xpack.core.security.action.role.PutRoleResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.SECURITY_TEMPLATE_NAME;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests security with tribe nodes
 */
public class SecurityTribeTests extends NativeRealmIntegTestCase {

    private static final String SECOND_CLUSTER_NODE_PREFIX = "node_cluster2_";
    private static InternalTestCluster cluster2;
    private static boolean useSSL;
    private static Hasher hasher;

    @BeforeClass
    public static void setHasher() {
        hasher = Hasher.resolve("BCRYPT");
    }


    private Node tribeNode;
    private Client tribeClient;

    @BeforeClass
    public static void setupSSL() {
        useSSL = randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        return builder.build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (cluster2 == null) {
            SecuritySettingsSource cluster2SettingsSource =
                    new SecuritySettingsSource(useSSL, createTempDir(), Scope.SUITE) {
                        @Override
                        public Settings nodeSettings(int nodeOrdinal) {
                            Settings.Builder builder = Settings.builder()
                                    .put(super.nodeSettings(nodeOrdinal))
                                    .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                                    .put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");

                            if (builder.getSecureSettings() == null) {
                                builder.setSecureSettings(new MockSecureSettings());
                            }
                            ((MockSecureSettings) builder.getSecureSettings()).setString("bootstrap.password",
                                    BOOTSTRAP_PASSWORD.toString());
                            return builder.build();
                        }

                        @Override
                        public Collection<Class<? extends Plugin>> nodePlugins() {
                            ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
                            plugins.add(MockTribePlugin.class);
                            plugins.add(TribeAwareTestZenDiscoveryPlugin.class);
                            return plugins;
                        }
                    };

            cluster2 = new InternalTestCluster(randomLong(), createTempDir(), true, true, 1, 2,
                    UUIDs.randomBase64UUID(random()), cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(),
                    getClientWrapper());
            cluster2.beforeTest(random(), 0.1);
            cluster2.ensureAtLeastNumDataNodes(2);
        }
        assertSecurityIndexActive(cluster2);
    }

    @Override
    public boolean transportSSLEnabled() {
        return useSSL;
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
                cluster2.wipe(Collections.singleton(SECURITY_TEMPLATE_NAME));
                try {
                    // this is a hack to clean up the .security index since only the XPackSecurity user or superusers can delete it
                    final Client cluster2Client = cluster2.client().filterWithHeader(Collections.singletonMap("Authorization",
                            UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_SUPERUSER,
                                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
                    cluster2Client.admin().indices().prepareDelete(SecurityIndexManager.INTERNAL_SECURITY_INDEX).get();
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

    @Override
    protected boolean shouldSetReservedUserPasswords() {
        return false;
    }

    @Override
    protected boolean addTestZenDiscovery() {
        return false;
    }

    public static class TribeAwareTestZenDiscoveryPlugin extends TestZenDiscovery.TestPlugin {

        public TribeAwareTestZenDiscoveryPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public Settings additionalSettings() {
            if (settings.getGroups("tribe", true).isEmpty()) {
                return super.additionalSettings();
            } else {
                return Settings.EMPTY;
            }
        }
    }

    public static class MockTribePlugin extends TribePlugin {

        public MockTribePlugin(Settings settings) {
            super(settings);
        }

        protected Function<Settings, Node> nodeBuilder(Path configPath) {
            return settings -> new MockNode(new Environment(settings, configPath), internalCluster().getPlugins());
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTribePlugin.class);
        plugins.add(TribeAwareTestZenDiscoveryPlugin.class);
        return plugins;
    }

    private void setupTribeNode(Settings settings) throws Exception {
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(useSSL, createTempDir(), Scope.TEST) {
                    @Override
                    public Settings nodeSettings(int nodeOrdinal) {
                        return Settings.builder()
                            .put(super.nodeSettings(nodeOrdinal))
                            .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                            .build();
                    }
                };
        final Settings settingsTemplate = cluster2SettingsSource.nodeSettings(0);
        Settings.Builder tribe1Defaults = Settings.builder();
        Settings.Builder tribe2Defaults = Settings.builder();
        Settings tribeSettings = settingsTemplate.filter(k -> {
            if (k.equals(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey())) {
                return false;
            } if (k.startsWith("path.")) {
                return false;
            } else if (k.equals("transport.tcp.port")) {
                return false;
            } else if (k.startsWith("xpack.security.transport.ssl.")) {
                return false;
            }
            return true;
        });
        if (useSSL) {
            Settings.Builder addSSLBuilder = Settings.builder().put(tribeSettings, false);
            SecuritySettingsSource.addSSLSettingsForPEMFiles(addSSLBuilder,
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem",
                "testnode",
                "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt",
                Arrays.asList("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode-client-profile.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/active-directory-ca.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/openldap.crt",
                    "/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"));
            addSSLBuilder.put("xpack.security.transport.ssl.enabled", true);
            tribeSettings = addSSLBuilder.build();
        }
        tribe1Defaults.put(tribeSettings, false);
        tribe1Defaults.normalizePrefix("tribe.t1.");
        tribe2Defaults.put(tribeSettings, false);
        tribe2Defaults.normalizePrefix("tribe.t2.");
        // TODO: rethink how these settings are generated for tribes once we support more than just string settings...
        MockSecureSettings secureSettingsTemplate =
            (MockSecureSettings) Settings.builder().put(settingsTemplate).getSecureSettings();
        MockSecureSettings secureSettings = new MockSecureSettings();
        if (secureSettingsTemplate != null) {
            for (String settingName : secureSettingsTemplate.getSettingNames()) {
                if (settingName.startsWith("xpack.security.transport.ssl.")) {
                    continue;
                }
                String settingValue = secureSettingsTemplate.getString(settingName).toString();
                secureSettings.setString(settingName, settingValue);
                secureSettings.setString("tribe.t1." + settingName, settingValue);
                secureSettings.setString("tribe.t2." + settingName, settingValue);
            }
        }

        Settings merged = Settings.builder()
                .put(internalCluster().getDefaultSettings())
                .put(tribeSettings, false)
                .put("tribe.t1.cluster.name", internalCluster().getClusterName())
                .put("tribe.t1.transport.tcp.port", 0)
                .put("tribe.t2.cluster.name", cluster2.getClusterName())
                .put("tribe.t2.transport.tcp.port", 0)
                .put("tribe.blocks.write", false)
                .put("tribe.on_conflict", "prefer_t1")
                .put(tribe1Defaults.build())
                .put(tribe2Defaults.build())
                .put(settings)
                .put("node.name", "tribe_node") // make sure we can identify threads from this node
                .setSecureSettings(secureSettings)
                .build();
        final List<Class<? extends Plugin>> classpathPlugins = new ArrayList<>(nodePlugins());
        classpathPlugins.addAll(getMockPlugins());
        tribeNode = new MockNode(merged, classpathPlugins, cluster2SettingsSource.nodeConfigPath(0)).start();
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

        assertTribeNodeHasAllIndices();
    }

    public void testThatTribeCanAuthenticateElasticUser() throws Exception {
        ensureElasticPasswordBootstrapped(internalCluster());
        setupTribeNode(Settings.EMPTY);
        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", getReservedPassword())))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeCanAuthenticateElasticUserWithChangedPassword() throws Exception {
        InternalTestCluster cluster = randomBoolean() ? internalCluster() : cluster2;
        ensureElasticPasswordBootstrapped(cluster);
        setupTribeNode(Settings.EMPTY);
        securityClient(cluster.client()).prepareChangePassword("elastic", "password".toCharArray(), hasher).get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecureString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testThatTribeClustersHaveDifferentPasswords() throws Exception {
        ensureElasticPasswordBootstrapped(internalCluster());
        ensureElasticPasswordBootstrapped(cluster2);
        setupTribeNode(Settings.EMPTY);
        securityClient().prepareChangePassword("elastic", "password".toCharArray(), hasher).get();
        securityClient(cluster2.client()).prepareChangePassword("elastic", "password2".toCharArray(), hasher)
            .get();

        assertTribeNodeHasAllIndices();
        ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue("elastic", new SecureString("password".toCharArray()))))
                .admin().cluster().prepareHealth().get();
        assertNoTimeout(response);
    }

    public void testUsersInBothTribes() throws Exception {
        ensureElasticPasswordBootstrapped(internalCluster());
        ensureElasticPasswordBootstrapped(cluster2);

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

            PutUserResponse response = securityClient(clusterClient)
                .preparePutUser(username, "password".toCharArray(), hasher, "superuser").get();
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
                    UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString("password".toCharArray()))))
                    .admin().cluster().prepareHealth().get();
            assertNoTimeout(response);
        }

        for (String username : shouldFailUsers) {
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () ->
                    tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                            UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString("password".toCharArray()))))
                            .admin().cluster().prepareHealth().get());
            assertThat(e.getMessage(), containsString("authenticate"));
        }
    }

    public void testUsersInNonPreferredClusterOnly() throws Exception {
        final String preferredTribe = randomBoolean() ? "t1" : "t2";
        // only create users in the non preferred client
        final InternalTestCluster nonPreferredCluster = "t1".equals(preferredTribe) ? cluster2 : internalCluster();
        ensureElasticPasswordBootstrapped(nonPreferredCluster);
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomUsers = scaledRandomIntBetween(3, 8);

        List<String> shouldBeSuccessfulUsers = new ArrayList<>();


        for (int i = 0; i < randomUsers; i++) {
            final String username = "user" + i;
            PutUserResponse response = securityClient(nonPreferredCluster.client())
                .preparePutUser(username, "password".toCharArray(), hasher, "superuser").get();
            assertTrue(response.created());
            shouldBeSuccessfulUsers.add(username);
        }

        assertTribeNodeHasAllIndices();
        for (String username : shouldBeSuccessfulUsers) {
            ClusterHealthResponse response = tribeClient.filterWithHeader(Collections.singletonMap("Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString("password".toCharArray()))))
                    .admin().cluster().prepareHealth().get();
            assertNoTimeout(response);
        }
    }

    private void ensureElasticPasswordBootstrapped(InternalTestCluster cluster) {
        NodesInfoResponse nodesInfoResponse = cluster.client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        try (RestClient restClient = createRestClient(nodesInfoResponse.getNodes(), null, "http")) {
            setupReservedPasswords(restClient);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testUserModificationUsingTribeNodeAreDisabled() throws Exception {
        ensureElasticPasswordBootstrapped(internalCluster());

        setupTribeNode(Settings.EMPTY);
        SecurityClient securityClient = securityClient(tribeClient);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.preparePutUser("joe", "password".toCharArray(), hasher).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareSetEnabled("elastic", randomBoolean()).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class,
                () -> securityClient.prepareChangePassword("elastic", "password".toCharArray(), hasher).get());
        assertThat(e.getMessage(), containsString("users may not be created or modified using a tribe node"));
        e = expectThrows(UnsupportedOperationException.class, () -> securityClient.prepareDeleteUser("joe").get());
        assertThat(e.getMessage(), containsString("users may not be deleted using a tribe node"));
    }

    public void testRetrieveRolesOnTribeNode() throws Exception {
        ensureElasticPasswordBootstrapped(internalCluster());
        ensureElasticPasswordBootstrapped(cluster2);

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
        final InternalTestCluster nonPreferredCluster = "t1".equals(preferredTribe) ? cluster2 : internalCluster();
        ensureElasticPasswordBootstrapped(nonPreferredCluster);
        setupTribeNode(Settings.builder().put("tribe.on_conflict", "prefer_" + preferredTribe).build());
        final int randomRoles = scaledRandomIntBetween(3, 8);
        List<String> shouldBeSuccessfulRoles = new ArrayList<>();

        Client nonPreferredClient = nonPreferredCluster.client();

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

    public void testTribeSettingNames() throws Exception {
        TribeService.TRIBE_SETTING_KEYS
                .forEach(s -> assertThat("a new setting has been introduced for tribe that security needs to know about in Security.java",
                        s, anyOf(startsWith("tribe.blocks"), startsWith("tribe.name"), startsWith("tribe.on_conflict"))));
    }

    public void testNoTribeSecureSettings() throws Exception {
        MockSecureSettings secureSettings = new MockSecureSettings();
        Path home = createTempDir();
        secureSettings.setString("xpack.security.http.ssl.keystore.secure_password", "dummypass");
        secureSettings.setString("xpack.security.authc.token.passphrase", "dummypass");
        Settings settings = Settings.builder().setSecureSettings(secureSettings)
            .put("path.home", home)
            .put("tribe.t1.cluster.name", "foo")
            .put("xpack.security.enabled", true).build();
        Security security = new Security(settings, home.resolve("config"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, security::additionalSettings);
        // can't rely on order of the strings printed in the exception message
        assertThat(e.getMessage(), containsString("xpack.security.http.ssl.keystore.secure_password"));
        assertThat(e.getMessage(), containsString("xpack.security.authc.token.passphrase"));
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.hasItem;

/**
 * Base class to run tests against a cluster with X-Pack installed and security enabled.
 * The default {@link org.elasticsearch.test.ESIntegTestCase.Scope} is {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE}
 *
 * @see SecuritySettingsSource
 */
public abstract class SecurityIntegTestCase extends ESIntegTestCase {

    private static SecuritySettingsSource SECURITY_DEFAULT_SETTINGS;
    protected static SecureString BOOTSTRAP_PASSWORD = null;

    /**
     * Settings used when the {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     * so that some of the configuration parameters can be overridden through test instance methods, similarly
     * to how {@link ESIntegTestCase#nodeSettings(int, Settings)} works.
     */
    private static CustomSecuritySettingsSource customSecuritySettingsSource = null;
    private TestSecurityClient securityClient;

    @BeforeClass
    public static void generateBootstrapPassword() {
        BOOTSTRAP_PASSWORD = TEST_PASSWORD_SECURE_STRING.clone();
    }

    // UnicastZen requires the number of nodes in a cluster to generate the unicast configuration.
    // The number of nodes is randomized though, but we can predict what the maximum number of nodes will be
    // and configure them all in unicast.hosts
    protected static int defaultMaxNumberOfNodes() {
        ClusterScope clusterScope = SecurityIntegTestCase.class.getAnnotation(ClusterScope.class);
        if (clusterScope == null) {
            return InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES + InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES
                + InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
        } else {
            int clientNodes = clusterScope.numClientNodes();
            if (clientNodes < 0) {
                clientNodes = InternalTestCluster.DEFAULT_MAX_NUM_CLIENT_NODES;
            }
            int masterNodes = 0;
            if (clusterScope.supportsDedicatedMasters()) {
                masterNodes = InternalTestCluster.DEFAULT_HIGH_NUM_MASTER_NODES;
            }

            int dataNodes = 0;
            if (clusterScope.numDataNodes() < 0) {
                if (clusterScope.maxNumDataNodes() < 0) {
                    dataNodes = InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES;
                } else {
                    dataNodes = clusterScope.maxNumDataNodes();
                }
            } else {
                dataNodes = clusterScope.numDataNodes();
            }
            return masterNodes + dataNodes + clientNodes;
        }
    }

    private static ClusterScope getAnnotation(Class<?> clazz) {
        if (clazz == Object.class || clazz == SecurityIntegTestCase.class) {
            return null;
        }
        ClusterScope annotation = clazz.getAnnotation(ClusterScope.class);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass());
    }

    Scope getCurrentClusterScope() {
        return getCurrentClusterScope(this.getClass());
    }

    private static Scope getCurrentClusterScope(Class<?> clazz) {
        ClusterScope annotation = getAnnotation(clazz);
        return annotation == null ? Scope.SUITE : annotation.scope();
    }

    @BeforeClass
    public static void initDefaultSettings() {
        if (SECURITY_DEFAULT_SETTINGS == null) {
            SECURITY_DEFAULT_SETTINGS = new SecuritySettingsSource(randomBoolean(), createTempDir(), Scope.SUITE);
        }
    }

    /**
     * Set the static default settings to null to prevent a memory leak. The test framework also checks for memory leaks
     * and computes the size, this can cause issues when running with the security manager as it tries to do reflection
     * into protected sun packages.
     */
    @AfterClass
    public static void destroyDefaultSettings() {
        SECURITY_DEFAULT_SETTINGS = null;
        customSecuritySettingsSource = null;
    }

    @Rule
    // Rules are the only way to have something run before the before (final) method inherited from ESIntegTestCase
    public ExternalResource externalResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            Scope currentClusterScope = getCurrentClusterScope();
            switch (currentClusterScope) {
                case SUITE:
                    if (customSecuritySettingsSource == null) {
                        customSecuritySettingsSource = new CustomSecuritySettingsSource(
                            transportSSLEnabled(),
                            createTempDir(),
                            currentClusterScope
                        );
                    }
                    break;
                case TEST:
                    customSecuritySettingsSource = new CustomSecuritySettingsSource(
                        transportSSLEnabled(),
                        createTempDir(),
                        currentClusterScope
                    );
                    break;
            }
        }
    };

    @Before
    // before methods from the superclass are run before this, which means that the current cluster is ready to go
    public void assertXPackIsInstalled() {
        doAssertXPackIsInstalled();
    }

    protected void doAssertXPackIsInstalled() {
        NodesInfoResponse nodeInfos = client().admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            // TODO: disable this assertion for now, due to random runs with mock plugins. perhaps run without mock plugins?
            // assertThat(nodeInfo.getPlugins().getInfos(), hasSize(2));
            Collection<String> pluginNames = nodeInfo.getInfo(PluginsAndModules.class)
                .getPluginInfos()
                .stream()
                .map(p -> p.descriptor().getClassname())
                .collect(Collectors.toList());
            assertThat(
                "plugin [" + xpackPluginClass().getName() + "] not found in [" + pluginNames + "]",
                pluginNames,
                hasItem(xpackPluginClass().getName())
            );
        }
    }

    protected Class<?> xpackPluginClass() {
        return LocalStateSecurity.class;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // Disable native ML autodetect_process as the c++ controller won't be available
        // builder.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        Settings customSettings = customSecuritySettingsSource.nodeSettings(nodeOrdinal, otherSettings);
        builder.put(customSettings, false); // handle secure settings separately
        builder.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        Settings.Builder customBuilder = Settings.builder().put(customSettings);
        if (customBuilder.getSecureSettings() != null) {
            SecuritySettingsSource.addSecureSettings(
                builder,
                secureSettings -> secureSettings.merge((MockSecureSettings) customBuilder.getSecureSettings())
            );
        }
        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        ((MockSecureSettings) builder.getSecureSettings()).setString("bootstrap.password", BOOTSTRAP_PASSWORD.toString());
        return builder.build();
    }

    @Override
    protected Path nodeConfigPath(int nodeOrdinal) {
        return customSecuritySettingsSource.nodeConfigPath(nodeOrdinal);
    }

    @Override
    protected boolean addMockTransportService() {
        return false; // security has its own transport service
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return customSecuritySettingsSource.nodePlugins();
    }

    /**
     * Allows to override the users config file when the {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     */
    protected String configUsers() {
        return SECURITY_DEFAULT_SETTINGS.configUsers();
    }

    /**
     * Allows to override the users_roles config file when the {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     */
    protected String configUsersRoles() {
        return SECURITY_DEFAULT_SETTINGS.configUsersRoles();
    }

    /**
     * Allows to override the roles config file when the {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     */
    protected String configRoles() {
        return SECURITY_DEFAULT_SETTINGS.configRoles();
    }

    protected String configOperatorUsers() {
        return SECURITY_DEFAULT_SETTINGS.configOperatorUsers();
    }

    /**
     * Allows to override the node client username (used while sending requests to the test cluster) when the
     * {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     */
    protected String nodeClientUsername() {
        return SECURITY_DEFAULT_SETTINGS.nodeClientUsername();
    }

    /**
     * Allows to override the node client password (used while sending requests to the test cluster) when the
     * {@link org.elasticsearch.test.ESIntegTestCase.ClusterScope} is set to
     * {@link org.elasticsearch.test.ESIntegTestCase.Scope#SUITE} or {@link org.elasticsearch.test.ESIntegTestCase.Scope#TEST}
     */
    protected SecureString nodeClientPassword() {
        return SECURITY_DEFAULT_SETTINGS.nodeClientPassword();
    }

    /**
     * Allows to control whether ssl key information is auto generated or not on the transport layer
     */
    protected boolean transportSSLEnabled() {
        return randomBoolean();
    }

    protected int maxNumberOfNodes() {
        return defaultMaxNumberOfNodes();
    }

    private class CustomSecuritySettingsSource extends SecuritySettingsSource {

        private CustomSecuritySettingsSource(boolean sslEnabled, Path configDir, Scope scope) {
            super(sslEnabled, configDir, scope);
        }

        @Override
        protected String configUsers() {
            return SecurityIntegTestCase.this.configUsers();
        }

        @Override
        protected String configUsersRoles() {
            return SecurityIntegTestCase.this.configUsersRoles();
        }

        @Override
        protected String configRoles() {
            return SecurityIntegTestCase.this.configRoles();
        }

        @Override
        protected String configOperatorUsers() {
            return SecurityIntegTestCase.this.configOperatorUsers();
        }

        @Override
        protected String nodeClientUsername() {
            return SecurityIntegTestCase.this.nodeClientUsername();
        }

        @Override
        protected SecureString nodeClientPassword() {
            return SecurityIntegTestCase.this.nodeClientPassword();
        }
    }

    /**
     * Creates the indices provided as argument, randomly associating them with aliases, indexes one dummy document per index
     * and refreshes the new indices
     */
    protected void createIndicesWithRandomAliases(String... indices) {
        createIndex(indices);

        if (frequently()) {
            boolean aliasAdded = false;
            IndicesAliasesRequestBuilder builder = indicesAdmin().prepareAliases();
            for (String index : indices) {
                if (frequently()) {
                    // one alias per index with prefix "alias-"
                    builder.addAlias(index, "alias-" + index);
                    aliasAdded = true;
                }
            }
            // If we get to this point and we haven't added an alias to the request we need to add one
            // or the request will fail so use noAliasAdded to force adding the alias in this case
            if (aliasAdded == false || randomBoolean()) {
                // one alias pointing to all indices
                for (String index : indices) {
                    builder.addAlias(index, "alias");
                }
            }
            assertAcked(builder);
        }

        for (String index : indices) {
            client().prepareIndex(index).setSource("field", "value").get();
        }
        refresh(indices);
    }

    @Override
    protected Function<Client, Client> getClientWrapper() {
        Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword())
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // return a node client
        return client -> (client instanceof NodeClient) ? client.filterWithHeader(headers) : client;
    }

    public void assertSecurityIndexActive() throws Exception {
        assertSecurityIndexActive(cluster());
    }

    public void assertSecurityIndexActive(TestCluster testCluster) throws Exception {
        for (Client client : testCluster.getClients()) {
            assertBusy(() -> {
                ClusterState clusterState = client.admin().cluster().prepareState().setLocal(true).get().getState();
                assertFalse(clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK));
                Index securityIndex = resolveSecurityIndex(clusterState.metadata());
                if (securityIndex != null) {
                    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(securityIndex);
                    if (indexRoutingTable != null) {
                        assertTrue(indexRoutingTable.allPrimaryShardsActive());
                    }
                }
            }, 30L, TimeUnit.SECONDS);
        }
    }

    protected void deleteSecurityIndex() {
        final Client client = client().filterWithHeader(
            Collections.singletonMap(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(
                    SecuritySettingsSource.ES_TEST_ROOT_USER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                )
            )
        );
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(SECURITY_MAIN_ALIAS);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).actionGet();
        if (getIndexResponse.getIndices().length > 0) {
            // this is a hack to clean up the .security index since only a superuser can delete it
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(getIndexResponse.getIndices());
            client.admin().indices().delete(deleteIndexRequest).actionGet();
        }
    }

    private static Index resolveSecurityIndex(Metadata metadata) {
        final IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(SECURITY_MAIN_ALIAS);
        if (indexAbstraction != null) {
            return indexAbstraction.getIndices().get(0);
        }
        return null;
    }

    public static Hasher getFastStoredHashAlgoForTests() {
        return inFipsJvm()
            ? Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch"))
            : Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch", "bcrypt", "bcrypt9"));
    }

    protected TestSecurityClient getSecurityClient(RequestOptions requestOptions) {
        return new TestSecurityClient(getRestClient(), requestOptions);
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = getSecurityClient(SecuritySettingsSource.SECURITY_REQUEST_OPTIONS);
        }
        return securityClient;
    }
}

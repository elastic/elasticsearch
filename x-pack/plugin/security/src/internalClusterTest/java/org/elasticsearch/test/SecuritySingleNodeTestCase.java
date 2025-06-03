/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.apache.http.HttpHost;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.getMigrationVersionFromIndexMetadata;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

/**
 * A test that starts a single node with security enabled. This test case allows for customization
 * of users and roles that the cluster starts with. If an integration test is needed to test but
 * multiple nodes are not needed, then this class should be favored over
 * {@link SecurityIntegTestCase} due to simplicity and improved speed from not needing to start
 * multiple nodes and wait for the cluster to form.
 */
public abstract class SecuritySingleNodeTestCase extends ESSingleNodeTestCase {

    private static SecuritySettingsSource SECURITY_DEFAULT_SETTINGS = null;
    private static CustomSecuritySettingsSource customSecuritySettingsSource = null;
    private TestSecurityClient securityClient;
    private static RestClient restClient = null;

    @BeforeClass
    public static void initDefaultSettings() {
        if (SECURITY_DEFAULT_SETTINGS == null) {
            SECURITY_DEFAULT_SETTINGS = new SecuritySettingsSource(randomBoolean(), createTempDir(), ESIntegTestCase.Scope.SUITE);
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
        tearDownRestClient();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        deleteSecurityIndexIfExists();
        createSecurityIndexWithWaitForActiveShards();
    }

    @Override
    public void tearDown() throws Exception {
        awaitSecurityMigration();
        super.tearDown();
        if (resetNodeAfterTest()) {
            tearDownRestClient();
        }
    }

    private boolean isMigrationComplete(ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().getProject().index(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7);
        if (indexMetadata == null) {
            // If index doesn't exist, no migration needed
            return true;
        }
        return getMigrationVersionFromIndexMetadata(indexMetadata) == SecurityMigrations.MIGRATIONS_BY_VERSION.lastKey();
    }

    protected void awaitSecurityMigration() {
        final var latch = new CountDownLatch(1);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.addListener((event) -> {
            if (isMigrationComplete(event.state())) {
                latch.countDown();
            }
        });
        if (isMigrationComplete(clusterService.state())) {
            latch.countDown();
        }
        safeAwait(latch);
    }

    private static void tearDownRestClient() {
        if (restClient != null) {
            IOUtils.closeWhileHandlingException(restClient);
            restClient = null;
        }
    }

    @Rule
    // Rules are the only way to have something run before the before (final) method inherited from ESSingleNodeTestCase
    public ExternalResource externalResource = new ExternalResource() {
        @Override
        protected void before() {
            if (customSecuritySettingsSource == null) {
                customSecuritySettingsSource = new CustomSecuritySettingsSource(
                    transportSSLEnabled(),
                    createTempDir(),
                    ESIntegTestCase.Scope.SUITE
                );
            }
        }
    };

    @Before
    // before methods from the superclass are run before this, which means that the current cluster is ready to go
    public void assertXPackIsInstalled() {
        doAssertXPackIsInstalled();
    }

    private void doAssertXPackIsInstalled() {
        NodesInfoResponse nodeInfos = clusterAdmin().prepareNodesInfo().clear().setPlugins(true).get();
        for (NodeInfo nodeInfo : nodeInfos.getNodes()) {
            // TODO: disable this assertion for now, due to random runs with mock plugins. perhaps run without mock plugins?
            // assertThat(nodeInfo.getInfo(PluginsAndModules.class).getInfos(), hasSize(2));
            Collection<String> pluginNames = nodeInfo.getInfo(PluginsAndModules.class)
                .getPluginInfos()
                .stream()
                .filter(p -> p.descriptor().isStable() == false)
                .map(p -> p.descriptor().getClassname())
                .collect(Collectors.toList());
            assertThat(
                "plugin [" + LocalStateSecurity.class.getName() + "] not found in [" + pluginNames + "]",
                pluginNames,
                hasItem(LocalStateSecurity.class.getName())
            );
        }
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        Settings customSettings = customSecuritySettingsSource.nodeSettings(0, Settings.EMPTY);
        builder.put(customSettings, false); // handle secure settings separately
        builder.put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        builder.put("transport.type", "security4");
        builder.put("path.home", customSecuritySettingsSource.homePath(0));
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
        SecureString boostrapPassword = getBootstrapPassword();
        if (boostrapPassword != null) {
            ((MockSecureSettings) builder.getSecureSettings()).setString("bootstrap.password", boostrapPassword.toString());
        }
        return builder.build();
    }

    protected SecureString getBootstrapPassword() {
        return TEST_PASSWORD_SECURE_STRING;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return customSecuritySettingsSource.nodePlugins();
    }

    /**
     * Allows to override the users config file
     */
    protected String configUsers() {
        return SECURITY_DEFAULT_SETTINGS.configUsers();
    }

    /**
     * Allows to override the users_roles config file
     */
    protected String configUsersRoles() {
        return SECURITY_DEFAULT_SETTINGS.configUsersRoles();
    }

    /**
     * Allows to override the roles config file
     */
    protected String configRoles() {
        return SECURITY_DEFAULT_SETTINGS.configRoles();
    }

    protected String configOperatorUsers() {
        return SECURITY_DEFAULT_SETTINGS.configOperatorUsers();
    }

    protected String configServiceTokens() {
        return SECURITY_DEFAULT_SETTINGS.configServiceTokens();
    }

    /**
     * Allows to override the node client username
     */
    protected String nodeClientUsername() {
        return SECURITY_DEFAULT_SETTINGS.nodeClientUsername();
    }

    /**
     * Allows to override the node client password (used while sending requests to the test node)
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

    private class CustomSecuritySettingsSource extends SecuritySettingsSource {

        private CustomSecuritySettingsSource(boolean sslEnabled, Path configDir, ESIntegTestCase.Scope scope) {
            super(sslEnabled, configDir, scope);
        }

        @Override
        protected String configUsers() {
            return SecuritySingleNodeTestCase.this.configUsers();
        }

        @Override
        protected String configUsersRoles() {
            return SecuritySingleNodeTestCase.this.configUsersRoles();
        }

        @Override
        protected String configRoles() {
            return SecuritySingleNodeTestCase.this.configRoles();
        }

        @Override
        protected String configOperatorUsers() {
            return SecuritySingleNodeTestCase.this.configOperatorUsers();
        }

        @Override
        protected String configServiceTokens() {
            return SecuritySingleNodeTestCase.this.configServiceTokens();
        }

        @Override
        protected String nodeClientUsername() {
            return SecuritySingleNodeTestCase.this.nodeClientUsername();
        }

        @Override
        protected SecureString nodeClientPassword() {
            return SecuritySingleNodeTestCase.this.nodeClientPassword();
        }
    }

    @Override
    public Client wrapClient(final Client client) {
        Map<String, String> headers = Collections.singletonMap(
            "Authorization",
            basicAuthHeaderValue(nodeClientUsername(), nodeClientPassword())
        );
        // we need to wrap node clients because we do not specify a user for nodes and all requests will use the system
        // user. This is ok for internal n2n stuff but the test framework does other things like wiping indices, repositories, etc
        // that the system user cannot do. so we wrap the node client with a user that can do these things since the client() calls
        // are all using a node client
        return client.filterWithHeader(headers);
    }

    protected boolean isTransportSSLEnabled() {
        return customSecuritySettingsSource.isSslEnabled();
    }

    /**
     * Returns an instance of {@link RestClient} pointing to the current node.
     * Creates a new client if the method is invoked for the first time in the context of the current test scope.
     * The returned client gets automatically closed when needed, it shouldn't be closed as part of tests otherwise
     * it cannot be reused by other tests anymore.
     * Requires that {@link org.elasticsearch.test.ESSingleNodeTestCase#addMockHttpTransport()} is overriden and set to false.
     */
    protected RestClient getRestClient() {
        return getRestClient(client());
    }

    protected RestClient createRestClient(RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback, String protocol) {
        return createRestClient(client(), httpClientConfigCallback, protocol);
    }

    protected static Hasher getFastStoredHashAlgoForTests() {
        return inFipsJvm()
            ? Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch"))
            : Hasher.resolve(randomFrom("pbkdf2", "pbkdf2_1000", "pbkdf2_stretch_1000", "pbkdf2_stretch", "bcrypt", "bcrypt9"));
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = getSecurityClient(SecuritySettingsSource.SECURITY_REQUEST_OPTIONS);
        }
        return securityClient;
    }

    protected TestSecurityClient getSecurityClient(RequestOptions requestOptions) {
        return new TestSecurityClient(getRestClient(), requestOptions);
    }

    private static synchronized RestClient getRestClient(Client client) {
        if (restClient == null) {
            restClient = createRestClient(client, null, "http");
        }
        return restClient;
    }

    private static RestClient createRestClient(
        Client client,
        RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback,
        String protocol
    ) {
        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        assertEquals(nodesInfoResponse.getNodes().size(), 1);
        NodeInfo node = nodesInfoResponse.getNodes().get(0);
        assertNotNull(node.getInfo(HttpInfo.class));
        TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
        InetSocketAddress address = publishAddress.address();
        final HttpHost host = new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), protocol);
        RestClientBuilder builder = RestClient.builder(host);
        if (httpClientConfigCallback != null) {
            builder.setHttpClientConfigCallback(httpClientConfigCallback);
        }
        return builder.build();
    }

    protected void deleteSecurityIndexIfExists() {
        // delete the security index, if it exist
        GetIndexRequest getIndexRequest = new GetIndexRequest(TEST_REQUEST_TIMEOUT);
        getIndexRequest.indices(SECURITY_MAIN_ALIAS);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = client().admin().indices().getIndex(getIndexRequest).actionGet();
        if (getIndexResponse.getIndices().length > 0) {
            assertThat(getIndexResponse.getIndices().length, is(1));
            assertThat(getIndexResponse.getIndices()[0], is(TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7));

            // Security migration needs to finish before deleting the index
            awaitSecurityMigration();
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(getIndexResponse.getIndices());
            assertAcked(client().admin().indices().delete(deleteIndexRequest).actionGet());
        }
    }

    protected void createSecurityIndexWithWaitForActiveShards() {
        final Client client = client().filterWithHeader(
            Collections.singletonMap(
                "Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(
                    SecuritySettingsSource.ES_TEST_ROOT_USER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
                )
            )
        );
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(SECURITY_MAIN_ALIAS).waitForActiveShards(ActiveShardCount.ALL)
            .masterNodeTimeout(TEST_REQUEST_TIMEOUT);
        try {
            client.admin().indices().create(createIndexRequest).actionGet();
        } catch (ResourceAlreadyExistsException e) {
            logger.info("Security index already exists, waiting for it to become available", e);
            ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, SECURITY_MAIN_ALIAS).waitForActiveShards(
                ActiveShardCount.ALL
            );
            ClusterHealthResponse healthResponse = client.admin().cluster().health(healthRequest).actionGet();
            assertThat(healthResponse.isTimedOut(), is(false));
        }
    }
}

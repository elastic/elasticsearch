/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit.index;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.zen.SettingsBasedHostsProvider;
import org.elasticsearch.http.HttpChannel;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.index.IndexAuditTrailField;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail.Message;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.DAILY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.HOURLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.MONTHLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.WEEKLY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ESIntegTestCase.ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class IndexAuditTrailTests extends SecurityIntegTestCase {
    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + SUITE_CLUSTER_NODE_PREFIX;

    private static boolean remoteIndexing;
    private static boolean useSSL;
    private static InternalTestCluster remoteCluster;
    private static Settings remoteSettings;
    private static int numShards = -1;
    private static int numReplicas = -1;

    private TransportAddress remoteAddress = buildNewFakeTransportAddress();
    private TransportAddress localAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 0);
    private IndexNameResolver.Rollover rollover;
    private IndexAuditTrail auditor;
    private SetOnce<Message> enqueuedMessage;
    private ThreadPool threadPool;
    private boolean includeRequestBody;

    @BeforeClass
    public static void configureBeforeClass() {
        useSSL = randomBoolean();
        remoteIndexing = randomBoolean();
        if (remoteIndexing == false) {
            remoteSettings = Settings.EMPTY;
        }
    }

    @AfterClass
    public static void cleanupAfterTest() {
        if (remoteCluster != null) {
            remoteCluster.close();
            remoteCluster = null;

        }
        remoteSettings = null;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return useSSL;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        if (numShards == -1) {
            numShards = numberOfShards();
        }
        if (numReplicas == -1) {
            numReplicas = numberOfReplicas();
        }

        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.security.audit.index.settings.index.number_of_shards", numShards)
                .put("xpack.security.audit.index.settings.index.number_of_replicas", numReplicas)
                .build();
    }

    @Before
    public void initializeRemoteClusterIfNecessary() throws Exception {
        if (remoteIndexing == false) {
            logger.info("--> remote indexing disabled.");
            return;
        }

        if (remoteCluster != null) {
            return;
        }

        // create another cluster
        String cluster2Name = clusterName(Scope.SUITE.name(), randomLong());

        // Setup a second test cluster with randomization for number of nodes, security enabled, and SSL
        final int numNodes = randomIntBetween(1, 2);
        final boolean useSecurity = randomBoolean();
        final boolean remoteUseSSL = useSecurity && useSSL;
        logger.info("--> remote indexing enabled. security enabled: [{}], SSL enabled: [{}], nodes: [{}]", useSecurity, useSSL,
                numNodes);
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(useSSL, createTempDir(), Scope.SUITE) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        .put(DiscoveryModule.DISCOVERY_HOSTS_PROVIDER_SETTING.getKey(), "file")
                        .putList(SettingsBasedHostsProvider.DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.getKey())
                        .put(TestZenDiscovery.USE_ZEN2.getKey(), getUseZen2())
                        .put("xpack.security.audit.index.settings.index.number_of_shards", numShards)
                        .put("xpack.security.audit.index.settings.index.number_of_replicas", numReplicas)
                        // Disable native ML autodetect_process as the c++ controller won't be available
//                        .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                        .put(XPackSettings.SECURITY_ENABLED.getKey(), useSecurity);
                String transport = builder.get(NetworkModule.TRANSPORT_TYPE_KEY);
                if (useSecurity == false && (transport == null || SecurityField.NAME4.equals(transport)
                    || SecurityField.NIO.equals(transport))) {
                    builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
                }
                return builder.build();
            }

            @Override
            public Settings transportClientSettings() {
                if (useSecurity) {
                    return super.transportClientSettings();
                } else {
                    Settings.Builder builder = Settings.builder()
                            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
                            .put(super.transportClientSettings());
                    if (builder.get(NetworkModule.TRANSPORT_TYPE_KEY) == null) {
                        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
                    }
                    return builder.build();
                }
            }

            @Override
            protected void addDefaultSecurityTransportType(Settings.Builder builder, Settings settings) {
                if (useSecurity) {
                    super.addDefaultSecurityTransportType(builder, settings);
                }
            }
        };


        Set<Class<? extends Plugin>> mockPlugins = new HashSet<>(getMockPlugins());
        if (useSecurity == false) {
            mockPlugins.add(getTestTransportPlugin());
        }
        remoteCluster = new InternalTestCluster(randomLong(), createTempDir(), false, true, numNodes, numNodes, cluster2Name,
                cluster2SettingsSource, 0, SECOND_CLUSTER_NODE_PREFIX, mockPlugins,
                useSecurity ? getClientWrapper() : Function.identity());
        remoteCluster.beforeTest(random(), 0.5);

        NodesInfoResponse response = remoteCluster.client().admin().cluster().prepareNodesInfo().execute().actionGet();
        TransportInfo info = response.getNodes().get(0).getTransport();
        TransportAddress inet = info.address().publishAddress();

        Settings.Builder builder = Settings.builder()
                .put("xpack.security.audit.index.client." + XPackSettings.SECURITY_ENABLED.getKey(), useSecurity)
                .put(remoteSettings(NetworkAddress.format(inet.address().getAddress()), inet.address().getPort(), cluster2Name))
                .put("xpack.security.audit.index.client.xpack.security.user", SecuritySettingsSource.TEST_USER_NAME + ":" +
                        SecuritySettingsSourceField.TEST_PASSWORD);

        if (remoteUseSSL) {
            cluster2SettingsSource.addClientSSLSettings(builder, "xpack.security.audit.index.client.");
            builder.put("xpack.security.audit.index.client.xpack.security.transport.ssl.enabled", true);
        }
        if (useSecurity == false && builder.get(NetworkModule.TRANSPORT_TYPE_KEY) == null) {
            builder.put("xpack.security.audit.index.client." + NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        }
        remoteSettings = builder.build();
    }

    @After
    public void afterTest() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
        if (auditor != null) {
            auditor.stop();
        }

        if (remoteCluster != null) {
            remoteCluster.wipe(excludeTemplates());
        }
    }

    @Override
    protected Set<String> excludeTemplates() {
        return Sets.newHashSet(SecurityIndexManager.SECURITY_TEMPLATE_NAME, IndexAuditTrail.INDEX_TEMPLATE_NAME);
    }

    @Override
    protected int maximumNumberOfShards() {
        return 3;
    }

    private Settings commonSettings(IndexNameResolver.Rollover rollover) {
        return Settings.builder()
                .put("xpack.security.audit.enabled", true)
                .put("xpack.security.audit.outputs", "index, logfile")
                .put("xpack.security.audit.index.bulk_size", 1)
                .put("xpack.security.audit.index.flush_interval", "1ms")
                .put("xpack.security.audit.index.rollover", rollover.name().toLowerCase(Locale.ENGLISH))
                .put("xpack.security.audit.index.settings.index.number_of_shards", numShards)
                .put("xpack.security.audit.index.settings.index.number_of_replicas", numReplicas)
                .build();
    }

    static Settings remoteSettings(String address, int port, String clusterName) {
        return Settings.builder()
                .put("xpack.security.audit.index.client.hosts", address + ":" + port)
                .put("xpack.security.audit.index.client.cluster.name", clusterName)
                .build();
    }

    static Settings levelSettings(String[] includes, String[] excludes) {
        Settings.Builder builder = Settings.builder();
        if (includes != null) {
            builder.putList("xpack.security.audit.index.events.include", includes);
        }
        if (excludes != null) {
            builder.putList("xpack.security.audit.index.events.exclude", excludes);
        }
        return builder.build();
    }

    private Settings settings(IndexNameResolver.Rollover rollover, String[] includes, String[] excludes) {
        Settings.Builder builder = Settings.builder();
        builder.put(levelSettings(includes, excludes));
        builder.put(commonSettings(rollover));
        builder.put("xpack.security.audit.index.events.emit_request_body", includeRequestBody);
        return builder.build();
    }

    private Client getClient() {
        return remoteIndexing ? remoteCluster.client() : client();
    }

    private void initialize() throws Exception {
        initialize(null, null);
    }

    private void initialize(String[] includes, String[] excludes) throws Exception {
        initialize(includes, excludes, Settings.EMPTY);
    }

    private void initialize(final String[] includes, final String[] excludes, final Settings additionalSettings) throws Exception {
        rollover = randomFrom(HOURLY, DAILY, WEEKLY, MONTHLY);
        includeRequestBody = randomBoolean();
        Settings.Builder builder = Settings.builder();
        if (remoteIndexing) {
            builder.put(remoteSettings);
        }
        builder.put(settings(rollover, includes, excludes)).put(additionalSettings).build();
        // IndexAuditTrail should ignore secure settings
        // they are merged on the master node creating the audit index
        if (randomBoolean()) {
            MockSecureSettings ignored = new MockSecureSettings();
            if (randomBoolean()) {
                ignored.setString(KeyStoreWrapper.SEED_SETTING.getKey(), "non-empty-secure-settings");
            }
            builder.setSecureSettings(ignored);
        }
        Settings settings = builder.build();

        logger.info("--> settings: [{}]", settings);
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(remoteAddress.getAddress());
        when(localNode.getHostName()).thenReturn(remoteAddress.getAddress());
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.state()).thenReturn(client().admin().cluster().prepareState().get().getState());
        when(state.getNodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
        threadPool = new TestThreadPool("index audit trail tests");
        enqueuedMessage = new SetOnce<>();
        auditor = new IndexAuditTrail(settings, client(), threadPool, clusterService) {

            @Override
            void enqueue(Message message, String type) {
                enqueuedMessage.set(message);
                super.enqueue(message, type);
            }

            @Override
            List<Class<? extends Plugin>> remoteTransportClientPlugins() {
                return Arrays.asList(LocalStateSecurity.class, getTestTransportPlugin());
            }
        };
        auditor.start();
    }

    public void testIndexTemplateUpgrader() throws Exception {
        final MetaDataUpgrader metaDataUpgrader = internalCluster().getInstance(MetaDataUpgrader.class);
        final Map<String, IndexTemplateMetaData> updatedTemplates = metaDataUpgrader.indexTemplateMetaDataUpgraders.apply(emptyMap());
        final IndexTemplateMetaData indexAuditTrailTemplate = updatedTemplates.get(IndexAuditTrail.INDEX_TEMPLATE_NAME);
        assertThat(indexAuditTrailTemplate, notNullValue());
        // test custom index settings override template
        assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(indexAuditTrailTemplate.settings()), is(numReplicas));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(indexAuditTrailTemplate.settings()), is(numShards));
        // test upgrade template and installed template are equal
        final GetIndexTemplatesRequest request = new GetIndexTemplatesRequest(IndexAuditTrail.INDEX_TEMPLATE_NAME);
        final GetIndexTemplatesResponse response = client().admin().indices().getTemplates(request).get();
        assertThat(response.getIndexTemplates(), hasSize(1));
        assertThat(indexAuditTrailTemplate, is(response.getIndexTemplates().get(0)));
    }

    public void testProcessorsSetting() {
        final boolean explicitProcessors = randomBoolean();
        final int processors;
        if (explicitProcessors) {
            processors = randomIntBetween(1, 16);
        } else {
            processors = EsExecutors.PROCESSORS_SETTING.get(Settings.EMPTY);
        }
        final boolean explicitClientProcessors = randomBoolean();
        final int clientProcessors;
        if (explicitClientProcessors) {
            clientProcessors = randomIntBetween(1, 16);
        } else {
            clientProcessors = EsExecutors.PROCESSORS_SETTING.get(Settings.EMPTY);
        }

        final Settings.Builder additionalSettingsBuilder =
                Settings.builder()
                        .put("xpack.security.audit.index.client.cluster.name", "remote")
                        .put("xpack.security.audit.index.client.hosts", "localhost:9300");

        if (explicitProcessors) {
            additionalSettingsBuilder.put(EsExecutors.PROCESSORS_SETTING.getKey(), processors);
        }
        if (explicitClientProcessors) {
            additionalSettingsBuilder.put("xpack.security.audit.index.client.processors", clientProcessors);
        }

        final ThrowingRunnable runnable = () -> initialize(null, null, additionalSettingsBuilder.build());
        if (processors == clientProcessors || explicitClientProcessors == false) {
            // okay, the client initialized which is all we care about but no nodes are available because we never set up the remote cluster
            expectThrows(NoNodeAvailableException.class, runnable);
        } else {
            final IllegalStateException e = expectThrows(IllegalStateException.class, runnable);
            assertThat(
                    e,
                    hasToString(containsString(
                            "explicit processor setting [" + clientProcessors + "]" +
                                    " for audit trail remote client does not match inherited processor setting [" + processors + "]")));
        }
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.anonymousAccessDenied(randomAlphaOfLengthBetween(6, 12), "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteAddress.getAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals(localAddress.getAddress(), sourceMap.get("origin_address"));
        }

        assertEquals("_action", sourceMap.get("action"));
        assertEquals("transport", sourceMap.get("origin_type"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAnonymousAccessDeniedRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.anonymousAccessDenied(randomAlphaOfLengthBetween(6, 12), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat(NetworkAddress.format(InetAddress.getLoopbackAddress()), equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedTransport() throws Exception {
        initialize();
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), new MockToken(), "_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertAuditMessage(hit, "transport", "authentication_failed");

        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteAddress.getAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals(localAddress.getAddress(), sourceMap.get("origin_address"));
        }

        assertEquals("_principal", sourceMap.get("principal"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("transport", sourceMap.get("origin_type"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedTransportNoToken() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), "_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "authentication_failed");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteAddress.getAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals(localAddress.getAddress(), sourceMap.get("origin_address"));
        }

        assertThat(sourceMap.get("principal"), nullValue());
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("transport", sourceMap.get("origin_type"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), new MockToken(), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat(sourceMap.get("principal"), is((Object) "_principal"));
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedTransportRealm() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), "_realm", new MockToken(), "_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "realm_authentication_failed");
        Map<String, Object> sourceMap = hit.getSourceAsMap();

        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteAddress.getAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals(localAddress.getAddress(), sourceMap.get("origin_address"));
        }

        assertEquals("transport", sourceMap.get("origin_type"));
        assertEquals("_principal", sourceMap.get("principal"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("_realm", sourceMap.get("realm"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(randomAlphaOfLengthBetween(6, 12), "_realm", new MockToken(), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "realm_authentication_failed");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertEquals("_realm", sourceMap.get("realm"));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAccessGranted() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String role = randomAlphaOfLengthBetween(1, 6);
        auditor.accessGranted(randomAlphaOfLengthBetween(6, 12), createAuthentication(user), "_action", message, new String[] { role });

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "access_granted");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("realm"), is("lookRealm"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
            assertThat(sourceMap.get("run_by_realm"), is("authRealm"));
        } else {
            assertThat(sourceMap.get("principal"), is("_username"));
            assertThat(sourceMap.get("realm"), is("authRealm"));
        }
        assertEquals("_action", sourceMap.get("action"));
        assertThat((Iterable<String>) sourceMap.get(IndexAuditTrail.Field.ROLE_NAMES), containsInAnyOrder(role));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testSystemAccessGranted() throws Exception {
        initialize(new String[] { "system_access_granted" }, null);
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        String role = randomAlphaOfLengthBetween(1, 6);
        auditor.accessGranted(randomAlphaOfLength(8), createAuthentication(SystemUser.INSTANCE), "internal:_action", message,
            new String[] { role });

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "access_granted");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertEquals(SystemUser.INSTANCE.principal(), sourceMap.get("principal"));
        assertThat(sourceMap.get("realm"), is("authRealm"));
        assertEquals("internal:_action", sourceMap.get("action"));
        assertThat((Iterable<String>) sourceMap.get(IndexAuditTrail.Field.ROLE_NAMES), containsInAnyOrder(role));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAccessDenied() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        String role = randomAlphaOfLengthBetween(1, 6);
        auditor.accessDenied(randomAlphaOfLengthBetween(6, 12), createAuthentication(user), "_action", message, new String[] { role });

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertAuditMessage(hit, "transport", "access_denied");
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("realm"), is("lookRealm"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
            assertThat(sourceMap.get("run_by_realm"), is("authRealm"));
        } else {
            assertThat(sourceMap.get("principal"), is("_username"));
            assertThat(sourceMap.get("realm"), is("authRealm"));
        }
        assertEquals("_action", sourceMap.get("action"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
        assertThat((Iterable<String>) sourceMap.get(IndexAuditTrail.Field.ROLE_NAMES), containsInAnyOrder(role));
    }

    public void testTamperedRequestRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.tamperedRequest(randomAlphaOfLengthBetween(6, 12), request);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "rest", "tampered_request");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testTamperedRequest() throws Exception {
        initialize();
        TransportRequest message = new RemoteHostMockTransportRequest();
        auditor.tamperedRequest(randomAlphaOfLengthBetween(6, 12), "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertAuditMessage(hit, "transport", "tampered_request");
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is(nullValue()));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testTamperedRequestWithUser() throws Exception {
        initialize();
        TransportRequest message = new RemoteHostMockTransportRequest();
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        auditor.tamperedRequest(randomAlphaOfLengthBetween(6, 12), user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "tampered_request");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
        } else {
            assertEquals("_username", sourceMap.get("principal"));
        }
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testConnectionGranted() throws Exception {
        initialize();
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        auditor.connectionGranted(inetAddress, "default", rule);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "ip_filter", "connection_granted");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("allow default:accept_all", sourceMap.get("rule"));
        assertEquals("default", sourceMap.get("transport_profile"));
    }

    public void testConnectionDenied() throws Exception {
        initialize();
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        SecurityIpFilterRule rule = new SecurityIpFilterRule(false, "_all");
        auditor.connectionDenied(inetAddress, "default", rule);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "ip_filter", "connection_denied");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("deny _all", sourceMap.get("rule"));
        assertEquals("default", sourceMap.get("transport_profile"));
    }

    public void testRunAsGranted() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        User user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        String role = randomAlphaOfLengthBetween(1, 6);
        auditor.runAsGranted(randomAlphaOfLengthBetween(6, 12), createAuthentication(user), "_action", message, new String[] { role });

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "run_as_granted");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is("_username"));
        assertThat(sourceMap.get("realm"), is("authRealm"));
        assertThat(sourceMap.get("run_as_principal"), is("running as"));
        assertThat(sourceMap.get("run_as_realm"), is("lookRealm"));
        assertThat((Iterable<String>) sourceMap.get(IndexAuditTrail.Field.ROLE_NAMES), containsInAnyOrder(role));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testRunAsDenied() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        User user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        auditor.runAsDenied(randomAlphaOfLengthBetween(6, 12), createAuthentication(user), "_action", message, new String[] { "r1" });

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "run_as_denied");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is("_username"));
        assertThat(sourceMap.get("realm"), is("authRealm"));
        assertThat(sourceMap.get("run_as_principal"), is("running as"));
        assertThat(sourceMap.get("run_as_realm"), is("lookRealm"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationSuccessRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String realm = "_realm";
        auditor.authenticationSuccess(randomAlphaOfLengthBetween(6, 12), realm, user, request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_success");
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertRequestBody(sourceMap);
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
        } else {
            assertEquals("_username", sourceMap.get("principal"));
        }
        assertEquals("_realm", sourceMap.get("realm"));
    }

    public void testAuthenticationSuccessTransport() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("running as", new String[]{"r2"}, new User("_username", new String[] {"r1"}));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String realm = "_realm";
        auditor.authenticationSuccess(randomAlphaOfLengthBetween(6, 12), realm, user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertAuditMessage(hit, "transport", "authentication_success");
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
        } else {
            assertEquals("_username", sourceMap.get("principal"));
        }
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("_realm", sourceMap.get("realm"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    private void assertAuditMessage(SearchHit hit, String layer, String type) {
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        assertThat(sourceMap.get("@timestamp"), notNullValue());
        DateTime dateTime = ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime((String) sourceMap.get("@timestamp"));
        final DateTime now = DateTime.now(DateTimeZone.UTC);
        assertThat(dateTime + " should be on/before " + now, dateTime.isAfter(now), equalTo(false));

        assertThat(remoteAddress.getAddress(), equalTo(sourceMap.get("node_host_name")));
        assertThat(remoteAddress.getAddress(), equalTo(sourceMap.get("node_host_address")));

        assertEquals(layer, sourceMap.get("layer"));
        assertEquals(type, sourceMap.get("event_type"));
    }

    private void assertRequestBody(Map<String, Object> sourceMap) {
        if (includeRequestBody) {
            assertThat(sourceMap.get("request_body"), notNullValue());
        } else {
            assertThat(sourceMap.get("request_body"), nullValue());
        }
    }
    private class LocalHostMockMessage extends TransportMessage {
        LocalHostMockMessage() {
            remoteAddress(localAddress);
        }
    }

    private class RemoteHostMockMessage extends TransportMessage {
        RemoteHostMockMessage() throws Exception {
            remoteAddress(remoteAddress);
        }
    }

    private class RemoteHostMockTransportRequest extends TransportRequest {
        RemoteHostMockTransportRequest() throws Exception {
            remoteAddress(remoteAddress);
        }
    }

    private class MockIndicesTransportMessage extends RemoteHostMockMessage implements IndicesRequest {
        MockIndicesTransportMessage() throws Exception {
            super();
        }

        @Override
        public String[] indices() {
            return new String[] { "foo", "bar", "baz" };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return null;
        }
    }

    private static class MockToken implements AuthenticationToken {
        @Override
        public String principal() {
            return "_principal";
        }

        @Override
        public Object credentials() {
            fail("it's not allowed to print the credentials of the auth token");
            return null;
        }

        @Override
        public void clearCredentials() {
        }
    }

    private RestRequest mockRestRequest() {
        RestRequest request = mock(RestRequest.class);
        HttpChannel httpChannel = mock(HttpChannel.class);
        when(request.getHttpChannel()).thenReturn(httpChannel);
        when(httpChannel.getRemoteAddress()).thenReturn(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9200));
        when(request.uri()).thenReturn("_uri");
        return request;
    }

    private SearchHit getIndexedAuditMessage(Message message) throws InterruptedException {
        assertNotNull("no audit message was enqueued", message);
        final String indexName = IndexNameResolver.resolve(IndexAuditTrailField.INDEX_NAME_PREFIX, message.timestamp, rollover);
        ensureYellowAndNoInitializingShards(indexName);
        GetSettingsResponse settingsResponse = getClient().admin().indices().prepareGetSettings(indexName).get();
        assertThat(settingsResponse.getSetting(indexName, "index.number_of_shards"), is(Integer.toString(numShards)));
        assertThat(settingsResponse.getSetting(indexName, "index.number_of_replicas"), is(Integer.toString(numReplicas)));

        final SetOnce<SearchResponse> searchResponseSetOnce = new SetOnce<>();
        final boolean found = awaitBusy(() -> {
            try {
                SearchResponse searchResponse = getClient()
                        .prepareSearch(indexName)
                        .setTypes(IndexAuditTrail.DOC_TYPE)
                        .get();
                if (searchResponse.getHits().getTotalHits().value > 0L) {
                    searchResponseSetOnce.set(searchResponse);
                    return true;
                }
            } catch (Exception e) {
                logger.debug("caught exception while executing search", e);
            }
            return false;
        });
        assertThat("no audit document exists!", found, is(true));
        SearchResponse response = searchResponseSetOnce.get();
        assertNotNull(response);

        assertEquals(1, response.getHits().getTotalHits().value);
        return response.getHits().getHits()[0];
    }

    @Override
    public ClusterHealthStatus ensureYellowAndNoInitializingShards(String... indices) {
        if (remoteIndexing == false) {
            return super.ensureYellowAndNoInitializingShards(indices);
        }

        // pretty ugly but just a rip of ensureYellowAndNoInitializingShards that uses a different client
        ClusterHealthResponse actionGet = getClient().admin().cluster().health(Requests.clusterHealthRequest(indices)
                .waitForNoRelocatingShards(true)
                .waitForYellowStatus()
                .waitForEvents(Priority.LANGUID)
                .waitForNoInitializingShards(true))
                .actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}",
                    getClient().admin().cluster().prepareState().get().getState(),
                    getClient().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }

        logger.debug("indices {} are yellow", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    private static Authentication createAuthentication(User user) {
        final RealmRef lookedUpBy = user.authenticatedUser() == user ? null : new RealmRef("lookRealm", "up", "by");
        return new Authentication(user, new RealmRef("authRealm", "test", "foo"), lookedUpBy);
    }
}


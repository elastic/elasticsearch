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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail.Message;
import org.elasticsearch.xpack.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.DAILY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.HOURLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.MONTHLY;
import static org.elasticsearch.xpack.security.audit.index.IndexNameResolver.Rollover.WEEKLY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ESIntegTestCase.ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class IndexAuditTrailTests extends SecurityIntegTestCase {
    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + SUITE_CLUSTER_NODE_PREFIX;

    private static boolean remoteIndexing;
    private static InternalTestCluster remoteCluster;
    private static Settings remoteSettings;
    private static byte[] systemKey;

    private TransportAddress remoteAddress = buildNewFakeTransportAddress();
    private TransportAddress localAddress = new TransportAddress(InetAddress.getLoopbackAddress(), 0);
    private IndexNameResolver.Rollover rollover;
    private IndexAuditTrail auditor;
    private SetOnce<Message> enqueuedMessage;
    private int numShards;
    private int numReplicas;
    private ThreadPool threadPool;
    private boolean includeRequestBody;

    @BeforeClass
    public static void configureBeforeClass() {
        remoteIndexing = randomBoolean();
        systemKey = CryptoService.generateKey();
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
        final boolean useGeneratedSSL = useSecurity && randomBoolean();
        logger.info("--> remote indexing enabled. security enabled: [{}], SSL enabled: [{}], nodes: [{}]", useSecurity, useGeneratedSSL,
                numNodes);
        SecuritySettingsSource cluster2SettingsSource =
                new SecuritySettingsSource(numNodes, useGeneratedSSL, systemKey(), createTempDir(), Scope.SUITE) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        .put(XPackSettings.SECURITY_ENABLED.getKey(), useSecurity);
                if (useSecurity == false && builder.get(NetworkModule.TRANSPORT_TYPE_KEY) == null) {
                    builder.put(NetworkModule.TRANSPORT_TYPE_KEY, MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME);
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
                        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME);
                    }
                    return builder.build();
                }
            }
        };


        Set<Class<? extends Plugin>> mockPlugins = new HashSet<>(getMockPlugins());
        if (useSecurity == false) {
            mockPlugins.add(MockTcpTransportPlugin.class);
        }
        remoteCluster = new InternalTestCluster(randomLong(), createTempDir(), false, true, numNodes, numNodes, cluster2Name,
                cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, mockPlugins,
                useSecurity ? getClientWrapper() : Function.identity());
        remoteCluster.beforeTest(random(), 0.5);

        NodesInfoResponse response = remoteCluster.client().admin().cluster().prepareNodesInfo().execute().actionGet();
        TransportInfo info = response.getNodes().get(0).getTransport();
        TransportAddress inet = info.address().publishAddress();

        Settings.Builder builder = Settings.builder()
                .put("xpack.security.audit.index.client." + XPackSettings.SECURITY_ENABLED.getKey(), useSecurity)
                .put(remoteSettings(NetworkAddress.format(inet.address().getAddress()), inet.address().getPort(), cluster2Name))
                .put("xpack.security.audit.index.client.xpack.security.user", SecuritySettingsSource.DEFAULT_USER_NAME + ":" +
                        SecuritySettingsSource.DEFAULT_PASSWORD);

        if (useGeneratedSSL == false) {
            for (Map.Entry<String, String> entry : cluster2SettingsSource.getClientSSLSettings().getAsMap().entrySet()) {
                builder.put("xpack.security.audit.index.client." + entry.getKey(), entry.getValue());
            }
        }
        if (useSecurity == false && builder.get(NetworkModule.TRANSPORT_TYPE_KEY) == null) {
            builder.put("xpack.security.audit.index.client." + NetworkModule.TRANSPORT_TYPE_KEY,
                    MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME);
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
            remoteCluster.wipe(Collections.<String>emptySet());
        }
    }

    @Override
    protected Set<String> excludeTemplates() {
        return Collections.singleton(IndexAuditTrail.INDEX_TEMPLATE_NAME);
    }

    @Override
    protected byte[] systemKey() {
        return systemKey;
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
            builder.putArray("xpack.security.audit.index.events.include", includes);
        }
        if (excludes != null) {
            builder.putArray("xpack.security.audit.index.events.exclude", excludes);
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

    private void initialize() throws IOException, InterruptedException {
        initialize(null, null);
    }

    private void initialize(String[] includes, String[] excludes) throws IOException, InterruptedException {
        rollover = randomFrom(HOURLY, DAILY, WEEKLY, MONTHLY);
        numReplicas = numberOfReplicas();
        numShards = numberOfShards();
        includeRequestBody = randomBoolean();
        Settings.Builder builder = Settings.builder();
        if (remoteIndexing) {
            builder.put(remoteSettings);
        }

        Settings settings = builder.put(settings(rollover, includes, excludes)).build();
        logger.info("--> settings: [{}]", settings.getAsMap().toString());
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getHostAddress()).thenReturn(remoteAddress.getAddress());
        when(localNode.getHostName()).thenReturn(remoteAddress.getAddress());
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.state()).thenReturn(state);
        when(state.getNodes()).thenReturn(nodes);
        when(nodes.isLocalNodeElectedMaster()).thenReturn(true);
        threadPool = new TestThreadPool("index audit trail tests");
        enqueuedMessage = new SetOnce<>();
        auditor = new IndexAuditTrail(settings, internalClient(), threadPool, clusterService) {
            @Override
            void enqueue(Message message, String type) {
                enqueuedMessage.set(message);
                super.enqueue(message, type);
            }

            @Override
            List<Class<? extends Plugin>> remoteTransportClientPlugins() {
                return Arrays.asList(XPackPlugin.class, MockTcpTransportPlugin.class);
            }
        };
        auditor.start(true);
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.anonymousAccessDenied("_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        auditor.anonymousAccessDenied(request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(NetworkAddress.format(InetAddress.getLoopbackAddress()), equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedTransport() throws Exception {
        initialize();
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        auditor.authenticationFailed(new MockToken(), "_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        auditor.authenticationFailed("_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        auditor.authenticationFailed(new MockToken(), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), is((Object) "_principal"));
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testAuthenticationFailedTransportRealm() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.authenticationFailed("_realm", new MockToken(), "_action", message);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "realm_authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();

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
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed("_realm", new MockToken(), request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "realm_authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        auditor.accessGranted(user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "access_granted");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
        } else {
            assertEquals("_username", sourceMap.get("principal"));
        }
        assertEquals("_action", sourceMap.get("action"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testSystemAccessGranted() throws Exception {
        initialize(new String[] { "system_access_granted" }, null);
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        auditor.accessGranted(SystemUser.INSTANCE, "internal:_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "access_granted");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertEquals(SystemUser.INSTANCE.principal(), sourceMap.get("principal"));
        assertEquals("internal:_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAccessDenied() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        auditor.accessDenied(user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertAuditMessage(hit, "transport", "access_denied");
        assertEquals("transport", sourceMap.get("origin_type"));
        if (runAs) {
            assertThat(sourceMap.get("principal"), is("running as"));
            assertThat(sourceMap.get("run_by_principal"), is("_username"));
        } else {
            assertEquals("_username", sourceMap.get("principal"));
        }
        assertEquals("_action", sourceMap.get("action"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, containsInAnyOrder((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testTamperedRequestRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.tamperedRequest(request);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "rest", "tampered_request");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertRequestBody(sourceMap);
    }

    public void testTamperedRequest() throws Exception {
        initialize();
        TransportRequest message = new RemoteHostMockTransportRequest();
        auditor.tamperedRequest("_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
            user = new User("_username", new String[]{"r1"},
                    new User("running as", new String[] {"r2"}));
        } else {
            user = new User("_username", new String[]{"r1"});
        }
        auditor.tamperedRequest(user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "transport", "tampered_request");
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("deny _all", sourceMap.get("rule"));
        assertEquals("default", sourceMap.get("transport_profile"));
    }

    public void testRunAsGranted() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        User user = new User("_username", new String[]{"r1"}, new User("running as", new String[] {"r2"}));
        auditor.runAsGranted(user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "run_as_granted");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is("_username"));
        assertThat(sourceMap.get("run_as_principal"), is("running as"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testRunAsDenied() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        User user = new User("_username", new String[]{"r1"}, new User("running as", new String[] {"r2"}));
        auditor.runAsDenied(user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        assertAuditMessage(hit, "transport", "run_as_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is("_username"));
        assertThat(sourceMap.get("run_as_principal"), is("running as"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationSuccessRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        final boolean runAs = randomBoolean();
        User user;
        if (runAs) {
            user = new User("_username", new String[] { "r1" }, new User("running as", new String[] { "r2" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String realm = "_realm";
        auditor.authenticationSuccess(realm, user, request);
        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());

        assertAuditMessage(hit, "rest", "authentication_success");
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
            user = new User("_username", new String[] { "r1" }, new User("running as", new String[] { "r2" }));
        } else {
            user = new User("_username", new String[] { "r1" });
        }
        String realm = "_realm";
        auditor.authenticationSuccess(realm, user, "_action", message);

        SearchHit hit = getIndexedAuditMessage(enqueuedMessage.get());
        Map<String, Object> sourceMap = hit.sourceAsMap();
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
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("@timestamp"), notNullValue());
        DateTime dateTime = ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime((String) sourceMap.get("@timestamp"));
        assertThat(dateTime.isBefore(DateTime.now(DateTimeZone.UTC)), is(true));

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
        when(request.getRemoteAddress()).thenReturn(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9200));
        when(request.uri()).thenReturn("_uri");
        return request;
    }

    private SearchHit getIndexedAuditMessage(Message message) throws InterruptedException {
        assertNotNull("no audit message was enqueued", message);
        final String indexName = IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, message.timestamp, rollover);
        ensureYellow(indexName);
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
                if (searchResponse.getHits().totalHits() > 0L) {
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

        assertEquals(1, response.getHits().getTotalHits());
        return response.getHits().getHits()[0];
    }

    @Override
    public ClusterHealthStatus ensureYellow(String... indices) {
        if (remoteIndexing == false) {
            return super.ensureYellow(indices);
        }

        // pretty ugly but just a rip of ensureYellow that uses a different client
        ClusterHealthResponse actionGet = getClient().admin().cluster().health(Requests.clusterHealthRequest(indices)
                .waitForNoRelocatingShards(true).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}",
                    getClient().admin().cluster().prepareState().get().getState(),
                    getClient().admin().cluster().preparePendingClusterTasks().get());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }

        logger.debug("indices {} are yellow", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }
}


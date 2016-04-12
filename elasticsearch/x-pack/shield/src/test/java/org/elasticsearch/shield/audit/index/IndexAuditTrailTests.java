/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.crypto.InternalCryptoService;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.shield.transport.filter.ShieldIpFilterRule;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.transport.TransportRequest;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.DAILY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.HOURLY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.MONTHLY;
import static org.elasticsearch.shield.audit.index.IndexNameResolver.Rollover.WEEKLY;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.InternalTestCluster.clusterName;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1)
public class IndexAuditTrailTests extends ShieldIntegTestCase {
    public static final String SECOND_CLUSTER_NODE_PREFIX = "remote_" + SUITE_CLUSTER_NODE_PREFIX;

    private static boolean remoteIndexing;
    private static InternalTestCluster remoteCluster;
    private static Settings remoteSettings;
    private static byte[] systemKey;

    private IndexNameResolver.Rollover rollover;
    private IndexAuditTrail auditor;
    private int numShards;
    private int numReplicas;
    private ThreadPool threadPool;

    @BeforeClass
    public static void configureBeforeClass() {
        remoteIndexing = randomBoolean();
        systemKey = InternalCryptoService.generateKey();
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

        // Setup a second test cluster with randomization for number of nodes, shield enabled, and SSL
        final int numNodes = randomIntBetween(1, 2);
        final boolean useShield = randomBoolean();
        final boolean useSSL = useShield && randomBoolean();
        logger.info("--> remote indexing enabled. shield enabled: [{}], SSL enabled: [{}], nodes: [{}]", useShield, useSSL, numNodes);
        ShieldSettingsSource cluster2SettingsSource =
                new ShieldSettingsSource(numNodes, useSSL, randomBoolean(), systemKey(), createTempDir(), Scope.SUITE) {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        .put(super.nodeSettings(nodeOrdinal))
                        .put(Security.enabledSetting(), useShield);
                return builder.build();
            }
        };
        remoteCluster = new InternalTestCluster("network", randomLong(), createTempDir(), numNodes, numNodes, cluster2Name,
                cluster2SettingsSource, 0, false, SECOND_CLUSTER_NODE_PREFIX, getMockPlugins(),
                useShield ? getClientWrapper() : Function.identity());
        remoteCluster.beforeTest(random(), 0.5);

        NodesInfoResponse response = remoteCluster.client().admin().cluster().prepareNodesInfo().execute().actionGet();
        TransportInfo info = response.getNodes()[0].getTransport();
        InetSocketTransportAddress inet = (InetSocketTransportAddress) info.address().publishAddress();

        Settings.Builder builder = Settings.builder()
                .put(Security.enabledSetting(), useShield)
                .put(remoteSettings(NetworkAddress.format(inet.address().getAddress()), inet.address().getPort(), cluster2Name))
                .put("xpack.security.audit.index.client.xpack.security.user", ShieldSettingsSource.DEFAULT_USER_NAME + ":" +
                        ShieldSettingsSource.DEFAULT_PASSWORD);

        if (useSSL) {
            for (Map.Entry<String, String> entry : cluster2SettingsSource.getClientSSLSettings().getAsMap().entrySet()) {
                builder.put("xpack.security.audit.index.client." + entry.getKey(), entry.getValue());
            }
        } else {
            builder.put("xpack.security.audit.index.client." + ShieldNettyTransport.SSL_SETTING.getKey(), false);
        }
        remoteSettings = builder.build();
    }

    @After
    public void afterTest() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
        if (auditor != null) {
            auditor.close();
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
        Settings.Builder builder = Settings.builder();
        if (remoteIndexing) {
            builder.put(remoteSettings);
        }

        Settings settings = builder.put(settings(rollover, includes, excludes)).build();
        logger.info("--> settings: [{}]", settings.getAsMap().toString());
        Transport transport = mock(Transport.class);
        BoundTransportAddress boundTransportAddress = new BoundTransportAddress(new TransportAddress[]{DummyTransportAddress.INSTANCE},
                DummyTransportAddress.INSTANCE);
        when(transport.boundAddress()).thenReturn(boundTransportAddress);
        threadPool = new ThreadPool("index audit trail tests");
        auditor = new IndexAuditTrail(settings, transport, Providers.of(internalClient()), threadPool, mock(ClusterService.class));
        auditor.start(true);
    }

    public void testAnonymousAccessDeniedTransport() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.anonymousAccessDenied("_action", message);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
        assertAuditMessage(hit, "transport", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteHostAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals("local[local_host]", sourceMap.get("origin_address"));
        }

        assertEquals("_action", sourceMap.get("action"));
        assertEquals("transport", sourceMap.get("origin_type"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, contains((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAnonymousAccessDeniedRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.anonymousAccessDenied(request);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "rest", "anonymous_access_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(NetworkAddress.format(InetAddress.getLoopbackAddress()), equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertThat(sourceMap.get("request_body"), notNullValue());
    }

    public void testAuthenticationFailedTransport() throws Exception {
        initialize();
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        auditor.authenticationFailed(new MockToken(), "_action", message);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertAuditMessage(hit, "transport", "authentication_failed");

        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteHostAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals("local[local_host]", sourceMap.get("origin_address"));
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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "transport", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteHostAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals("local[local_host]", sourceMap.get("origin_address"));
        }

        assertThat(sourceMap.get("principal"), nullValue());
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("transport", sourceMap.get("origin_type"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, contains((Object[]) ((IndicesRequest) message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(new MockToken(), request);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), is((Object) "_principal"));
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertThat(sourceMap.get("request_body"), notNullValue());
    }

    public void testAuthenticationFailedRestNoToken() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed(request);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertThat(sourceMap.get("request_body"), notNullValue());
    }

    public void testAuthenticationFailedTransportRealm() throws Exception {
        initialize();
        TransportMessage message = randomFrom(new RemoteHostMockMessage(), new LocalHostMockMessage(), new MockIndicesTransportMessage());
        auditor.authenticationFailed("_realm", new MockToken(), "_action", message);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "transport", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();

        if (message instanceof RemoteHostMockMessage) {
            assertEquals(remoteHostAddress(), sourceMap.get("origin_address"));
        } else {
            assertEquals("local[local_host]", sourceMap.get("origin_address"));
        }

        assertEquals("transport", sourceMap.get("origin_type"));
        assertEquals("_principal", sourceMap.get("principal"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals("_realm", sourceMap.get("realm"));
        if (message instanceof IndicesRequest) {
            List<Object> indices = (List<Object>) sourceMap.get("indices");
            assertThat(indices, contains((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testAuthenticationFailedRestRealm() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.authenticationFailed("_realm", new MockToken(), request);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "rest", "authentication_failed");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertEquals("_realm", sourceMap.get("realm"));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertThat(sourceMap.get("request_body"), notNullValue());
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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
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
            assertThat(indices, contains((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testSystemAccessGranted() throws Exception {
        initialize(new String[] { "system_access_granted" }, null);
        TransportMessage message = randomBoolean() ? new RemoteHostMockMessage() : new LocalHostMockMessage();
        auditor.accessGranted(SystemUser.INSTANCE, "internal:_action", message);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
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
            assertThat(indices, contains((Object[]) ((IndicesRequest)message).indices()));
        }
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    public void testTamperedRequestRest() throws Exception {
        initialize();
        RestRequest request = mockRestRequest();
        auditor.tamperedRequest(request);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
        assertAuditMessage(hit, "rest", "tampered_request");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("principal"), nullValue());
        assertThat("127.0.0.1", equalTo(sourceMap.get("origin_address")));
        assertThat("_uri", equalTo(sourceMap.get("uri")));
        assertThat(sourceMap.get("origin_type"), is("rest"));
        assertThat(sourceMap.get("request_body"), notNullValue());
    }

    public void testTamperedRequest() throws Exception {
        initialize();
        TransportRequest message = new RemoteHostMockTransportRequest();
        auditor.tamperedRequest("_action", message);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

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
        ShieldIpFilterRule rule = IPFilter.DEFAULT_PROFILE_ACCEPT_ALL;
        auditor.connectionGranted(inetAddress, "default", rule);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

        assertAuditMessage(hit, "ip_filter", "connection_granted");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("allow default:accept_all", sourceMap.get("rule"));
        assertEquals("default", sourceMap.get("transport_profile"));
    }

    public void testConnectionDenied() throws Exception {
        initialize();
        InetAddress inetAddress = InetAddress.getLoopbackAddress();
        ShieldIpFilterRule rule = new ShieldIpFilterRule(false, "_all");
        auditor.connectionDenied(inetAddress, "default", rule);
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();

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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
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
        awaitAuditDocumentCreation(resolveIndexName());

        SearchHit hit = getIndexedAuditMessage();
        assertAuditMessage(hit, "transport", "run_as_denied");
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertEquals("transport", sourceMap.get("origin_type"));
        assertThat(sourceMap.get("principal"), is("_username"));
        assertThat(sourceMap.get("run_as_principal"), is("running as"));
        assertEquals("_action", sourceMap.get("action"));
        assertEquals(sourceMap.get("request"), message.getClass().getSimpleName());
    }

    private void assertAuditMessage(SearchHit hit, String layer, String type) {
        Map<String, Object> sourceMap = hit.sourceAsMap();
        assertThat(sourceMap.get("@timestamp"), notNullValue());
        DateTime dateTime = ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime((String) sourceMap.get("@timestamp"));
        assertThat(dateTime.isBefore(DateTime.now(DateTimeZone.UTC)), is(true));

        assertThat(DummyTransportAddress.INSTANCE.getHost(), equalTo(sourceMap.get("node_host_name")));
        assertThat(DummyTransportAddress.INSTANCE.getAddress(), equalTo(sourceMap.get("node_host_address")));

        assertEquals(layer, sourceMap.get("layer"));
        assertEquals(type, sourceMap.get("event_type"));
    }

    private static class LocalHostMockMessage extends TransportMessage {
        LocalHostMockMessage() {
            remoteAddress(new LocalTransportAddress("local_host"));
        }
    }

    private static class RemoteHostMockMessage extends TransportMessage {
        RemoteHostMockMessage() throws Exception {
            remoteAddress(DummyTransportAddress.INSTANCE);
        }
    }

    private static class RemoteHostMockTransportRequest extends TransportRequest {
        RemoteHostMockTransportRequest() throws Exception {
            remoteAddress(DummyTransportAddress.INSTANCE);
        }
    }

    private static class MockIndicesTransportMessage extends RemoteHostMockMessage implements IndicesRequest {
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

    private SearchHit getIndexedAuditMessage() {

        SearchResponse response = getClient().prepareSearch(resolveIndexName())
                .setTypes(IndexAuditTrail.DOC_TYPE)
                .execute().actionGet();

        assertEquals(1, response.getHits().getTotalHits());
        return response.getHits().getHits()[0];
    }

    private String[] fieldList() {
        return new String[] {
                "@timestamp",
                "node_name",
                "node_host_name",
                "node_host_address",
                "layer",
                "event_type",
                "origin_address",
                "origin_type",
                "principal",
                "run_by_principal",
                "run_as_principal",
                "action",
                "indices",
                "request",
                "request_body",
                "uri",
                "realm",
                "transport_profile",
                "rule"
        };
    }

    private void awaitAuditDocumentCreation(final String indexName) throws InterruptedException {
        ensureYellow(indexName);
        boolean found = awaitBusy(() -> {
            try {
                SearchResponse searchResponse = getClient().prepareSearch(indexName).setSize(0).setTerminateAfter(1).execute().actionGet();
                return searchResponse.getHits().totalHits() > 0L;
            } catch (Exception e) {
                return false;
            }
        });
        assertThat("no audit document exists!", found, is(true));

        GetSettingsResponse response = getClient().admin().indices().prepareGetSettings(indexName).execute().actionGet();
        assertThat(response.getSetting(indexName, "index.number_of_shards"), is(Integer.toString(numShards)));
        assertThat(response.getSetting(indexName, "index.number_of_replicas"), is(Integer.toString(numReplicas)));
    }

    @Override
    public ClusterHealthStatus ensureYellow(String... indices) {
        if (remoteIndexing == false) {
            return super.ensureYellow(indices);
        }

        // pretty ugly but just a rip of ensureYellow that uses a different client
        ClusterHealthResponse actionGet = getClient().admin().cluster().health(Requests.clusterHealthRequest(indices)
                .waitForRelocatingShards(0).waitForYellowStatus().waitForEvents(Priority.LANGUID)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureYellow timed out, cluster state:\n{}\n{}",
                    getClient().admin().cluster().prepareState().get().getState().prettyPrint(),
                    getClient().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for yellow", actionGet.isTimedOut(), equalTo(false));
        }

        logger.debug("indices {} are yellow", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    private String resolveIndexName() {
        return IndexNameResolver.resolve(IndexAuditTrail.INDEX_NAME_PREFIX, DateTime.now(DateTimeZone.UTC), rollover);
    }

    static String remoteHostAddress() throws Exception {
        return DummyTransportAddress.INSTANCE.toString();
    }
}


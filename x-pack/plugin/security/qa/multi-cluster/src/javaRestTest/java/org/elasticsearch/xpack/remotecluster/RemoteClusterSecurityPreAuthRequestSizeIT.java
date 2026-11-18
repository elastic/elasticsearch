/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.PASS;
import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.USER;
import static org.elasticsearch.xpack.remotecluster.AbstractRemoteClusterSecurityTestCase.createCrossClusterAccessApiKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Checks how much of an unauthenticated request the fulfilling cluster reads on the RCS 2.0 remote cluster port.
 * <p>
 * Two limits apply before a request on this port is authenticated. While the frame is decoded, a variable header larger than
 * {@code remote_cluster.max_request_header_size} (64KB by default) closes the connection. Once the header is decoded,
 * {@code SecurityNetty4Transport} stops reading from the channel and authenticates the cross-cluster headers; if that fails, the
 * request is rejected and the channel closed without the body ever being read. So an unauthenticated peer cannot make the node buffer,
 * breaker-account or deserialize a request body, whatever its size.
 * <p>
 * Each test sends a large, well-formed body and measures the node's transport {@code rx_size_in_bytes}, with a control that sends the
 * same body with valid credentials to show that the measurement does detect a body being read.
 * <p>
 * The cluster also sets {@code remote_cluster.max_authenticated_request_header_size}, so a header above the 64KB limit is accepted, up
 * to that size, once the cross-cluster credentials serialized at its start authenticate.
 */
public class RemoteClusterSecurityPreAuthRequestSizeIT extends ESRestTestCase {

    private static final String SHARD_GET_ACTION = TransportGetAction.TYPE.name() + "[s]";
    private static final RemoteClusterActionType<GetResponse> SHARD_GET = new RemoteClusterActionType<>(SHARD_GET_ACTION, GetResponse::new);
    private static final int LARGE_BODY_BYTES = 8 * 1024 * 1024;

    @ClassRule
    public static ElasticsearchCluster testCluster = ElasticsearchCluster.local()
        .name("test-cluster")
        .module("analysis-common")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("remote_cluster_server.enabled", "true")
        .setting("remote_cluster.port", "0")
        .setting("xpack.security.remote_cluster_server.ssl.enabled", "false")
        .setting("remote_cluster.max_authenticated_request_header_size", "1mb")
        .user(USER, PASS.toString())
        .build();

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Override
    protected String getTestRestCluster() {
        return testCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @After
    public void terminateThreadPool() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * A large body sent with credentials for an API key that does not exist is rejected with a 401 without the body being read, while
     * the same body sent with valid credentials is read in full.
     */
    public void testLargeBodyWithInvalidCredentialsIsNotRead() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();

        final long rxBeforeInvalid = transportBytesReceived();
        try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, invalidCredentialsHeaders()))) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, largeGetRequest())
            );
            assertAuthenticationFailure(e, "unable to find apikey");
        }
        assertThat(transportBytesReceived() - rxBeforeInvalid, lessThan((long) LARGE_BODY_BYTES));

        final long rxBeforeValid = transportBytesReceived();
        try (MockTransportService service = startTransport(validApiKey, Map.of())) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, largeGetRequest())
            );
            assertNotAuthenticationFailure(e);
        }
        assertThat(transportBytesReceived() - rxBeforeValid, greaterThanOrEqualTo((long) LARGE_BODY_BYTES));
    }

    /**
     * A large body sent without any cross-cluster headers is rejected by the same header authentication, without the body being read.
     */
    public void testLargeBodyWithoutCrossClusterHeadersIsNotRead() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();

        final long rxBefore = transportBytesReceived();
        try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, threadContext -> {}))) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, largeGetRequest())
            );
            assertAuthenticationFailure(e, "failed to extract credentials from headers");
        }
        assertThat(transportBytesReceived() - rxBefore, lessThan((long) LARGE_BODY_BYTES));
    }

    /**
     * With the in-flight requests breaker lowered below the body size, an unauthenticated request still fails authentication, because
     * its body is never charged to the breaker. An authenticated request with the same body trips the breaker.
     */
    public void testInFlightRequestsBreakerIsOnlyChargedAfterAuthentication() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();

        updateInFlightRequestsBreakerLimit("1mb");
        try {
            try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, invalidCredentialsHeaders()))) {
                final Exception e = expectThrows(
                    Exception.class,
                    () -> executeRemote(remoteClusterClient(service), SHARD_GET, largeGetRequest())
                );
                assertAuthenticationFailure(e, "unable to find apikey");
            }

            try (MockTransportService service = startTransport(validApiKey, Map.of())) {
                final CircuitBreakingException e = expectThrows(
                    CircuitBreakingException.class,
                    () -> executeRemote(remoteClusterClient(service), SHARD_GET, largeGetRequest())
                );
                assertThat(e.getMessage(), containsString("[inflight_requests]"));
            }
        } finally {
            updateInFlightRequestsBreakerLimit(null);
        }
    }

    /**
     * A header larger than {@code remote_cluster.max_request_header_size} whose credentials do not authenticate closes the connection
     * before the rest of the header is read.
     */
    public void testOversizedHeaderWithInvalidCredentialsClosesConnection() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();
        final CheckedConsumer<ThreadContext, IOException> oversizedHeaders = threadContext -> new CrossClusterAccessHeaders(
            invalidEncodedApiKey(),
            subjectInfoWithIndexNameOfLength(128 * 1024)
        ).writeToContext(threadContext, null);

        try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, oversizedHeaders))) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, new GetRequest("index", "1"))
            );
            assertThat(e, instanceOf(ConnectTransportException.class));
        }
    }

    /**
     * A header larger than {@code remote_cluster.max_request_header_size}, but within {@code max_authenticated_request_header_size},
     * is accepted when its credentials authenticate, and the request goes on to authorization.
     */
    public void testOversizedHeaderWithValidCredentialsIsAccepted() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();
        final CheckedConsumer<ThreadContext, IOException> oversizedHeaders = threadContext -> new CrossClusterAccessHeaders(
            "ApiKey " + validApiKey,
            subjectInfoWithIndexNameOfLength(128 * 1024)
        ).writeToContext(threadContext, null);

        try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, oversizedHeaders))) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, new GetRequest("index", "1"))
            );
            assertNotAuthenticationFailure(e);
        }
    }

    /**
     * A header larger than {@code max_authenticated_request_header_size} closes the connection even with valid credentials.
     */
    public void testHeaderAboveAuthenticatedLimitClosesConnection() throws Exception {
        final String validApiKey = createValidCrossClusterApiKey();
        final CheckedConsumer<ThreadContext, IOException> oversizedHeaders = threadContext -> new CrossClusterAccessHeaders(
            "ApiKey " + validApiKey,
            subjectInfoWithIndexNameOfLength(1024 * 1024)
        ).writeToContext(threadContext, null);

        try (MockTransportService service = startTransport(validApiKey, Map.of(SHARD_GET_ACTION, oversizedHeaders))) {
            final Exception e = expectThrows(
                Exception.class,
                () -> executeRemote(remoteClusterClient(service), SHARD_GET, new GetRequest("index", "1"))
            );
            assertThat(e, instanceOf(ConnectTransportException.class));
        }
    }

    private static CrossClusterAccessSubjectInfo subjectInfoWithIndexNameOfLength(int length) throws IOException {
        return new CrossClusterAccessSubjectInfo(
            Authentication.newRealmAuthentication(new User("query-user", "role"), new Authentication.RealmRef("file", "file", "node")),
            new RoleDescriptorsIntersection(
                new RoleDescriptor(
                    "cross_cluster",
                    null,
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices(randomAlphaOfLength(length)).privileges("read").build() },
                    null
                )
            )
        );
    }

    /**
     * A failed header authentication sends a 401 and then closes the channel. If the client is still writing the large body when the
     * channel closes, the connection is reset and the 401 may never be read, so a disconnect is an equally valid outcome.
     */
    private static void assertAuthenticationFailure(Exception e, String message) {
        if (e instanceof ConnectTransportException) {
            return;
        }
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.UNAUTHORIZED));
        assertThat(e.getMessage(), containsString(message));
    }

    private static void assertNotAuthenticationFailure(Exception e) {
        assertThat(e, not(instanceOf(ConnectTransportException.class)));
        if (e instanceof ElasticsearchSecurityException securityException) {
            assertThat(securityException.status(), not(equalTo(RestStatus.UNAUTHORIZED)));
        }
    }

    private static String createValidCrossClusterApiKey() throws IOException {
        final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey(adminClient(), """
            {
              "search": [
                {
                   "names": ["index"]
                }
              ]
            }""");
        return (String) apiKeyMap.get("encoded");
    }

    private static String invalidEncodedApiKey() {
        final String idAndSecret = randomAlphaOfLength(20) + ":" + randomAlphaOfLength(22);
        return "ApiKey " + Base64.getEncoder().encodeToString(idAndSecret.getBytes(StandardCharsets.UTF_8));
    }

    private static CheckedConsumer<ThreadContext, IOException> invalidCredentialsHeaders() {
        return threadContext -> new CrossClusterAccessHeaders(
            invalidEncodedApiKey(),
            SystemUser.crossClusterAccessSubjectInfo(TransportVersion.current(), "node")
        ).writeToContext(threadContext, null);
    }

    /**
     * A well-formed shard-level get request whose document ID makes the serialized body at least {@link #LARGE_BODY_BYTES} long.
     */
    private static GetRequest largeGetRequest() {
        return new GetRequest("index", randomAlphaOfLength(LARGE_BODY_BYTES));
    }

    private static long transportBytesReceived() throws IOException {
        final ObjectPath stats = assertOKAndCreateObjectPath(
            adminClient().performRequest(new Request("GET", "/_nodes/_local/stats/transport"))
        );
        final Map<String, Object> nodes = stats.evaluate("nodes");
        assertThat(nodes.size(), equalTo(1));
        final String nodeId = nodes.keySet().iterator().next();
        return ((Number) stats.evaluate("nodes." + nodeId + ".transport.rx_size_in_bytes")).longValue();
    }

    private static void updateInFlightRequestsBreakerLimit(String limit) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            limit == null
                ? "{ \"persistent\": { \"network.breaker.inflight_requests.limit\": null } }"
                : "{ \"persistent\": { \"network.breaker.inflight_requests.limit\": \"" + limit + "\" } }"
        );
        assertOK(adminClient().performRequest(request));
    }

    private static RemoteClusterClient remoteClusterClient(MockTransportService service) {
        return service.getRemoteClusterService()
            .getRemoteClusterClient(
                "my_remote_cluster",
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
            );
    }

    private static <Req extends ActionRequest, Resp extends ActionResponse> Resp executeRemote(
        RemoteClusterClient client,
        RemoteClusterActionType<Resp> action,
        Req request
    ) throws Exception {
        final var future = new PlainActionFuture<Resp>();
        client.execute(action, request, future);
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (ExceptionsHelper.unwrapCause(e.getCause()) instanceof Exception cause) {
                throw cause;
            }
            throw new AssertionError(e);
        }
    }

    /**
     * Starts a transport service that acts as a querying cluster. Connection setup and every action not in {@code headerOverrides}
     * carry valid cross-cluster headers for {@code encodedApiKey}; actions in {@code headerOverrides} get their headers written by the
     * given consumer instead, so a single request can carry invalid, missing or oversized headers.
     */
    private MockTransportService startTransport(
        String encodedApiKey,
        Map<String, CheckedConsumer<ThreadContext, IOException>> headerOverrides
    ) {
        final String remoteClusterServerEndpoint = testCluster.getRemoteClusterServerEndpoint(0);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote_cluster.credentials", encodedApiKey);
        final Settings settings = Settings.builder()
            .put("node.name", "node")
            .put("xpack.security.remote_cluster_client.ssl.enabled", "false")
            .put("cluster.remote.my_remote_cluster.mode", "proxy")
            .put("cluster.remote.my_remote_cluster.skip_unavailable", "false")
            .put("cluster.remote.my_remote_cluster.proxy_address", remoteClusterServerEndpoint)
            .setSecureSettings(secureSettings)
            .build();

        final MockTransportService service = MockTransportService.createNewService(
            settings,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool,
            null
        );
        boolean success = false;
        try {
            service.addSendBehavior((connection, requestId, action, request, options) -> {
                final ThreadContext threadContext = threadPool.getThreadContext();
                try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                    headerOverrides.getOrDefault(
                        action,
                        ctx -> new CrossClusterAccessHeaders(
                            "ApiKey " + encodedApiKey,
                            SystemUser.crossClusterAccessSubjectInfo(TransportVersion.current(), "node")
                        ).writeToContext(ctx, null)
                    ).accept(threadContext);
                    connection.sendRequest(requestId, action, request, options);
                }
            });
            service.start();
            success = true;
        } finally {
            if (success == false) {
                service.close();
            }
        }
        return service;
    }
}

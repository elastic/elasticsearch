/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.remoteaccess;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.elasticsearch.xpack.security.authz.RBACEngine;
import org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.UUIDs.randomBase64UUID;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class RemoteAccessHeadersRestIT extends SecurityOnTrialLicenseRestTestCase {
    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() throws IOException {
        createUser(REMOTE_SEARCH_USER, PASSWORD, List.of(REMOTE_SEARCH_ROLE));
        createIndex(adminClient(), "index-a", null, null, null);
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "remote_indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));
    }

    @After
    public void cleanup() throws IOException {
        // TODO combine with teardown?
        deleteUser(REMOTE_SEARCH_USER);
        deleteRole(REMOTE_SEARCH_ROLE);
        deleteIndex(adminClient(), "index-a");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testRemoteAccessHeadersSent() throws Exception {
        final String remoteNodeName = "remoteNode";
        final String clusterCredential = randomBase64UUID(random());
        final BlockingQueue<CapturedActionWithHeaders> capturedHeaders = ConcurrentCollections.newBlockingQueue();
        try (MockTransportService remoteTransport = startTransport(remoteNodeName, threadPool, capturedHeaders)) {
            final DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();
            final boolean useProxyMode = randomBoolean();
            if (useProxyMode) {
                updateRemoteClusterSettings(
                    Map.of("mode", "proxy", "proxy_address", remoteNode.getAddress().toString(), "authorization", clusterCredential)
                );
            } else {
                updateRemoteClusterSettings(Map.of("seeds", remoteNode.getAddress().toString(), "authorization", clusterCredential));
            }
            final boolean minimizeRoundtrips = randomBoolean();
            final Request searchRequest = new Request(
                "GET",
                "/my_remote_cluster:index-a/_search?ccs_minimize_roundtrips=" + (minimizeRoundtrips ? "true" : "false")
            );
            searchRequest.setOptions(
                searchRequest.getOptions()
                    .toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
            );

            assertOK(client().performRequest(searchRequest));

            final List<CapturedActionWithHeaders> actualHeaders = List.copyOf(capturedHeaders);
            final Set<String> expectedActions = minimizeRoundtrips
                ? Set.of(ClusterStateAction.NAME, SearchAction.NAME)
                : Set.of(ClusterStateAction.NAME, ClusterSearchShardsAction.NAME);
            assertThat(
                actualHeaders.stream().map(CapturedActionWithHeaders::action).collect(Collectors.toUnmodifiableSet()),
                equalTo(expectedActions)
            );
            for (CapturedActionWithHeaders actual : actualHeaders) {
                switch (actual.action) {
                    // the cluster state action is run by the system user, so we expect an authentication header, instead of remote access
                    // until we implement remote access handling for internal users
                    case ClusterStateAction.NAME -> {
                        assertThat(actual.headers().keySet(), contains(AuthenticationField.AUTHENTICATION_KEY));
                        assertThat(
                            decodeAuthentication(actual.headers().get(AuthenticationField.AUTHENTICATION_KEY)).getEffectiveSubject()
                                .getUser(),
                            is(SystemUser.INSTANCE)
                        );
                    }
                    case SearchAction.NAME, ClusterSearchShardsAction.NAME -> {
                        assertContainsRemoteAccessHeaders(actual.headers());
                        assertThat(
                            actual.headers(),
                            hasEntry(
                                SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY,
                                "ApiKey " + clusterCredential
                            )
                        );
                        assertHasRemoteAccessAuthenticationHeader(
                            actual.headers(),
                            new RemoteAccessAuthentication(
                                Authentication.newRealmAuthentication(
                                    new User(REMOTE_SEARCH_USER, REMOTE_SEARCH_ROLE),
                                    // TODO deal with node name
                                    new Authentication.RealmRef("default_native", "native", "javaRestTest-0")
                                ),
                                new RoleDescriptorsIntersection(
                                    List.of(
                                        Set.of(
                                            new RoleDescriptor(
                                                RBACEngine.REMOTE_USER_ROLE_NAME,
                                                null,
                                                new RoleDescriptor.IndicesPrivileges[] {
                                                    RoleDescriptor.IndicesPrivileges.builder()
                                                        .indices("index-a")
                                                        .privileges("read", "read_cross_cluster")
                                                        .build() },
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null
                                            )
                                        )
                                    )
                                )
                            )
                        );
                    }
                    default -> fail("Unexpected action [" + actual.action + "]");
                }
            }
        }
    }

    private void assertContainsRemoteAccessHeaders(final Map<String, String> actualHeaders) {
        assertThat(
            actualHeaders.keySet(),
            containsInAnyOrder(
                RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY,
                SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY
            )
        );
    }

    private void assertHasRemoteAccessAuthenticationHeader(
        final Map<String, String> actualHeaders,
        final RemoteAccessAuthentication expectedRemoteAccessAuthentication
    ) throws IOException {
        final var actualRemoteAccessAuthentication = RemoteAccessAuthentication.decode(
            actualHeaders.get(RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY)
        );
        assertThat(
            actualRemoteAccessAuthentication.getAuthentication().getEffectiveSubject().getUser(),
            equalTo(expectedRemoteAccessAuthentication.getAuthentication().getEffectiveSubject().getUser())
        );
        assertThat(
            actualRemoteAccessAuthentication.getRoleDescriptorsBytesList(),
            equalTo(expectedRemoteAccessAuthentication.getRoleDescriptorsBytesList())
        );
    }

    private Authentication decodeAuthentication(final String rawAuthentication) throws IOException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, rawAuthentication);
        return Objects.requireNonNull(new AuthenticationContextSerializer().readFromContext(threadContext));
    }

    private static MockTransportService startTransport(
        final String nodeName,
        final ThreadPool threadPool,
        final BlockingQueue<CapturedActionWithHeaders> capturedHeaders
    ) {
        boolean success = false;
        final Settings settings = Settings.builder().put("node.name", nodeName).build();
        final ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        final MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
        try {
            service.registerRequestHandler(
                ClusterStateAction.NAME,
                ThreadPool.Names.SAME,
                ClusterStateRequest::new,
                (request, channel, task) -> {
                    capturedHeaders.add(
                        new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                    );
                    channel.sendResponse(new ClusterStateResponse(clusterName, ClusterState.builder(clusterName).build(), false));
                }
            );
            service.registerRequestHandler(
                ClusterSearchShardsAction.NAME,
                ThreadPool.Names.SAME,
                ClusterSearchShardsRequest::new,
                (request, channel, task) -> {
                    capturedHeaders.add(
                        new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                    );
                    channel.sendResponse(
                        new ClusterSearchShardsResponse(new ClusterSearchShardsGroup[0], new DiscoveryNode[0], Collections.emptyMap())
                    );
                }
            );
            service.registerRequestHandler(SearchAction.NAME, ThreadPool.Names.SAME, SearchRequest::new, (request, channel, task) -> {
                capturedHeaders.add(
                    new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                );
                channel.sendResponse(
                    new SearchResponse(
                        new InternalSearchResponse(
                            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                            InternalAggregations.EMPTY,
                            null,
                            null,
                            false,
                            null,
                            1
                        ),
                        null,
                        1,
                        1,
                        0,
                        100,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY
                    )
                );
            });
            service.start();
            service.acceptIncomingRequests();
            success = true;
            return service;
        } finally {
            if (success == false) {
                service.close();
            }
        }
    }

    private record CapturedActionWithHeaders(String action, Map<String, String> headers) {}

    private static void updateRemoteClusterSettings(final Map<String, Object> settings) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setEntity(buildUpdateSettingsRequestBody(settings));
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static HttpEntity buildUpdateSettingsRequestBody(final Map<String, Object> settings) throws IOException {
        final String requestBody;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("persistent");
                {
                    builder.startObject("cluster.remote.my_remote_cluster");
                    {
                        for (Map.Entry<String, Object> entry : settings.entrySet()) {
                            builder.field(entry.getKey(), entry.getValue());
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            requestBody = Strings.toString(builder);
        }
        return new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    }
}

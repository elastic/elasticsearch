/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.crossclusteraccess;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchShardsAction;
import org.elasticsearch.action.search.SearchShardsRequest;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.user.CrossClusterAccessUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrossClusterAccessHeadersForCcsRestIT extends SecurityOnTrialLicenseRestTestCase {
    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    private static final String CLUSTER_A = "my_remote_cluster_a";
    private static final String CLUSTER_B = "my_remote_cluster_b";
    private static final String CLUSTER_A_CREDENTIALS = "cluster_a_credentials";
    private static final String CLUSTER_B_CREDENTIALS = "cluster_b_credentials";
    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final SecureString PASSWORD = new SecureString("super-secret-password".toCharArray());
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() throws IOException {
        createUser(REMOTE_SEARCH_USER, PASSWORD, List.of(REMOTE_SEARCH_ROLE));

        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": ["manage_api_key"],
              "indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["read"]
                }
              ],
              "remote_indices": [
                {
                  "names": ["index-a"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster*"]
                },
                {
                  "names": ["index-b"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster_b"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        final var indexDocRequest = new Request("POST", "/index-a/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
        assertOK(adminClient().performRequest(indexDocRequest));
    }

    @After
    public void cleanup() throws IOException {
        deleteUser(REMOTE_SEARCH_USER);
        deleteRole(REMOTE_SEARCH_ROLE);
        deleteIndex(adminClient(), "index-a");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testCrossClusterAccessHeadersSentSingleRemote() throws Exception {
        final BlockingQueue<CapturedActionWithHeaders> capturedHeaders = ConcurrentCollections.newBlockingQueue();
        try (MockTransportService remoteTransport = startTransport("remoteNodeA", threadPool, capturedHeaders)) {
            final TransportAddress remoteAddress = remoteTransport.getOriginalTransport()
                .profileBoundAddresses()
                .get("_remote_cluster")
                .publishAddress();
            final boolean useProxyMode = randomBoolean();
            setupClusterSettings(CLUSTER_A, remoteAddress, useProxyMode);
            final boolean alsoSearchLocally = randomBoolean();
            final boolean minimizeRoundtrips = randomBoolean();
            final Request searchRequest = new Request(
                "GET",
                Strings.format(
                    "/%s%s:index-a/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "index-a," : "",
                    CLUSTER_A,
                    minimizeRoundtrips
                )
            );
            searchRequest.setOptions(
                searchRequest.getOptions()
                    .toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
            );

            final Response response = client().performRequest(searchRequest);
            assertOK(response);
            assertThat(ObjectPath.createFromResponse(response).evaluate("hits.total.value"), equalTo(alsoSearchLocally ? 1 : 0));

            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeaders),
                useProxyMode,
                minimizeRoundtrips,
                CLUSTER_A_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesNativeUser,
                new RoleDescriptorsIntersection(
                    new RoleDescriptor(
                        Role.REMOTE_USER_ROLE_NAME,
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
            );
        }
    }

    public void testCrossClusterAccessHeadersSentMultipleRemotes() throws Exception {
        final Map<String, BlockingQueue<CapturedActionWithHeaders>> capturedHeadersByCluster = Map.of(
            CLUSTER_A,
            ConcurrentCollections.newBlockingQueue(),
            CLUSTER_B,
            ConcurrentCollections.newBlockingQueue()
        );
        try (
            MockTransportService remoteTransportA = startTransport("remoteNodeA", threadPool, capturedHeadersByCluster.get(CLUSTER_A));
            MockTransportService remoteTransportB = startTransport("remoteNodeB", threadPool, capturedHeadersByCluster.get(CLUSTER_B))
        ) {
            final boolean useProxyModeA = randomBoolean();
            setupClusterSettings(
                CLUSTER_A,
                remoteTransportA.getOriginalTransport().profileBoundAddresses().get("_remote_cluster").publishAddress(),
                useProxyModeA
            );

            final boolean useProxyModeB = randomBoolean();
            setupClusterSettings(
                CLUSTER_B,
                remoteTransportB.getOriginalTransport().profileBoundAddresses().get("_remote_cluster").publishAddress(),
                useProxyModeB
            );

            final boolean minimizeRoundtrips = randomBoolean();
            final Request searchRequest = new Request(
                "GET",
                Strings.format("/%s:index-a,%s:index-*/_search?ccs_minimize_roundtrips=%s", CLUSTER_A, CLUSTER_B, minimizeRoundtrips)
            );
            searchRequest.setOptions(
                searchRequest.getOptions()
                    .toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
            );

            final Response response = client().performRequest(searchRequest);
            assertOK(response);
            assertThat(ObjectPath.createFromResponse(response).evaluate("hits.total.value"), equalTo(0));

            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeadersByCluster.get(CLUSTER_A)),
                useProxyModeA,
                minimizeRoundtrips,
                CLUSTER_A_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesNativeUser,
                new RoleDescriptorsIntersection(
                    new RoleDescriptor(
                        Role.REMOTE_USER_ROLE_NAME,
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
            );
            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeadersByCluster.get(CLUSTER_B)),
                useProxyModeB,
                minimizeRoundtrips,
                CLUSTER_B_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesNativeUser,
                new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder()
                                        .indices("index-a")
                                        .privileges("read", "read_cross_cluster")
                                        .build(),
                                    RoleDescriptor.IndicesPrivileges.builder()
                                        .indices("index-b")
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
            );
        }
    }

    public void testApiKeyCrossClusterAccessHeadersSentMultipleRemotes() throws Exception {
        final Tuple<String, String> apiKeyTuple = createOrGrantApiKey("""
            {
              "name": "my-api-key",
              "role_descriptors": {
                "role-a": {
                  "index": [
                    {
                      "names": ["index-a*"],
                      "privileges": ["all"]
                    }
                  ],
                  "remote_indices": [
                    {
                      "names": ["index-a*"],
                      "privileges": ["all"],
                      "clusters": ["my_remote_cluster*"]
                    }
                  ]
                },
                "role-b": {
                  "index": [
                    {
                      "names": ["index-b*"],
                      "privileges": ["all"]
                    }
                  ],
                  "remote_indices": [
                    {
                      "names": ["index-b*"],
                      "privileges": ["all"],
                      "clusters": ["my_remote_cluster_b"]
                    }
                  ]
                }
              }
            }
            """);

        final String apiKeyEncoded = apiKeyTuple.v2();

        final Map<String, BlockingQueue<CapturedActionWithHeaders>> capturedHeadersByCluster = Map.of(
            CLUSTER_A,
            ConcurrentCollections.newBlockingQueue(),
            CLUSTER_B,
            ConcurrentCollections.newBlockingQueue()
        );
        try (
            MockTransportService remoteTransportA = startTransport("remoteNodeA", threadPool, capturedHeadersByCluster.get(CLUSTER_A));
            MockTransportService remoteTransportB = startTransport("remoteNodeB", threadPool, capturedHeadersByCluster.get(CLUSTER_B))
        ) {
            final boolean useProxyModeA = randomBoolean();
            setupClusterSettings(
                CLUSTER_A,
                remoteTransportA.getOriginalTransport().profileBoundAddresses().get("_remote_cluster").publishAddress(),
                useProxyModeA
            );

            final boolean useProxyModeB = randomBoolean();
            setupClusterSettings(
                CLUSTER_B,
                remoteTransportB.getOriginalTransport().profileBoundAddresses().get("_remote_cluster").publishAddress(),
                useProxyModeB
            );

            final boolean minimizeRoundtrips = randomBoolean();
            final Request searchRequest = new Request(
                "GET",
                Strings.format("/%s:index-a,%s:index-*/_search?ccs_minimize_roundtrips=%s", CLUSTER_A, CLUSTER_B, minimizeRoundtrips)
            );
            searchRequest.setOptions(searchRequest.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + apiKeyEncoded));

            final Response response = client().performRequest(searchRequest);
            assertOK(response);
            assertThat(ObjectPath.createFromResponse(response).evaluate("hits.total.value"), equalTo(0));

            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeadersByCluster.get(CLUSTER_A)),
                useProxyModeA,
                minimizeRoundtrips,
                CLUSTER_A_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesApiKey,
                new RoleDescriptorsIntersection(
                    List.of(
                        // Base API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index-a*").privileges("all").build() },
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        ),
                        // Limited by API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
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
            );
            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeadersByCluster.get(CLUSTER_B)),
                useProxyModeB,
                minimizeRoundtrips,
                CLUSTER_B_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesApiKey,
                new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index-a*").privileges("all").build(),
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index-b*").privileges("all").build() },
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        ),
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder()
                                        .indices("index-a")
                                        .privileges("read", "read_cross_cluster")
                                        .build(),
                                    RoleDescriptor.IndicesPrivileges.builder()
                                        .indices("index-b")
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
            );
        }
    }

    public void testApiKeyCrossClusterAccessHeadersSentSingleRemote() throws Exception {
        final boolean createApiKeyWithRoleDescriptors = randomBoolean();
        final Tuple<String, String> apiKeyTuple; // id, encoded
        if (createApiKeyWithRoleDescriptors) {
            apiKeyTuple = createOrGrantApiKey("""
                {
                  "name": "my-api-key",
                  "role_descriptors": {
                    "role-a": {
                      "index": [
                        {
                          "names": ["index-a*"],
                          "privileges": ["all"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["index-a*"],
                          "privileges": ["all"],
                          "clusters": ["my_remote_cluster*"]
                        }
                      ]
                    },
                    "role-b": {
                      "index": [
                        {
                          "names": ["index-b*"],
                          "privileges": ["all"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["index-b*"],
                          "privileges": ["all"],
                          "clusters": ["my_remote_cluster_b"]
                        }
                      ]
                    }
                  }
                }
                """);
        } else {
            apiKeyTuple = createOrGrantApiKey("""
                {
                  "name": "my-api-key"
                }
                """);
        }

        final String apiKeyId = apiKeyTuple.v1();
        final String apiKeyEncoded = apiKeyTuple.v2();

        {
            final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection;
            if (createApiKeyWithRoleDescriptors) {
                expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                    List.of(
                        // Base API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index-a*").privileges("all").build() },
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        ),
                        // Limited by API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
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
                );
            } else {
                expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
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
                );
            }
            testCcsWithApiKeyCrossClusterAccessAuthenticationAgainstSingleCluster(
                CLUSTER_A + "_1",
                apiKeyEncoded,
                expectedRoleDescriptorsIntersection
            );
        }

        // updating API key to test opposite
        // -> if we created API key with role descriptors, then we test authentication after removing them and vice versa
        boolean updateApiKeyWithRoleDescriptors = createApiKeyWithRoleDescriptors == false;
        if (updateApiKeyWithRoleDescriptors) {
            updateOrBulkUpdateApiKey(apiKeyId, """
                 {
                    "role-a": {
                      "index": [
                        {
                          "names": ["index-a*"],
                          "privileges": ["all"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["index-a*"],
                          "privileges": ["all"],
                          "clusters": ["my_remote_cluster*"]
                        }
                      ]
                    },
                    "role-b": {
                      "index": [
                        {
                          "names": ["index-b*"],
                          "privileges": ["all"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["index-b*"],
                          "privileges": ["all"],
                          "clusters": ["my_remote_cluster_b"]
                        }
                      ]
                    }
                 }
                """);
        } else {
            updateOrBulkUpdateApiKey(apiKeyId, """
                 { }
                """);
        }

        {
            final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection;
            if (updateApiKeyWithRoleDescriptors) {
                expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                    List.of(
                        // Base API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index-a*").privileges("all").build() },
                                null,
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        ),
                        // Limited by API key role
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
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
                );
            } else {
                expectedRoleDescriptorsIntersection = new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                Role.REMOTE_USER_ROLE_NAME,
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
                );
            }
            testCcsWithApiKeyCrossClusterAccessAuthenticationAgainstSingleCluster(
                CLUSTER_A + "_2",
                apiKeyEncoded,
                expectedRoleDescriptorsIntersection
            );
        }
    }

    public void testRemoteClustersXPackUsage() throws IOException {
        try (MockTransportService remoteTransport = startTransport("remoteNodeA", threadPool, ConcurrentCollections.newBlockingQueue())) {
            final TransportAddress transportPortAddress = remoteTransport.getOriginalTransport().boundAddress().publishAddress();
            final TransportAddress remoteClusterServerPortAddress = remoteTransport.getOriginalTransport()
                .boundRemoteIngressAddress()
                .publishAddress();
            final int numberOfRemoteClusters = randomIntBetween(0, 5);
            final int numberOfApiKeySecured = randomIntBetween(0, Math.min(2, numberOfRemoteClusters));
            final int numberOfCertSecured = numberOfRemoteClusters - numberOfApiKeySecured;
            final List<Boolean> useProxyModes = randomList(numberOfRemoteClusters, numberOfRemoteClusters, ESTestCase::randomBoolean);

            // Remote clusters with new configurable model
            switch (numberOfApiKeySecured) {
                case 0 -> {
                }
                case 1 -> setupClusterSettings(CLUSTER_A, remoteClusterServerPortAddress, useProxyModes.get(0));
                case 2 -> {
                    setupClusterSettings(CLUSTER_A, remoteClusterServerPortAddress, useProxyModes.get(0));
                    setupClusterSettings(CLUSTER_B, remoteClusterServerPortAddress, useProxyModes.get(1));
                }
                default -> throw new IllegalArgumentException("invalid number of api_key secured remote clusters");
            }

            // Remote clusters with basic model
            for (int i = 0; i < numberOfCertSecured; i++) {
                setupClusterSettings("basic_cluster_" + i, transportPortAddress, useProxyModes.get(i + numberOfApiKeySecured));
            }

            final Request xPackUsageRequest = new Request("GET", "/_xpack/usage");
            final Response xPackUsageResponse = adminClient().performRequest(xPackUsageRequest);
            assertOK(xPackUsageResponse);
            final ObjectPath path = ObjectPath.createFromResponse(xPackUsageResponse);

            assertThat(path.evaluate("remote_clusters.size"), equalTo(numberOfRemoteClusters));
            final int numberOfProxyModes = (int) useProxyModes.stream().filter(e -> e).count();
            assertThat(path.evaluate("remote_clusters.mode.proxy"), equalTo(numberOfProxyModes));
            assertThat(path.evaluate("remote_clusters.mode.sniff"), equalTo(numberOfRemoteClusters - numberOfProxyModes));
            assertThat(path.evaluate("remote_clusters.security.cert"), equalTo(numberOfCertSecured));
            assertThat(path.evaluate("remote_clusters.security.api_key"), equalTo(numberOfApiKeySecured));

            assertThat(path.evaluate("security.remote_cluster_server.available"), is(true));
            assertThat(path.evaluate("security.remote_cluster_server.enabled"), is(false));
            assertThat(path.evaluate("security.ssl.remote_cluster_server.enabled"), nullValue());
            assertThat(path.evaluate("security.ssl.remote_cluster_client.enabled"), is(false));
        }
    }

    private void testCcsWithApiKeyCrossClusterAccessAuthenticationAgainstSingleCluster(
        String cluster,
        String apiKeyEncoded,
        RoleDescriptorsIntersection expectedRoleDescriptorsIntersection
    ) throws IOException {
        final BlockingQueue<CapturedActionWithHeaders> capturedHeaders = ConcurrentCollections.newBlockingQueue();
        try (MockTransportService remoteTransport = startTransport("remoteNode-" + cluster, threadPool, capturedHeaders)) {
            final TransportAddress remoteAddress = remoteTransport.getOriginalTransport()
                .profileBoundAddresses()
                .get("_remote_cluster")
                .publishAddress();
            final boolean useProxyMode = randomBoolean();
            setupClusterSettings(cluster, remoteAddress, useProxyMode);
            final boolean alsoSearchLocally = randomBoolean();
            final boolean minimizeRoundtrips = randomBoolean();
            final Request searchRequest = new Request(
                "GET",
                Strings.format(
                    "/%s%s:index-a/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "index-a," : "",
                    cluster,
                    minimizeRoundtrips
                )
            );
            searchRequest.setOptions(searchRequest.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + apiKeyEncoded));

            final Response response = client().performRequest(searchRequest);
            assertOK(response);
            assertThat(ObjectPath.createFromResponse(response).evaluate("hits.total.value"), equalTo(alsoSearchLocally ? 1 : 0));

            expectActionsAndHeadersForCluster(
                List.copyOf(capturedHeaders),
                useProxyMode,
                minimizeRoundtrips,
                CLUSTER_A_CREDENTIALS,
                this::assertCrossClusterAccessSubjectInfoMatchesApiKey,
                expectedRoleDescriptorsIntersection
            );
        }
    }

    private Tuple<String, String> createOrGrantApiKey(String body) throws IOException {
        final Request createApiKeyRequest;
        final boolean grantApiKey = randomBoolean();
        if (grantApiKey) {
            createApiKeyRequest = new Request("POST", "/_security/api_key/grant");
            createApiKeyRequest.setJsonEntity(Strings.format("""
                    {
                        "grant_type" : "password",
                        "username"   : "%s",
                        "password"   : "%s",
                        "api_key"    :  %s
                    }
                """, REMOTE_SEARCH_USER, PASSWORD, body));
        } else {
            createApiKeyRequest = new Request("POST", "_security/api_key");
            createApiKeyRequest.setJsonEntity(body);
            createApiKeyRequest.setOptions(
                createApiKeyRequest.getOptions()
                    .toBuilder()
                    .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
            );
        }

        final Response createApiKeyResponse;
        if (grantApiKey) {
            createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        } else {
            createApiKeyResponse = client().performRequest(createApiKeyRequest);
        }
        assertOK(createApiKeyResponse);

        final ObjectPath path = ObjectPath.createFromResponse(createApiKeyResponse);
        final String apiKeyEncoded = path.evaluate("encoded");
        final String apiKeyId = path.evaluate("id");
        assertThat(apiKeyEncoded, notNullValue());
        assertThat(apiKeyId, notNullValue());

        return Tuple.tuple(apiKeyId, apiKeyEncoded);
    }

    private void updateOrBulkUpdateApiKey(String id, String roleDescriptors) throws IOException {
        final Request updateApiKeyRequest;
        final boolean bulkUpdate = randomBoolean();
        if (bulkUpdate) {
            updateApiKeyRequest = new Request("POST", "_security/api_key/_bulk_update");
            updateApiKeyRequest.setJsonEntity(Strings.format("""
                {
                    "ids": [ "%s" ],
                    "role_descriptors": %s
                }
                """, id, roleDescriptors));
        } else {
            updateApiKeyRequest = new Request("PUT", "_security/api_key/" + id);
            updateApiKeyRequest.setJsonEntity(Strings.format("""
                {
                    "role_descriptors": %s
                }
                """, roleDescriptors));
        }

        updateApiKeyRequest.setOptions(
            updateApiKeyRequest.getOptions()
                .toBuilder()
                .addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(REMOTE_SEARCH_USER, PASSWORD))
        );

        final Response updateApiKeyResponse = client().performRequest(updateApiKeyRequest);
        assertOK(updateApiKeyResponse);

        if (bulkUpdate) {
            List<String> updated = ObjectPath.createFromResponse(updateApiKeyResponse).evaluate("updated");
            assertThat(updated.size(), equalTo(1));
            assertThat(updated.get(0), equalTo(id));
        } else {
            boolean updated = ObjectPath.createFromResponse(updateApiKeyResponse).evaluate("updated");
            assertThat(updated, equalTo(true));
        }
    }

    private void setupClusterSettings(final String clusterAlias, final TransportAddress remoteAddress, boolean useProxyMode)
        throws IOException {
        if (useProxyMode) {
            updateRemoteClusterSettings(clusterAlias, Map.of("mode", "proxy", "proxy_address", remoteAddress.toString()));
        } else {
            updateRemoteClusterSettings(clusterAlias, Map.of("seeds", remoteAddress.toString()));
        }
    }

    private void assertCrossClusterAccessSubjectInfoMatchesNativeUser(
        final CrossClusterAccessSubjectInfo actualCrossClusterAccessSubjectInfo,
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection
    ) {
        try {
            final CrossClusterAccessSubjectInfo expectedCrossClusterAccessSubjectInfo = new CrossClusterAccessSubjectInfo(
                Authentication.newRealmAuthentication(
                    new User(REMOTE_SEARCH_USER, REMOTE_SEARCH_ROLE),
                    new Authentication.RealmRef(
                        "default_native",
                        "native",
                        // Since we are running on a multi-node cluster the actual node name may be different between runs
                        // so just copy the one from the actual result
                        actualCrossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getRealm().getNodeName()
                    )
                ),
                expectedRoleDescriptorsIntersection
            );
            assertThat(actualCrossClusterAccessSubjectInfo, equalTo(expectedCrossClusterAccessSubjectInfo));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void assertCrossClusterAccessSubjectInfoMatchesApiKey(
        final CrossClusterAccessSubjectInfo actualCrossClusterAccessSubjectInfo,
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection
    ) {
        try {
            assertThat(
                actualCrossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getRealm(),
                equalTo(
                    new Authentication.RealmRef(
                        AuthenticationField.API_KEY_REALM_NAME,
                        AuthenticationField.API_KEY_REALM_TYPE,
                        // Since we are running on a multi-node cluster the actual node name may be different between runs
                        // so just copy the one from the actual result
                        actualCrossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getRealm().getNodeName()
                    )
                )
            );
            assertThat(
                actualCrossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getUser().principal(),
                equalTo(REMOTE_SEARCH_USER)
            );
            assertThat(
                actualCrossClusterAccessSubjectInfo.getRoleDescriptorsBytesList(),
                equalTo(toRoleDescriptorsBytesList(expectedRoleDescriptorsIntersection))
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<CrossClusterAccessSubjectInfo.RoleDescriptorsBytes> toRoleDescriptorsBytesList(
        final RoleDescriptorsIntersection roleDescriptorsIntersection
    ) throws IOException {
        final List<CrossClusterAccessSubjectInfo.RoleDescriptorsBytes> roleDescriptorsBytesList = new ArrayList<>();
        for (Set<RoleDescriptor> roleDescriptors : roleDescriptorsIntersection.roleDescriptorsList()) {
            roleDescriptorsBytesList.add(CrossClusterAccessSubjectInfo.RoleDescriptorsBytes.fromRoleDescriptors(roleDescriptors));
        }
        return roleDescriptorsBytesList;
    }

    private void expectActionsAndHeadersForCluster(
        final List<CapturedActionWithHeaders> actualActionsWithHeaders,
        boolean useProxyMode,
        boolean minimizeRoundtrips,
        final String encodedCredential,
        final BiConsumer<CrossClusterAccessSubjectInfo, RoleDescriptorsIntersection> crossClusterAccessSubjectInfoChecker,
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection
    ) throws IOException {
        final Set<String> expectedActions = new HashSet<>();
        if (minimizeRoundtrips) {
            expectedActions.add(SearchAction.NAME);
        } else {
            expectedActions.add(SearchShardsAction.NAME);
        }
        if (false == useProxyMode) {
            expectedActions.add(RemoteClusterNodesAction.NAME);
        }
        assertThat(
            actualActionsWithHeaders.stream().map(CapturedActionWithHeaders::action).collect(Collectors.toUnmodifiableSet()),
            equalTo(expectedActions)
        );
        for (CapturedActionWithHeaders actual : actualActionsWithHeaders) {
            switch (actual.action) {
                // this action is run by the cross cluster access user, so we expect a cross cluster access header with an internal user
                // authentication and pre-defined role descriptors intersection
                case RemoteClusterNodesAction.NAME -> {
                    // requests by internal users don't include an audit request ID; this is a current side effect of the audit setup where
                    // if the thread context is stashed, we don't persist the audit request ID by default
                    assertContainsHeadersExpectedForCrossClusterAccess(actual.headers(), false);
                    assertContainsCrossClusterAccessCredentialsHeader(encodedCredential, actual);
                    final var actualCrossClusterAccessSubjectInfo = CrossClusterAccessSubjectInfo.decode(
                        actual.headers().get(CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
                    );
                    final var expectedCrossClusterAccessSubjectInfo = CrossClusterAccessUser.subjectInfo(
                        TransportVersion.CURRENT,
                        // Since we are running on a multi-node cluster the actual node name may be different between runs
                        // so just copy the one from the actual result
                        actualCrossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getRealm().getNodeName()
                    );
                    assertThat(actualCrossClusterAccessSubjectInfo, equalTo(expectedCrossClusterAccessSubjectInfo));
                }
                case SearchAction.NAME, SearchShardsAction.NAME -> {
                    assertContainsHeadersExpectedForCrossClusterAccess(actual.headers());
                    assertContainsCrossClusterAccessCredentialsHeader(encodedCredential, actual);
                    final var actualCrossClusterAccessSubjectInfo = CrossClusterAccessSubjectInfo.decode(
                        actual.headers().get(CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY)
                    );
                    crossClusterAccessSubjectInfoChecker.accept(actualCrossClusterAccessSubjectInfo, expectedRoleDescriptorsIntersection);
                }
                default -> fail("Unexpected action [" + actual.action + "]");
            }
        }
    }

    private void assertContainsCrossClusterAccessCredentialsHeader(String encodedCredential, CapturedActionWithHeaders actual) {
        assertThat(actual.headers(), hasKey(CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY));
        assertThat(
            actual.headers().get(CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY),
            equalTo(ApiKeyService.withApiKeyPrefix(encodedCredential))
        );
    }

    private static MockTransportService startTransport(
        final String nodeName,
        final ThreadPool threadPool,
        final BlockingQueue<CapturedActionWithHeaders> capturedHeaders
    ) {
        boolean success = false;
        final Settings settings = Settings.builder()
            .put("node.name", nodeName)
            .put("remote_cluster_server.enabled", "true")
            .put("remote_cluster.port", "0")
            .put("xpack.security.remote_cluster_server.ssl.enabled", "false")
            .build();
        final MockTransportService service = MockTransportService.createNewService(
            settings,
            Version.CURRENT,
            TransportVersion.CURRENT,
            threadPool,
            null
        );
        try {
            service.registerRequestHandler(
                ClusterStateAction.NAME,
                ThreadPool.Names.SAME,
                ClusterStateRequest::new,
                (request, channel, task) -> {
                    capturedHeaders.add(
                        new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                    );
                    channel.sendResponse(
                        new ClusterStateResponse(ClusterName.DEFAULT, ClusterState.builder(ClusterName.DEFAULT).build(), false)
                    );
                }
            );
            service.registerRequestHandler(
                RemoteClusterNodesAction.NAME,
                ThreadPool.Names.SAME,
                RemoteClusterNodesAction.Request::new,
                (request, channel, task) -> {
                    capturedHeaders.add(
                        new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                    );
                    channel.sendResponse(new RemoteClusterNodesAction.Response(List.of()));
                }
            );
            service.registerRequestHandler(
                SearchShardsAction.NAME,
                ThreadPool.Names.SAME,
                SearchShardsRequest::new,
                (request, channel, task) -> {
                    capturedHeaders.add(
                        new CapturedActionWithHeaders(task.getAction(), Map.copyOf(threadPool.getThreadContext().getHeaders()))
                    );
                    channel.sendResponse(new SearchShardsResponse(List.of(), List.of(), Collections.emptyMap()));
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

    private void assertContainsHeadersExpectedForCrossClusterAccess(final Map<String, String> actualHeaders) {
        assertContainsHeadersExpectedForCrossClusterAccess(actualHeaders, true);
    }

    private void assertContainsHeadersExpectedForCrossClusterAccess(final Map<String, String> actualHeaders, boolean includeRequestId) {
        assertThat(
            actualHeaders.keySet(),
            includeRequestId
                ? containsInAnyOrder(
                    CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY,
                    CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY,
                    AuditUtil.AUDIT_REQUEST_ID
                )
                : containsInAnyOrder(
                    CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY,
                    CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                )
        );
        if (includeRequestId) {
            assertThat(actualHeaders.get(AuditUtil.AUDIT_REQUEST_ID), not(emptyOrNullString()));
        }
    }

    private record CapturedActionWithHeaders(String action, Map<String, String> headers) {}

    private static void updateRemoteClusterSettings(final String clusterAlias, final Map<String, Object> settings) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        request.setEntity(buildUpdateSettingsRequestBody(clusterAlias, settings));
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    private static HttpEntity buildUpdateSettingsRequestBody(final String clusterAlias, final Map<String, Object> settings)
        throws IOException {
        final String requestBody;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startObject("persistent");
                {
                    builder.startObject("cluster.remote." + clusterAlias);
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

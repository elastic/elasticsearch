/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRCS2FailureStoreRestIT extends AbstractRemoteClusterSecurityFailureStoreRestIT {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                API_KEY_MAP_REF.compareAndSet(null, createCrossClusterAccessApiKey("""
                    {
                        "search": [
                          {
                              "names": ["test*"]
                          }
                        ]
                    }"""));
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRCS2CrossClusterSearch() throws Exception {
        // configure remote cluster using API Key-based authentication
        configureRemoteCluster();
        final String crossClusterAccessApiKeyId = (String) API_KEY_MAP_REF.get().get("id");
        final boolean ccsMinimizeRoundtrips = randomBoolean();

        // fulfilling cluster setup
        setupTestDataStreamOnFulfillingCluster();

        // query cluster setup
        setupLocalDataOnQueryCluster();
        setupUserAndRoleOnQueryCluster();

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String backingDataIndexName = backingIndices.v1();
        final String backingFailureIndexName = backingIndices.v2();
        {
            // query remote cluster without selectors should succeed
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s&ignore_unavailable=false",
                    alsoSearchLocally ? "local_index," : "",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1", "test*", "*", backingDataIndexName),
                    ccsMinimizeRoundtrips
                )
            );
            final String[] expectedIndices = alsoSearchLocally
                ? new String[] { "local_index", backingDataIndexName }
                : new String[] { backingDataIndexName };
            assertSearchResponseContainsIndices(performRequestWithRemoteSearchUser(dataSearchRequest), expectedIndices);
        }
        {
            // query remote cluster using ::data selector should fail
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s&ignore_unavailable=false",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1::data", "test*::data", "*::data", "non-existing::data"),
                    ccsMinimizeRoundtrips
                )
            );
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(dataSearchRequest)
            );
            assertSelectorsNotSupported(exception);
        }
        {
            // query remote cluster using ::failures selector should fail
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(
                    new Request(
                        "GET",
                        String.format(
                            Locale.ROOT,
                            "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                            randomFrom("test1::failures", "test*::failures", "*::failures", "non-existing::failures"),
                            ccsMinimizeRoundtrips
                        )
                    )
                )
            );
            assertSelectorsNotSupported(exception);
        }
        {
            // direct access to backing failure index is not allowed - no explicit read privileges over .fs-* indices
            Request failureIndexSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                    backingFailureIndexName,
                    ccsMinimizeRoundtrips
                )
            );
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(failureIndexSearchRequest)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception.getMessage(),
                containsString(
                    "action ["
                        + (ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards")
                        + "] towards remote cluster is unauthorized for user [remote_search_user] "
                        + "with assigned roles [remote_search] authenticated by API key id ["
                        + crossClusterAccessApiKeyId
                        + "] of user [test_user] on indices ["
                        + backingFailureIndexName
                        + "], this action is granted by the index privileges ["
                        + (ccsMinimizeRoundtrips ? "read,all" : "view_index_metadata,manage,read_cross_cluster,all")
                        + "]"
                )
            );
        }
    }

    private static void setupLocalDataOnQueryCluster() throws IOException {
        // Index some documents, to use them in a mixed-cluster search
        final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
        assertOK(client().performRequest(indexDocRequest));
    }

    private static void setupUserAndRoleOnQueryCluster() throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "description": "Role with privileges for remote and local indices.",
              "indices": [
                {
                  "names": ["local_index"],
                  "privileges": ["read"]
                }
              ],
              "remote_indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));
        final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
        putUserRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles" : ["remote_search"]
            }""");
        assertOK(adminClient().performRequest(putUserRequest));
    }

}

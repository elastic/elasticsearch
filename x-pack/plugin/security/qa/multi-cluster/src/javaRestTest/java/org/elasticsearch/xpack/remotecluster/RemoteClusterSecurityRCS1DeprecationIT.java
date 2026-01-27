/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests the deprecation of RCS1.0 (certificate-based) security model.
 */
public class RemoteClusterSecurityRCS1DeprecationIT extends AbstractRemoteClusterSecurityTestCase {

    public static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").nodes(1).apply(commonClusterConfig).build();
        queryCluster = ElasticsearchCluster.local().nodes(1).name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testUsingRCS1GeneratesDeprecationWarning() throws Exception {
        final boolean rcs1 = true;
        final boolean useProxyMode = randomBoolean();
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, rcs1, useProxyMode, randomBoolean());

        {
            // Query cluster -> add role for test user
            var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["local_index"],
                      "privileges": ["read"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));

            // Query cluster -> create user and assign role
            var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Query cluster -> create test index
            var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Fulfilling cluster -> create test indices
            Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "secretindex" } }
                { "bar": "foo" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

            // Fulfilling cluster -> add role for remote search user
            var putRoleOnRemoteClusterRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleOnRemoteClusterRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["index*"],
                      "privileges": ["read", "read_cross_cluster"]
                    }
                  ]
                }""");
            assertOK(performRequestAgainstFulfillingCluster(putRoleOnRemoteClusterRequest));
        }
        {
            // perform a simple search request, so we can ensure the remote cluster is connected
            final Request searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:index1/_search?ccs_minimize_roundtrips=%s",
                    randomFrom(REMOTE_CLUSTER_ALIAS, "*", "my_remote_*"),
                    randomBoolean()
                )
            );
            assertOK(performRequestWithRemoteSearchUser(searchRequest));
        }
        {
            // verify that the deprecation warning is logged
            try (InputStream log = queryCluster.getNodeLog(0, LogType.DEPRECATION)) {
                Streams.readAllLines(
                    log,
                    line -> assertThat(
                        line,
                        containsString(
                            "The remote cluster connection to ["
                                + REMOTE_CLUSTER_ALIAS
                                + "] is using the certificate-based security model. "
                                + "The certificate-based security model is deprecated and will be removed in a future major version. "
                                + "Migrate the remote cluster from the certificate-based to the API key-based security model."
                        )
                    )
                );
            }
        }
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

}

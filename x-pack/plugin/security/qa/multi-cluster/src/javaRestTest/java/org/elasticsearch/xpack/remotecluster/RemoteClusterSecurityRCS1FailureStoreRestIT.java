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
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRCS1FailureStoreRestIT extends AbstractRemoteClusterSecurityFailureStoreRestIT {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(3)
            .apply(commonClusterConfig)
            .feature(FeatureFlag.FAILURE_STORE_ENABLED)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("query-cluster")
            .apply(commonClusterConfig)
            .feature(FeatureFlag.FAILURE_STORE_ENABLED)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRCS1CrossClusterSearch() throws Exception {
        final boolean rcs1Security = true;
        final boolean isProxyMode = randomBoolean();
        final boolean skipUnavailable = false; // we want to get actual failures and not skip and get empty results
        final boolean ccsMinimizeRoundtrips = randomBoolean();

        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, rcs1Security, isProxyMode, skipUnavailable);

        // fulfilling cluster setup
        setupRoleAndUserOnFulfillingCluster();
        setupTestDataStreamOnFulfillingCluster();

        // query cluster setup
        setupLocalDataOnQueryCluster();
        setupUserAndRoleOnQueryCluster();

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String backingDataIndexName = backingIndices.v1();
        final String backingFailureIndexName = backingIndices.v2();
        {
            // query remote cluster using ::data selector should succeed
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "local_index," : "",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1::data", "test1", "test*", "test*::data", "*", "*::data", backingDataIndexName),
                    ccsMinimizeRoundtrips
                )
            );
            final String[] expectedIndices = alsoSearchLocally
                ? new String[] { "local_index", backingDataIndexName }
                : new String[] { backingDataIndexName };
            assertSearchResponseContainsIndices(performRequestWithRemoteSearchUser(dataSearchRequest), expectedIndices);
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
                            "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s&ignore_unavailable=true",
                            randomFrom("test1::failures", "test*::failures", "*::failures"),
                            ccsMinimizeRoundtrips
                        )
                    )
                )
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(exception.getMessage(), containsString("failures selector is not supported with cross-cluster expressions"));
        }
        {
            // direct access to backing failure index is subject to the user's permissions
            // it might fail in some cases and work in others
            Request failureIndexSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                    backingFailureIndexName,
                    ccsMinimizeRoundtrips
                )
            );
            if (ccsMinimizeRoundtrips) {
                // this is a special case where indices:data/read/search will be sent to a remote cluster
                // and the request to backing failure store index will be authorized based on the datastream
                // which grants access to backing failure store indices (granted by read_failure_store privilege)
                // from a security perspective, this is a valid use case and there is no way to prevent this with RCS1 security model
                // since from the fulfilling cluster perspective this request is no different from any other local search request
                assertSearchResponseContainsIndices(performRequestWithRemoteSearchUser(failureIndexSearchRequest), backingFailureIndexName);
            } else {
                // in this case, the user does not have the necessary permissions to search the backing failure index
                // the request to failure store backing index is authorized based on the datastream
                // which does not grant access to the indices:admin/search/search_shards action
                // this action is granted by read_cross_cluster privilege which is currently
                // not supporting the failure backing indices (only data backing indices)
                final ResponseException exception = expectThrows(
                    ResponseException.class,
                    () -> performRequestWithRemoteSearchUser(failureIndexSearchRequest)
                );
                assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(
                    exception.getMessage(),
                    containsString(
                        "action [indices:admin/search/search_shards] is unauthorized for user [remote_search_user] "
                            + "with effective roles [remote_search] on indices ["
                            + backingFailureIndexName
                            + "], this action is granted by the index privileges [view_index_metadata,manage,read_cross_cluster,all]"
                    )
                );
            }
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
              "indices": [
                {
                  "names": ["local_index"],
                  "privileges": ["read"]
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

    private static void setupRoleAndUserOnFulfillingCluster() throws IOException {
        var putRoleOnFulfillingClusterRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleOnFulfillingClusterRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read", "read_cross_cluster", "read_failure_store"]
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(putRoleOnFulfillingClusterRequest));

        var putUserOnFulfillingClusterRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
        putUserOnFulfillingClusterRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles" : ["remote_search"]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(putUserOnFulfillingClusterRequest));
    }

}

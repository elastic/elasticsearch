/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
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

    private static final String ALL_ACCESS = "all_access";
    private static final String DATA_ACCESS = "data_access";
    private static final String FAILURE_STORE_ACCESS = "failure_store_access";
    private static final String MANAGE_FAILURE_STORE_ACCESS = "manage_failure_store_access";
    private static final String ONLY_READ_FAILURE_STORE_ACCESS = "only_read_failure_store_access";
    private static final String BACKING_FAILURE_STORE_INDEX_ACCESS = "backing_failure_store_index_access";
    private static final String BACKING_DATA_INDEX_ACCESS = "backing_data_index_access";

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

        final Tuple<String, String> otherBackingIndices = getSingleDataAndFailureIndices("other1");
        final String otherBackingDataIndexName = otherBackingIndices.v1();
        final String otherBackingFailureIndexName = otherBackingIndices.v2();

        testCcsWithDataSelectorNotSupported(ccsMinimizeRoundtrips);
        testCcsWithFailuresSelectorNotSupported(ccsMinimizeRoundtrips);
        testCcsWithoutSelectorsSupported(backingDataIndexName, ccsMinimizeRoundtrips);
        testSearchingUnauthorizedIndices(otherBackingFailureIndexName, otherBackingDataIndexName, ccsMinimizeRoundtrips);
        testSearchingWithAccessToAllIndices(ccsMinimizeRoundtrips, backingDataIndexName, otherBackingDataIndexName);
        testBackingFailureIndexAccess(ccsMinimizeRoundtrips, backingFailureIndexName);
        testBackingDataIndexAccess(ccsMinimizeRoundtrips, backingDataIndexName);
        testSearchingNonExistingIndices(ccsMinimizeRoundtrips);
        testResolveRemoteClustersIsUnauthorized();
    }

    private void testBackingDataIndexAccess(boolean ccsMinimizeRoundtrips, String backingDataIndexName) throws IOException {
        Request dataIndexSearchRequest = new Request(
            "GET",
            String.format(
                Locale.ROOT,
                "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                backingDataIndexName,
                ccsMinimizeRoundtrips
            )
        );
        assertSearchResponseContainsIndices(
            performRequestWithUser(BACKING_DATA_INDEX_ACCESS, dataIndexSearchRequest),
            backingDataIndexName
        );
    }

    private void testSearchingWithAccessToAllIndices(
        boolean ccsMinimizeRoundtrips,
        String backingDataIndexName,
        String otherBackingDataIndexName
    ) throws IOException {
        final boolean alsoSearchLocally = randomBoolean();
        final Request dataSearchRequest = new Request(
            "GET",
            String.format(
                Locale.ROOT,
                "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                alsoSearchLocally ? "local_index," : "",
                randomFrom("my_remote_cluster", "*", "my_remote_*"),
                "*",
                ccsMinimizeRoundtrips
            )
        );
        final String[] expectedIndices = alsoSearchLocally
            ? new String[] { "local_index", backingDataIndexName, otherBackingDataIndexName }
            : new String[] { backingDataIndexName, otherBackingDataIndexName };
        assertSearchResponseContainsIndices(performRequestWithUser(ALL_ACCESS, dataSearchRequest), expectedIndices);
    }

    private void testSearchingNonExistingIndices(boolean ccsMinimizeRoundtrips) {
        // searching non-existing index without permissions should result in 403
        {
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(
                    FAILURE_STORE_ACCESS,
                    new Request(
                        "GET",
                        String.format(
                            Locale.ROOT,
                            "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                            "non-existing-no-privileges",
                            ccsMinimizeRoundtrips
                        )
                    )
                )
            );
            final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
            assertActionUnauthorized(exception, FAILURE_STORE_ACCESS, action, "non-existing-no-privileges");
        }
        // searching non-existing index with permissions should result in 404
        {
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(
                    FAILURE_STORE_ACCESS,
                    new Request(
                        "GET",
                        String.format(
                            Locale.ROOT,
                            "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                            "non-existing-index",
                            ccsMinimizeRoundtrips
                        )
                    )
                )
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    private void testSearchingUnauthorizedIndices(
        String otherBackingFailureIndexName,
        String otherBackingDataIndexName,
        boolean ccsMinimizeRoundtrips
    ) {
        // try searching remote index for which user has no access
        final String indexToSearch = randomFrom("other1", otherBackingFailureIndexName, otherBackingDataIndexName);
        final ResponseException exception = expectThrows(
            ResponseException.class,
            () -> performRequestWithUser(
                FAILURE_STORE_ACCESS,
                new Request(
                    "GET",
                    String.format(
                        Locale.ROOT,
                        "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                        indexToSearch,
                        ccsMinimizeRoundtrips
                    )
                )
            )
        );
        final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
        assertActionUnauthorized(exception, FAILURE_STORE_ACCESS, action, indexToSearch);
    }

    private void testBackingFailureIndexAccess(boolean ccsMinimizeRoundtrips, String backingFailureIndexName) throws IOException {
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

        // user with access to all should be able to search the backing failure index
        assertSearchResponseContainsIndices(performRequestWithUser(ALL_ACCESS, failureIndexSearchRequest), backingFailureIndexName);

        // user with data only access should not be able to search the backing failure index
        {
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(DATA_ACCESS, failureIndexSearchRequest)
            );
            final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
            assertActionUnauthorized(exception, DATA_ACCESS, action, backingFailureIndexName);
        }

        // for user with access to failure store, it depends on the underlying action that is being sent to the remote cluster
        if (ccsMinimizeRoundtrips) {
            // this is a special case where indices:data/read/search will be sent to a remote cluster
            // and the request to backing failure store index will be authorized based on the datastream
            // which grants access to backing failure store indices (granted by read_failure_store privilege)
            // from a security perspective, this is a valid use case and there is no way to prevent this with RCS1 security model
            // since from the fulfilling cluster perspective this request is no different from any other local search request
            assertSearchResponseContainsIndices(
                performRequestWithUser(FAILURE_STORE_ACCESS, failureIndexSearchRequest),
                backingFailureIndexName
            );
        } else {
            // in this case, the user does not have the necessary permissions to search the backing failure index
            // the request to failure store backing index is authorized based on the datastream
            // which does not grant access to the indices:admin/search/search_shards action
            // this action is granted by read_cross_cluster privilege which is currently
            // not supporting the failure backing indices (only data backing indices)
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(FAILURE_STORE_ACCESS, failureIndexSearchRequest)
            );
            assertActionUnauthorized(exception, FAILURE_STORE_ACCESS, "indices:admin/search/search_shards", backingFailureIndexName);
        }

        // user with manage failure store access should be able to search the backing failure index
        assertSearchResponseContainsIndices(
            performRequestWithUser(MANAGE_FAILURE_STORE_ACCESS, failureIndexSearchRequest),
            backingFailureIndexName
        );

        assertSearchResponseContainsIndices(
            performRequestWithUser(BACKING_FAILURE_STORE_INDEX_ACCESS, failureIndexSearchRequest),
            backingFailureIndexName
        );

    }

    public void testCcsWithoutSelectorsSupported(String backingDataIndexName, boolean ccsMinimizeRoundtrips) throws IOException {
        final String[] users = { FAILURE_STORE_ACCESS, DATA_ACCESS };
        for (String user : users) {
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "local_index," : "",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1", "test*", "*", backingDataIndexName),
                    ccsMinimizeRoundtrips
                )
            );
            final String[] expectedIndices = alsoSearchLocally
                ? new String[] { "local_index", backingDataIndexName }
                : new String[] { backingDataIndexName };
            assertSearchResponseContainsIndices(performRequestWithUser(user, dataSearchRequest), expectedIndices);
        }
    }

    private void testCcsWithDataSelectorNotSupported(boolean ccsMinimizeRoundtrips) throws IOException {
        final String[] users = { FAILURE_STORE_ACCESS, DATA_ACCESS, ALL_ACCESS };
        for (String user : users) {
            // query remote cluster using ::data selector should not succeed
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1::data", "test*::data", "*::data", "non-existing::data"),
                    ccsMinimizeRoundtrips
                )
            );
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(user, dataSearchRequest)
            );
            assertSelectorsNotSupported(exception);
        }
    }

    private void testCcsWithFailuresSelectorNotSupported(boolean ccsMinimizeRoundtrips) {
        final String[] users = {
            FAILURE_STORE_ACCESS,
            DATA_ACCESS,
            ALL_ACCESS,
            MANAGE_FAILURE_STORE_ACCESS,
            BACKING_DATA_INDEX_ACCESS,
            BACKING_FAILURE_STORE_INDEX_ACCESS,
            ONLY_READ_FAILURE_STORE_ACCESS };
        for (String user : users) {
            // query remote cluster using ::failures selector should fail (regardless of the user's permissions)
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithUser(
                    user,
                    new Request(
                        "GET",
                        String.format(
                            Locale.ROOT,
                            "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s&ignore_unavailable=true",
                            randomFrom("test1::failures", "test*::failures", "*::failures", "other1::failures", "non-existing::failures"),
                            ccsMinimizeRoundtrips
                        )
                    )
                )
            );
            assertSelectorsNotSupported(exception);
        }
    }

    private void testResolveRemoteClustersIsUnauthorized() {
        // user with only read_failure_store access should not be able to resolve remote clusters
        var exc = expectThrows(
            ResponseException.class,
            () -> performRequestWithUser(ONLY_READ_FAILURE_STORE_ACCESS, new Request("GET", "/_resolve/cluster/" + REMOTE_CLUSTER_ALIAS))
        );
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            exc.getMessage(),
            containsString(
                "action ["
                    + "indices:admin/resolve/cluster"
                    + "] is unauthorized for user ["
                    + ONLY_READ_FAILURE_STORE_ACCESS
                    + "] "
                    + "with effective roles ["
                    + ONLY_READ_FAILURE_STORE_ACCESS
                    + "]"
            )
        );
    }

    private static void setupLocalDataOnQueryCluster() throws IOException {
        // Index some documents, to use them in a mixed-cluster search
        final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
        assertOK(client().performRequest(indexDocRequest));
    }

    private static void setupUserAndRoleOnQueryCluster() throws IOException {
        createRole(adminClient(), ALL_ACCESS, """
            {
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all"]
                }
              ]
            }""");
        createUser(adminClient(), ALL_ACCESS, PASS, ALL_ACCESS);
        // the role must simply exist on query cluster, the access is irrelevant,
        // but we here grant the access to local_index only to test mixed search
        createRole(adminClient(), FAILURE_STORE_ACCESS, """
            {
              "indices": [
                {
                  "names": ["local_index"],
                  "privileges": ["read"]
                }
              ]
            }""");
        createUser(adminClient(), FAILURE_STORE_ACCESS, PASS, FAILURE_STORE_ACCESS);
        createRole(adminClient(), DATA_ACCESS, """
            {
              "indices": [
                {
                  "names": ["local_index"],
                  "privileges": ["read"]
                }
              ]
            }""");
        createUser(adminClient(), DATA_ACCESS, PASS, DATA_ACCESS);
        createRole(adminClient(), MANAGE_FAILURE_STORE_ACCESS, """
            {
              "indices": [
                {
                  "names": ["local_index"],
                  "privileges": ["read"]
                }
              ]
            }""");
        createUser(adminClient(), MANAGE_FAILURE_STORE_ACCESS, PASS, MANAGE_FAILURE_STORE_ACCESS);

        createRole(adminClient(), ONLY_READ_FAILURE_STORE_ACCESS, """
            {
            }""");
        createUser(adminClient(), ONLY_READ_FAILURE_STORE_ACCESS, PASS, ONLY_READ_FAILURE_STORE_ACCESS);
        createRole(adminClient(), BACKING_FAILURE_STORE_INDEX_ACCESS, """
            {
            }""");
        createUser(adminClient(), BACKING_FAILURE_STORE_INDEX_ACCESS, PASS, BACKING_FAILURE_STORE_INDEX_ACCESS);
        createRole(adminClient(), BACKING_DATA_INDEX_ACCESS, """
            {
            }""");
        createUser(adminClient(), BACKING_DATA_INDEX_ACCESS, PASS, BACKING_DATA_INDEX_ACCESS);

    }

    private static void createRole(RestClient client, String role, String roleDescriptor) throws IOException {
        final Request putRoleRequest = new Request("PUT", "/_security/role/" + role);
        putRoleRequest.setJsonEntity(roleDescriptor);
        assertOK(client.performRequest(putRoleRequest));
    }

    private static void createUser(RestClient client, String user, SecureString password, String role) throws IOException {
        final Request putUserRequest = new Request("PUT", "/_security/user/" + user);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", password.toString(), role));
        assertOK(client.performRequest(putUserRequest));
    }

    private static void setupRoleAndUserOnFulfillingCluster() throws IOException {
        putRoleOnFulfillingCluster(DATA_ACCESS, """
            {
              "indices": [
                {
                  "names": ["test*"],
                  "privileges": ["read", "read_cross_cluster"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(DATA_ACCESS, DATA_ACCESS);

        putRoleOnFulfillingCluster(FAILURE_STORE_ACCESS, """
            {
              "indices": [
                {
                  "names": ["test*", "non-existing-index"],
                  "privileges": ["read", "read_cross_cluster", "read_failure_store"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(FAILURE_STORE_ACCESS, FAILURE_STORE_ACCESS);

        putRoleOnFulfillingCluster(MANAGE_FAILURE_STORE_ACCESS, """
            {
              "indices": [
                {
                  "names": ["test*", "non-existing-index"],
                  "privileges": ["manage_failure_store", "read_cross_cluster", "read_failure_store"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(MANAGE_FAILURE_STORE_ACCESS, MANAGE_FAILURE_STORE_ACCESS);

        putRoleOnFulfillingCluster(ALL_ACCESS, """
            {
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(ALL_ACCESS, ALL_ACCESS);

        putRoleOnFulfillingCluster(ONLY_READ_FAILURE_STORE_ACCESS, """
            {
              "indices": [
                {
                  "names": ["test*", "non-existing-index"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(ONLY_READ_FAILURE_STORE_ACCESS, ONLY_READ_FAILURE_STORE_ACCESS);

        putRoleOnFulfillingCluster(BACKING_DATA_INDEX_ACCESS, """
            {
              "indices": [
                {
                  "names": [".ds-test*"],
                  "privileges": ["read", "read_cross_cluster"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(BACKING_DATA_INDEX_ACCESS, BACKING_DATA_INDEX_ACCESS);

        putRoleOnFulfillingCluster(BACKING_FAILURE_STORE_INDEX_ACCESS, """
            {
              "indices": [
                {
                  "names": [".fs-test*"],
                  "privileges": ["read", "read_cross_cluster"]
                }
              ]
            }""");
        putUserOnFulfillingCluster(BACKING_FAILURE_STORE_INDEX_ACCESS, BACKING_FAILURE_STORE_INDEX_ACCESS);
    }

    private static void putRoleOnFulfillingCluster(String roleName, String roleDescriptor) throws IOException {
        Request request = new Request("PUT", "/_security/role/" + roleName);
        request.setJsonEntity(roleDescriptor);
        assertOK(performRequestAgainstFulfillingCluster(request));
    }

    private static void putUserOnFulfillingCluster(String user, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + user);
        request.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", PASS.toString(), role));
        assertOK(performRequestAgainstFulfillingCluster(request));
    }

    private static void assertActionUnauthorized(
        ResponseException exception,
        String userAndRole,
        String action,
        String backingFailureIndexName
    ) {
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            exception.getMessage(),
            containsString(
                "action ["
                    + action
                    + "] is unauthorized for user ["
                    + userAndRole
                    + "] "
                    + "with effective roles ["
                    + userAndRole
                    + "] on indices ["
                    + backingFailureIndexName
                    + "]"
            )
        );
    }
}

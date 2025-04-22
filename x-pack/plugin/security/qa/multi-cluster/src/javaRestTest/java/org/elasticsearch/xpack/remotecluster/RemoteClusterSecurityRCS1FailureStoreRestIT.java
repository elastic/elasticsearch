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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterSecurityRCS1FailureStoreRestIT extends AbstractRemoteClusterSecurityFailureStoreRestIT {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(3)
            .apply(commonClusterConfig)
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("query-cluster")
            .apply(commonClusterConfig)
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

    /**
     * Maps usernames to their role descriptors. The usernames are also used as role names.
     */
    private static final Map<String, String> usersAndRolesOnFulfillingCluster = Map.of(DATA_ACCESS, """
        {
          "indices": [
            {
              "names": ["test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""", FAILURE_STORE_ACCESS, """
        {
          "indices": [
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["read", "read_cross_cluster", "read_failure_store"]
            }
          ]
        }""", MANAGE_FAILURE_STORE_ACCESS, """
        {
          "indices": [
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["manage_failure_store", "read_cross_cluster", "read_failure_store"]
            }
          ]
        }""", ALL_ACCESS, """
        {
          "indices": [
            {
              "names": ["*"],
              "privileges": ["all"]
            }
          ]
        }""", ONLY_READ_FAILURE_STORE_ACCESS, """
        {
          "indices": [
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["read_failure_store"]
            }
          ]
        }""", BACKING_DATA_INDEX_ACCESS, """
        {
          "indices": [
            {
              "names": [".ds-test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""", BACKING_FAILURE_STORE_INDEX_ACCESS, """
        {
          "indices": [
            {
              "names": [".fs-test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""");

    /**
     * The role must simply exist on query cluster, the actual access is irrelevant for user authorization.
     * But we here grant access to the local and (for some also) remote indices to test mixed search
     * and API key authorization.
     */
    private static final Map<String, String> usersAndRolesOnQueryCluster = Map.of(DATA_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
             {
              "names": ["local_index"],
              "privileges": ["read"]
            },
            {
              "names": ["test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""", FAILURE_STORE_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
             {
              "names": ["local_index"],
              "privileges": ["read"]
            },
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["read", "read_cross_cluster", "read_failure_store"]
            }
          ]
        }""", MANAGE_FAILURE_STORE_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
            {
              "names": ["local_index"],
              "privileges": ["read"]
            },
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["manage_failure_store", "read_cross_cluster", "read_failure_store"]
            }
          ]
        }""", ALL_ACCESS, """
        {
          "cluster": ["all"],
          "indices": [
            {
              "names": ["*"],
              "privileges": ["all"]
            }
          ]
        }""", ONLY_READ_FAILURE_STORE_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
            {
              "names": ["test*", "non-existing-index"],
              "privileges": ["read_failure_store"]
            }
          ]
        }""", BACKING_DATA_INDEX_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
            {
              "names": [".ds-test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""", BACKING_FAILURE_STORE_INDEX_ACCESS, """
        {
          "cluster": ["manage_security"],
          "indices": [
            {
              "names": [".fs-test*"],
              "privileges": ["read", "read_cross_cluster"]
            }
          ]
        }""");

    /**
     * Maps usernames to their API keys.
     */
    private static Map<String, Tuple<String, String>> apiKeys = new HashMap<>();

    @Before
    public void resetApiKeys() {
        apiKeys = new HashMap<>();
    }

    private static void setupRoleAndUserOnFulfillingCluster() throws IOException {
        for (var userAndRole : usersAndRolesOnFulfillingCluster.entrySet()) {
            String roleAndUsername = userAndRole.getKey();
            String roleDescriptor = userAndRole.getValue();
            createRoleAndUserOnFulfillingCluster(roleAndUsername, roleDescriptor);
        }
    }

    private static void setupUserAndRoleOnQueryCluster() throws IOException {
        for (var userAndRole : usersAndRolesOnQueryCluster.entrySet()) {
            String roleAndUsername = userAndRole.getKey();
            String roleDescriptor = userAndRole.getValue();
            createRoleOnQueryCluster(adminClient(), roleAndUsername, roleDescriptor);
            createUserOnQueryCluster(adminClient(), roleAndUsername, PASS, roleAndUsername);
            createAndStoreApiKeyOnQueryCluster(roleAndUsername, roleDescriptor);
        }
    }

    /**
     * Index some documents on the query cluster to use them in a mixed-cluster search.
     */
    private static void setupLocalDataOnQueryCluster() throws IOException {
        final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
        assertOK(client().performRequest(indexDocRequest));
    }

    public void testRCS1CrossClusterSearch() throws Exception {
        final boolean rcs1Security = true;
        final boolean isProxyMode = randomBoolean();
        final boolean skipUnavailable = randomBoolean();
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
        testSearchingUnauthorizedIndices(otherBackingFailureIndexName, otherBackingDataIndexName, ccsMinimizeRoundtrips, skipUnavailable);
        testSearchingWithAccessToAllIndices(ccsMinimizeRoundtrips, backingDataIndexName, otherBackingDataIndexName);
        testBackingFailureIndexAccess(ccsMinimizeRoundtrips, backingFailureIndexName, skipUnavailable);
        testBackingDataIndexAccess(ccsMinimizeRoundtrips, backingDataIndexName);
        testSearchingNonExistingIndices(ccsMinimizeRoundtrips, skipUnavailable);
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
            performRequestMaybeUsingApiKey(BACKING_DATA_INDEX_ACCESS, dataIndexSearchRequest),
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
        assertSearchResponseContainsIndices(performRequestMaybeUsingApiKey(ALL_ACCESS, dataSearchRequest), expectedIndices);
    }

    private void testSearchingNonExistingIndices(boolean ccsMinimizeRoundtrips, boolean skipUnavailable) {
        // searching non-existing index without permissions should result in 403
        {
            final String indexToSearch = "non-existing-no-privileges";
            final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
            executeAndAssert(
                () -> performRequestMaybeUsingApiKey(
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
                ),
                exception -> assertActionUnauthorized(exception, FAILURE_STORE_ACCESS, action, indexToSearch),
                response -> assertActionUnauthorized(response, FAILURE_STORE_ACCESS, action, indexToSearch),
                skipUnavailable
            );
        }
        // searching non-existing index with permissions should result in 404
        {
            final String indexToSearch = "non-existing-index";
            executeAndAssert(
                () -> performRequestMaybeUsingApiKey(
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
                ),
                exception -> assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404)),
                response -> assertThat(
                    ObjectPath.createFromResponse(response)
                        .evaluate("_clusters.details.my_remote_cluster.failures.0.reason.reason")
                        .toString(),
                    containsString("no such index [" + indexToSearch + "]")
                ),
                skipUnavailable
            );
        }
    }

    private void testSearchingUnauthorizedIndices(
        String otherBackingFailureIndexName,
        String otherBackingDataIndexName,
        boolean ccsMinimizeRoundtrips,
        boolean skipUnavailable
    ) {
        // try searching remote index for which user has no access
        final String indexToSearch = randomFrom("other1", otherBackingFailureIndexName, otherBackingDataIndexName);
        final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
        executeAndAssert(
            () -> performRequestMaybeUsingApiKey(
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
            ),
            exception -> assertActionUnauthorized(exception, FAILURE_STORE_ACCESS, action, indexToSearch),
            response -> assertActionUnauthorized(response, FAILURE_STORE_ACCESS, action, indexToSearch),
            skipUnavailable
        );
    }

    private void testBackingFailureIndexAccess(boolean ccsMinimizeRoundtrips, String backingFailureIndexName, boolean skipUnavailable)
        throws IOException {
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
        assertSearchResponseContainsIndices(performRequestMaybeUsingApiKey(ALL_ACCESS, failureIndexSearchRequest), backingFailureIndexName);

        // user with data only access should not be able to search the backing failure index
        {
            final String action = ccsMinimizeRoundtrips ? "indices:data/read/search" : "indices:admin/search/search_shards";
            executeAndAssert(
                () -> performRequestMaybeUsingApiKey(DATA_ACCESS, failureIndexSearchRequest),
                exception -> assertActionUnauthorized(exception, DATA_ACCESS, action, backingFailureIndexName),
                response -> assertActionUnauthorized(response, DATA_ACCESS, action, backingFailureIndexName),
                skipUnavailable
            );
        }

        // for user with access to failure store, it depends on the underlying action that is being sent to the remote cluster
        if (ccsMinimizeRoundtrips) {
            // this is a special case where indices:data/read/search will be sent to a remote cluster
            // and the request to backing failure store index will be authorized based on the datastream
            // which grants access to backing failure store indices (granted by read_failure_store privilege)
            // from a security perspective, this is a valid use case and there is no way to prevent this with RCS1 security model
            // since from the fulfilling cluster perspective this request is no different from any other local search request
            assertSearchResponseContainsIndices(
                performRequestMaybeUsingApiKey(FAILURE_STORE_ACCESS, failureIndexSearchRequest),
                backingFailureIndexName
            );
        } else {
            // in this case, the user does not have the necessary permissions to search the backing failure index
            // the request to failure store backing index is authorized based on the datastream
            // which does not grant access to the indices:admin/search/search_shards action
            // this action is granted by read_cross_cluster privilege which is currently
            // not supporting the failure backing indices (only data backing indices)
            executeAndAssert(
                () -> performRequestMaybeUsingApiKey(FAILURE_STORE_ACCESS, failureIndexSearchRequest),
                exception -> assertActionUnauthorized(
                    exception,
                    FAILURE_STORE_ACCESS,
                    "indices:admin/search/search_shards",
                    backingFailureIndexName
                ),
                response -> assertActionUnauthorized(
                    response,
                    FAILURE_STORE_ACCESS,
                    "indices:admin/search/search_shards",
                    backingFailureIndexName
                ),
                skipUnavailable
            );
        }

        // user with manage failure store access should be able to search the backing failure index
        assertSearchResponseContainsIndices(
            performRequestMaybeUsingApiKey(MANAGE_FAILURE_STORE_ACCESS, failureIndexSearchRequest),
            backingFailureIndexName
        );

        assertSearchResponseContainsIndices(
            performRequestMaybeUsingApiKey(BACKING_FAILURE_STORE_INDEX_ACCESS, failureIndexSearchRequest),
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
            assertSearchResponseContainsIndices(performRequestMaybeUsingApiKey(user, dataSearchRequest), expectedIndices);
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
                () -> performRequestMaybeUsingApiKey(user, dataSearchRequest)
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
                () -> performRequestMaybeUsingApiKey(
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
            () -> performRequestMaybeUsingApiKey(ONLY_READ_FAILURE_STORE_ACCESS, new Request("GET", "/_resolve/cluster"))
        );
        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            exc.getMessage(),
            anyOf(
                containsString(
                    "action ["
                        + "indices:admin/resolve/cluster"
                        + "] is unauthorized for user ["
                        + ONLY_READ_FAILURE_STORE_ACCESS
                        + "] "
                        + "with effective roles ["
                        + ONLY_READ_FAILURE_STORE_ACCESS
                        + "]"
                ),
                containsString(
                    "action [indices:admin/resolve/cluster] is unauthorized for API key id ["
                        + apiKeys.get(ONLY_READ_FAILURE_STORE_ACCESS).v1()
                        + "] of user ["
                        + ONLY_READ_FAILURE_STORE_ACCESS
                        + "]"
                )
            )
        );
    }

    private static void createRoleOnQueryCluster(RestClient client, String role, String roleDescriptor) throws IOException {
        final Request putRoleRequest = new Request("PUT", "/_security/role/" + role);
        putRoleRequest.setJsonEntity(roleDescriptor);
        assertOK(client.performRequest(putRoleRequest));
    }

    private static void createUserOnQueryCluster(RestClient client, String user, SecureString password, String role) throws IOException {
        final Request putUserRequest = new Request("PUT", "/_security/user/" + user);
        putUserRequest.setJsonEntity(Strings.format("""
            {
              "password": "%s",
              "roles" : ["%s"]
            }""", password.toString(), role));
        assertOK(client.performRequest(putUserRequest));
    }

    private static void createRoleAndUserOnFulfillingCluster(String userAndRoleName, String roleDescriptor) throws IOException {
        putRoleOnFulfillingCluster(userAndRoleName, roleDescriptor);
        putUserOnFulfillingCluster(userAndRoleName, userAndRoleName);
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
            anyOf(
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
                ),
                containsString(
                    "action ["
                        + action
                        + "] is unauthorized for API key id ["
                        + (apiKeys.containsKey(userAndRole)
                            ? apiKeys.get(userAndRole).v1()
                            : "<there is no test API key for this user - ignore this assertion>")
                        + "] of user ["
                        + userAndRole
                        + "] on indices ["
                        + backingFailureIndexName
                        + "]"
                )
            )
        );
    }

    private static void assertActionUnauthorized(Response response, String userAndRole, String action, String backingFailureIndexName)
        throws IOException {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(
            ObjectPath.createFromResponse(response).evaluate("_clusters.details.my_remote_cluster.failures.0.reason.reason").toString(),
            anyOf(
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
                ),
                containsString(
                    "action ["
                        + action
                        + "] is unauthorized for API key id ["
                        + (apiKeys.containsKey(userAndRole)
                            ? apiKeys.get(userAndRole).v1()
                            : "<there is no test API key for this user - ignore this assertion>")
                        + "] of user ["
                        + userAndRole
                        + "] on indices ["
                        + backingFailureIndexName
                        + "]"
                )
            )
        );
    }

    protected static void createAndStoreApiKeyOnQueryCluster(String user, @Nullable String roleDescriptors) throws IOException {
        assertThat("API key already registered for user: " + user, apiKeys.containsKey(user), is(false));
        apiKeys.put(user, createApiKeyOnQueryCluster(user, roleDescriptors));
    }

    private static Tuple<String, String> createApiKeyOnQueryCluster(String user, String roleDescriptors) throws IOException {
        var request = new Request("POST", "/_security/api_key");
        if (roleDescriptors == null) {
            request.setJsonEntity("""
                {
                    "name": "test-api-key"
                }
                """);
        } else {
            request.setJsonEntity(org.elasticsearch.common.Strings.format("""
                {
                    "name": "test-api-key",
                    "role_descriptors": {
                        "%s": %s
                    }
                }
                """, user, roleDescriptors));
        }
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASS)).build());
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        return new Tuple<>((String) responseAsMap.get("id"), (String) responseAsMap.get("encoded"));
    }

    protected static Response performRequestMaybeUsingApiKey(String user, Request request) throws IOException {
        if (randomBoolean() && apiKeys.containsKey(user)) {
            return performRequestWithApiKey(apiKeys.get(user).v2(), request);
        } else {
            return performRequestWithUser(user, request);
        }
    }

    private static Response performRequestWithApiKey(String apiKey, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + apiKey).build());
        return client().performRequest(request);
    }

    private static <T extends Throwable> void executeAndAssert(
        ThrowableCommand<Response> requestCommand,
        Consumer<ResponseException> exceptionAssertion,
        ThrowableConsumer<Response> responseAssertion,
        boolean expectResponse
    ) {
        if (expectResponse) {
            try {
                responseAssertion.accept(requestCommand.execute());
            } catch (Exception e) {
                fail(e, "Not expected exception to be thrown: " + e.getMessage());
            }
        } else {
            exceptionAssertion.accept(expectThrows(ResponseException.class, requestCommand::execute));
        }
    }

    @FunctionalInterface
    private interface ThrowableCommand<T> {
        T execute() throws Exception;
    }

    @FunctionalInterface
    public interface ThrowableConsumer<T> {

        void accept(T t) throws Exception;
    }
}

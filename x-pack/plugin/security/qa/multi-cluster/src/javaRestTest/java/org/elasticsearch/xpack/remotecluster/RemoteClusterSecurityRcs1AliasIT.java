/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class RemoteClusterSecurityRcs1AliasIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").nodes(3).apply(commonClusterConfig).build();
        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @After
    public void wipeData() throws IOException {
        CheckedConsumer<RestClient, IOException> wipe = client -> {
            performRequestWithAdminUser(client, new Request("DELETE", "/index-000001/_alias/index-alias"));
            performRequestWithAdminUser(client, new Request("DELETE", "/index-000001"));
        };
        wipe.accept(fulfillingClusterClient);
    }

    @Before
    public void setup() throws Exception {
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, false, false);
        putSearchUserOnQueryingCluster();
    }

    public void testAliasUnderRcs1WithAliasAndIndexPrivileges() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                },
                {
                  "names": ["index-000001"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index-000001" } }
            { "foo": "1" }
            { "index": { "_index": "index-000001" } }
            { "foo": "2" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index-000001",
                    "alias": "index-alias"
                  }
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(aliasRequest));

        searchAndAssertFooValues("index-*", true, "1", "2");
        searchAndAssertFooValues("index-*", false, "1", "2");

        searchAndAssertFooValues("index-alias", true, "1", "2");
        searchAndAssertFooValues("index-alias", false, "1", "2");

        searchAndAssertFooValues("index-000001", true, "1", "2");
        searchAndAssertFooValues("index-000001", false, "1", "2");
    }

    public void testAliasUnderRcs1WithAliasOnlyPrivileges() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index-000001" } }
            { "foo": "1" }
            { "index": { "_index": "index-000001" } }
            { "foo": "2" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index-000001",
                    "alias": "index-alias"
                  }
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(aliasRequest));

        searchAndAssertFooValues("index-alias", true, "1", "2");
        // we throw a 403, inconsistent with minimize roundtrips
        expectThrows(Exception.class, () -> search("index-alias", false));
    }

    public void testAliasUnderRcs1WithAliasOnlyPrivilegesWithDummyFilter() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index-000001" } }
            { "foo": "1" }
            { "index": { "_index": "index-000001" } }
            { "foo": "2" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index-000001",
                    "alias": "index-alias",
                    "filter": { "match_all": {} }
                  }
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(aliasRequest));

        searchAndAssertFooValues("index-alias", true, "1", "2");
        // we don't throw, even though the filter is a dummy one so effectively should act the same as no filter
        searchAndAssertFooValues("index-alias", false, "1", "2");
    }

    public void testAliasUnderRcs1WithAliasOnlyPrivilegesWithRealFilter() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read", "read_cross_cluster", "view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index-000001" } }
            { "foo": "1" }
            { "index": { "_index": "index-000001" } }
            { "foo": "2" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index-000001",
                    "alias": "index-alias",
                    "filter": {
                      "term": {
                        "foo": "1"
                      }
                    }
                  }
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(aliasRequest));

        searchAndAssertFooValues("index-alias", true, "1");
        searchAndAssertFooValues("index-alias", false, "1");

        searchAndAssertFooValues("index-*", true, "1");
        searchAndAssertFooValues("index-*", false, "1");
    }

    public void testAliasUnderRcs1WithMixedPrivilegesWithRealFilter() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read_cross_cluster", "view_index_metadata"]
                },
                {
                  "names": ["index-000001"],
                  "privileges": ["read"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "index-000001" } }
            { "foo": "1" }
            { "index": { "_index": "index-000001" } }
            { "foo": "2" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        Request aliasRequest = new Request("POST", "/_aliases");
        aliasRequest.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "index-000001",
                    "alias": "index-alias",
                    "filter": {
                      "term": {
                        "foo": "1"
                      }
                    }
                  }
                }
              ]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(aliasRequest));

        searchAndAssertFooValues("index-*", true, "1", "2");
        // inconsistent with minimize roundtrips because we throw a 403
        expectThrows(Exception.class, () -> search("index-*", false));

        // 403 on both because we don't have read privilege on index-alias
        expectThrows(Exception.class, () -> search("index-alias", true));
        expectThrows(Exception.class, () -> search("index-alias", false));
    }

    private void searchAndAssertFooValues(String indexPattern, boolean ccsMinimizeRoundtrips, String... expectedFooValues)
        throws IOException {
        final SearchResponse searchResponse = search(indexPattern, ccsMinimizeRoundtrips);
        try {
            final SearchHit[] actual = searchResponse.getHits().getHits();
            final var actualFooValues = Arrays.stream(actual)
                .map(SearchHit::getSourceAsMap)
                .filter(Objects::nonNull)
                .map(m -> m.get("foo"))
                .toList();
            assertThat(
                "actual: [" + actualFooValues + "] expected: [" + Arrays.toString(expectedFooValues) + "]",
                actualFooValues,
                containsInAnyOrder((Object[]) expectedFooValues)
            );
        } finally {
            searchResponse.decRef();
        }
    }

    private void putSearchUserOnQueryingCluster() throws IOException {
        var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
        putUserRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles" : ["remote_search"]
            }""");
        assertOK(adminClient().performRequest(putUserRequest));
    }

    private SearchResponse search(String indexPattern, boolean ccsMinimizeRoundtrips) throws IOException {
        final var searchRequest = new Request(
            "GET",
            String.format(Locale.ROOT, "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s", indexPattern, ccsMinimizeRoundtrips)
        );
        final Response response = performRequestWithRemoteSearchUser(searchRequest);
        assertOK(response);
        return SearchResponseUtils.parseSearchResponse(responseAsParser(response));
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }
}

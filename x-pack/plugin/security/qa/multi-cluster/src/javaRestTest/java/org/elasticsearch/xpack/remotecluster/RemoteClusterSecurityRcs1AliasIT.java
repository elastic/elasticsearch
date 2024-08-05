/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.Build;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRcs1AliasIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("ingest-common")
            .nodes(3)
            .apply(commonClusterConfig)
            .build();
        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .build();
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
        assertOK(adminClient().performRequest(putRoleRequest));

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
        esqlSearchAndAssertResults("index-*", List.of("1", "1", "2", "2"));

        searchAndAssertFooValues("index-alias", true, "1", "2");
        searchAndAssertFooValues("index-alias", false, "1", "2");
        esqlSearchAndAssertResults("index-alias", List.of("1", "1", "2", "2"));

        searchAndAssertFooValues("index-000001", true, "1", "2");
        searchAndAssertFooValues("index-000001", false, "1", "2");
        esqlSearchAndAssertResults("index-000001", List.of("1", "1", "2", "2"));
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
        assertOK(adminClient().performRequest(putRoleRequest));

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
        esqlSearchAndAssertResults("index-alias", List.of("1", "1", "2", "2"));
        expectThrows403(() -> search("index-alias", false));
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
        assertOK(adminClient().performRequest(putRoleRequest));

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
        searchAndAssertFooValues("index-alias", false, "1", "2");
        esqlSearchAndAssertResults("index-alias", List.of("1", "1", "2", "2"));
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
        assertOK(adminClient().performRequest(putRoleRequest));

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
        esqlSearchAndAssertResults("index-alias", List.of("1", "1"));

        searchAndAssertFooValues("index-*", true, "1");
        searchAndAssertFooValues("index-*", false, "1");
        esqlSearchAndAssertResults("index-*", List.of("1", "1"));
    }

    public void testAliasUnderRcs1WithMixedPrivilegesWithRealFilterV1() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read"]
                },
                {
                  "names": ["index-000001"],
                  "privileges": ["view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);
        assertOK(adminClient().performRequest(putRoleRequest));

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

        // searchAndAssertFooValues("index-*", true, "1", "2");
        // searchAndAssertFooValues("index-*", false, "1", "2");
        esqlSearchAndAssertResults("index-*", List.of("1", "1", "2", "2"));

        // expectThrows403(() -> search("index-alias", true));
        // expectThrows403(() -> search("index-alias", false));
        // TODO this does not look right, maybe
        esqlSearchAndAssertResults("index-alias", List.of("1", "1"));
    }

    public void testAliasUnderRcs1WithMixedPrivilegesWithRealFilterV2() throws Exception {
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["index-alias"],
                  "privileges": ["read"]
                },
                {
                  "names": ["index-000001"],
                  "privileges": ["read_cross_cluster", "view_index_metadata"]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);
        assertOK(adminClient().performRequest(putRoleRequest));

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

        searchAndAssertFooValues("index-*", true, "1");
        expectThrows403(() -> search("index-*", false));
        // TODO this does not look right
        esqlSearchAndAssertResults("index-*", List.of("1", "1", "2", "2"));

        searchAndAssertFooValues("index-alias", true, "1");
        searchAndAssertFooValues("index-alias", false, "1");
        esqlSearchAndAssertResults("index-alias", List.of("1", "1"));
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

    private static void expectThrows403(ThrowingRunnable runnable) {
        assertThat(expectThrows(ResponseException.class, runnable).getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    private void esqlSearchAndAssertResults(String index, List<String> expectedResults) throws IOException {
        var response = performRequestWithRemoteSearchUser(esqlRequest(Strings.format("""
            FROM my_remote_cluster:%s
            | SORT foo ASC
            | LIMIT 10""", index)));
        assertResults(response, expectedResults);
    }

    @SuppressWarnings("unchecked")
    private void assertResults(Response response, List<String> expectedValues) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder(expectedValues.toArray()));
    }

    protected Request esqlRequest(String command) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();
        body.startObject();
        body.field("query", command);
        if (Build.current().isSnapshot() && randomBoolean()) {
            Settings.Builder settings = Settings.builder();
            if (randomBoolean()) {
                settings.put("page_size", between(1, 5));
            }
            if (randomBoolean()) {
                settings.put("exchange_buffer_size", between(1, 2));
            }
            if (randomBoolean()) {
                settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
            }
            if (randomBoolean()) {
                settings.put("enrich_max_workers", between(1, 5));
            }
            Settings pragmas = settings.build();
            if (pragmas != Settings.EMPTY) {
                body.startObject("pragma");
                body.value(pragmas);
                body.endObject();
            }
        }
        body.endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));
        return request;
    }
}

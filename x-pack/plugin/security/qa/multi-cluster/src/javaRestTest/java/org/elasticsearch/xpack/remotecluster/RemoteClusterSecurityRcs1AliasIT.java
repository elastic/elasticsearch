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
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRcs1AliasIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local().name("fulfilling-cluster").nodes(3).apply(commonClusterConfig).build();
        queryCluster = ElasticsearchCluster.local().name("query-cluster").apply(commonClusterConfig).build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testAliasUnderRcs1() throws Exception {
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());

        {
            putSearchUserOnQueryingCluster();

            // fulfilling cluster
            var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["index-alias"],
                      "privileges": ["read", "read_cross_cluster"]
                    },
                    {
                      "names": ["index-000001"],
                      "privileges": ["read", "read_cross_cluster"]
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

            // "filter": { "term": { "foo": "1" } }
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
        }

        {
            final SearchResponse searchResponse = search("index-*", true);
            try {
                final SearchHit[] actual = searchResponse.getHits().getHits();
                assertThat(actual.length, equalTo(2));
                assertThat(Objects.requireNonNull(actual[0].getSourceAsMap()).get("foo").toString(), equalTo("1"));
                assertThat(Objects.requireNonNull(actual[1].getSourceAsMap()).get("foo").toString(), equalTo("2"));
            } finally {
                searchResponse.decRef();
            }
        }

        {
            final SearchResponse searchResponse = search("index-*", false);
            try {
                final SearchHit[] actual = searchResponse.getHits().getHits();
                assertThat(actual.length, equalTo(2));
                assertThat(Objects.requireNonNull(actual[0].getSourceAsMap()).get("foo").toString(), equalTo("1"));
                assertThat(Objects.requireNonNull(actual[1].getSourceAsMap()).get("foo").toString(), equalTo("2"));
            } finally {
                searchResponse.decRef();
            }
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
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        return searchResponse;
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

}

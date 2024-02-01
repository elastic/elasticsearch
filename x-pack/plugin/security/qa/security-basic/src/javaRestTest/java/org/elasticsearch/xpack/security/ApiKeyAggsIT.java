/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.QueryApiKeyIT.createApiKey;
import static org.hamcrest.Matchers.is;

public class ApiKeyAggsIT extends SecurityInBasicRestTestCase {

    @SuppressWarnings("unchecked")
    public void testFiltersAggs() throws IOException {
        // admin keys
        createApiKey(
            "key1",
            Map.of("tags", List.of("prod", "est"), "label", "value1", "environment", Map.of("system", false, "hostname", "my-org-host-1")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        createApiKey(
            "key2",
            Map.of("tags", List.of("prod", "west"), "label", "value2", "environment", Map.of("system", false, "hostname", "my-org-host-2")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        createApiKey(
            "key3",
            Map.of("tags", List.of("prod", "south"), "label", "value3", "environment", Map.of("system", true, "hostname", "my-org-host-2")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        // user keys
        createApiKey(
            "key4",
            Map.of("tags", List.of("prod", "north"), "label", "value4", "environment", Map.of("system", true, "hostname", "my-org-host-1")),
            API_KEY_USER_AUTH_HEADER
        );
        createApiKey(
            "wild",
            Map.of(
                "tags",
                List.of("staging", "west"),
                "label",
                "value5",
                "environment",
                Map.of("system", true, "hostname", "my-org-host-3")
            ),
            API_KEY_USER_AUTH_HEADER
        );
        assertAggs(API_KEY_ADMIN_AUTH_HEADER, """
            {
              "aggs": {
                "hostnames": {
                  "filters": {
                    "filters": {
                      "my-org-host-1": { "term": {"metadata.environment.hostname": "my-org-host-1"}},
                      "my-org-host-2": { "match": {"metadata": "my-org-host-2"}}
                    }
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).size(), is(2));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).get(
                    "my-org-host-1"
                )).get("doc_count"),
                is(2)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).get(
                    "my-org-host-2"
                )).get("doc_count"),
                is(2)
            );
        });
        assertAggs(API_KEY_USER_AUTH_HEADER, """
            {
              "aggs": {
                "only_user_keys": {
                  "filters": {
                    "other_bucket_key": "other_user_keys",
                    "filters": {
                      "only_key4_match": { "bool": { "should": [{"prefix": {"name": "key"}}, {"match": {"metadata.tags": "prod"}}]}}
                    }
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).size(), is(2));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).get(
                    "only_key4_match"
                )).get("doc_count"),
                is(1)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).get(
                    "other_user_keys"
                )).get("doc_count"),
                is(1)
            );
        });
        assertAggs(API_KEY_USER_AUTH_HEADER, """
            {
              "aggs": {
                "all_user_keys": {
                  "filters": {
                    "other_bucket_key": "other_user_keys",
                    "filters": [
                      {"match_all": {}},
                      {"exists": {"field": "username"}},
                      {"wildcard": {"name": {"value": "*"}}}
                    ]
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).size(), is(4));
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(0).get("doc_count"),
                is(2)
            );
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(1).get("doc_count"),
                is(2)
            );
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(2).get("doc_count"),
                is(2)
            );
            // the "other" bucket
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(3).get("doc_count"),
                is(0)
            );
        });
    }

    void assertAggs(String authHeader, String body, Consumer<Map<String, Object>> aggsVerifier) throws IOException {
        final Request request = new Request("GET", "/_security/_query/api_key");
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final Map<String, Object> aggs = (Map<String, Object>) responseMap.get("aggregations");
        aggsVerifier.accept(aggs);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class QueryApiKeyIT extends SecurityInBasicRestTestCase {

    public void testQuery() throws IOException {
        createApiKeys();
        createUser("someone");

        // Admin with manage_api_key can search for all keys
        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            { "query": { "wildcard": {"name": "*alert*"} } }""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(2));
            assertThat(apiKeys.get(0).get("name"), oneOf("my-org/alert-key-1", "my-alert-key-2"));
            assertThat(apiKeys.get(1).get("name"), oneOf("my-org/alert-key-1", "my-alert-key-2"));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            { "query": { "match": {"name": {"query": "my-ingest-key-1 my-org/alert-key-1", "analyzer": "whitespace"} } } }""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(2));
            assertThat(apiKeys.get(0).get("name"), oneOf("my-ingest-key-1", "my-org/alert-key-1"));
            assertThat(apiKeys.get(1).get("name"), oneOf("my-ingest-key-1", "my-org/alert-key-1"));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        // An empty request body means search for all keys
        assertQuery(API_KEY_ADMIN_AUTH_HEADER, randomBoolean() ? "" : """
            {"query":{"match_all":{}}}""", apiKeys -> assertThat(apiKeys.size(), equalTo(6)));

        assertQuery(API_KEY_ADMIN_AUTH_HEADER, randomBoolean() ? "" : """
            { "query": { "match": {"type": "rest"} } }""", apiKeys -> assertThat(apiKeys.size(), equalTo(6)));

        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query":{"bool":{"must":[{"prefix":{"metadata.application":"fleet"}},{"term":{"metadata.environment.os":"Cat"}}]}}}""",
            apiKeys -> {
                assertThat(apiKeys, hasSize(2));
                assertThat(
                    apiKeys.stream().map(k -> k.get("name")).collect(Collectors.toList()),
                    containsInAnyOrder("my-org/ingest-key-1", "my-org/management-key-1")
                );
                apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
            }
        );

        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            {"query":{"terms":{"metadata.tags":["prod","east"]}}}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(5));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            {"query":{"range":{"creation":{"lt":"now"}}}}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(6));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        // Search for keys belong to an user
        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            { "query": { "term": {"username": "api_key_user"} } }""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(2));
            assertThat(
                apiKeys.stream().map(m -> m.get("name")).collect(Collectors.toSet()),
                equalTo(Set.of("my-ingest-key-1", "my-alert-key-2"))
            );
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        // Search for keys belong to users from a realm
        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            { "query": { "term": {"realm_name": "default_file"} } }""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(6));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
            // search using explicit IDs
            try {

                var subset = randomSubsetOf(randomIntBetween(1, 5), apiKeys);
                assertQuery(
                    API_KEY_ADMIN_AUTH_HEADER,
                    Strings.format(
                        """
                            { "query": { "ids": { "values": [%s] } } }""",
                        subset.stream().map(m -> "\"" + m.get("id") + "\"").collect(Collectors.joining(","))
                    ),
                    keys -> {
                        assertThat(keys, hasSize(subset.size()));
                        keys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
                    }
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // Search for fields outside of the allowlist fails
        ResponseException responseException = assertQueryError(API_KEY_ADMIN_AUTH_HEADER, 400, """
            { "query": { "prefix": {"api_key_hash": "{PBKDF2}10000$"} } }""");
        assertThat(responseException.getMessage(), containsString("Field [api_key_hash] is not allowed for querying"));

        // Search for fields that are not allowed in Query DSL but used internally by the service itself
        final String fieldName = randomFrom("doc_type", "api_key_invalidated", "invalidation_time");
        assertQueryError(API_KEY_ADMIN_AUTH_HEADER, 400, Strings.format("""
            { "query": { "term": {"%s": "%s"} } }""", fieldName, randomAlphaOfLengthBetween(3, 8)));

        // Search for api keys won't return other entities
        assertQuery(API_KEY_ADMIN_AUTH_HEADER, """
            { "query": { "term": {"name": "someone"} } }""", apiKeys -> { assertThat(apiKeys, empty()); });

        // User with manage_own_api_key will only see its own keys
        assertQuery(API_KEY_USER_AUTH_HEADER, randomBoolean() ? "" : "{\"query\":{\"match_all\":{}}}", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(2));
            assertThat(
                apiKeys.stream().map(m -> m.get("name")).collect(Collectors.toSet()),
                containsInAnyOrder("my-ingest-key-1", "my-alert-key-2")
            );
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        assertQuery(API_KEY_USER_AUTH_HEADER, """
            { "query": { "wildcard": {"name": "*alert*"} } }""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(1));
            assertThat(apiKeys.get(0).get("name"), equalTo("my-alert-key-2"));
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        // User without manage_api_key or manage_own_api_key gets 403 trying to search API keys
        assertQueryError(TEST_USER_AUTH_HEADER, 403, """
            { "query": { "wildcard": {"name": "*alert*"} } }""");

        // Invalidated API keys are returned by default, but can be filtered out
        final String authHeader = randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER);
        final String invalidatedApiKeyId1 = createAndInvalidateApiKey("temporary-key-1", authHeader);
        final String queryString = randomFrom("""
            {"query": { "term": {"name": "temporary-key-1"} } }""", Strings.format("""
            {"query":{"bool":{"must":[{"term":{"name":{"value":"temporary-key-1"}}},\
            {"range": {"invalidation": {"lte": "now"}}},
            {"term":{"invalidated":{"value":"%s"}}}]}}}
            """, randomBoolean()));

        assertQuery(authHeader, queryString, apiKeys -> {
            if (queryString.contains("""
                "invalidated":{"value":"false\"""")) {
                assertThat(apiKeys, empty());
            } else {
                assertThat(apiKeys.size(), equalTo(1));
                assertThat(apiKeys.get(0).get("name"), equalTo("temporary-key-1"));
                assertThat(apiKeys.get(0).get("id"), equalTo(invalidatedApiKeyId1));
                assertThat(apiKeys.get(0).get("invalidated"), is(true));
                assertThat(apiKeys.get(0).get("invalidation"), notNullValue());
            }
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });
    }

    public void testQueryShouldRespectOwnerIdentityWithApiKeyAuth() throws IOException {
        final Tuple<String, String> powerKey = createApiKey("power-key-1", null, null, API_KEY_ADMIN_AUTH_HEADER);
        final String powerKeyAuthHeader = "ApiKey "
            + Base64.getEncoder().encodeToString((powerKey.v1() + ":" + powerKey.v2()).getBytes(StandardCharsets.UTF_8));

        final Tuple<String, String> limitKey = createApiKey(
            "limit-key-1",
            Map.of("a", Map.of("cluster", List.of("manage_own_api_key"))),
            null,
            API_KEY_ADMIN_AUTH_HEADER
        );
        final String limitKeyAuthHeader = "ApiKey "
            + Base64.getEncoder().encodeToString((limitKey.v1() + ":" + limitKey.v2()).getBytes(StandardCharsets.UTF_8));

        createApiKey("power-key-1-derived-1", Map.of("a", Map.of()), null, powerKeyAuthHeader);
        createApiKey("limit-key-1-derived-1", Map.of("a", Map.of()), null, limitKeyAuthHeader);

        createApiKey("user-key-1", Map.of(), API_KEY_USER_AUTH_HEADER);
        createApiKey("user-key-2", Map.of(), API_KEY_USER_AUTH_HEADER);

        // powerKey gets back all keys since it has manage_api_key privilege
        assertQuery(powerKeyAuthHeader, "", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(6));
            assertThat(
                apiKeys.stream().map(m -> (String) m.get("name")).collect(Collectors.toUnmodifiableSet()),
                equalTo(Set.of("power-key-1", "limit-key-1", "power-key-1-derived-1", "limit-key-1-derived-1", "user-key-1", "user-key-2"))
            );
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

        // limitKey gets only itself. It cannot view other keys owned by the owner user. This is consistent with how
        // get api key works
        assertQuery(limitKeyAuthHeader, "", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(1));
            assertThat(
                apiKeys.stream().map(m -> (String) m.get("name")).collect(Collectors.toUnmodifiableSet()),
                equalTo(Set.of("limit-key-1"))
            );
            apiKeys.forEach(k -> assertThat(k, not(hasKey("_sort"))));
        });

    }

    public void testPagination() throws IOException, InterruptedException {
        final String authHeader = randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER);
        final int total = randomIntBetween(8, 12);
        final List<String> apiKeyNames = IntStream.range(0, total).mapToObj(i -> Strings.format("k-%02d", i)).toList();
        final List<String> apiKeyIds = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            apiKeyIds.add(createApiKey(apiKeyNames.get(i), null, authHeader).v1());
            Thread.sleep(10); // make sure keys are created with sufficient time interval to guarantee sorting order
        }

        final int from = randomIntBetween(0, 3);
        final int size = randomIntBetween(2, 5);
        final int remaining = total - from;
        final String sortField = randomFrom("name", "creation");

        final List<Map<String, Object>> apiKeyInfos = new ArrayList<>(remaining);
        final Request request1 = new Request("GET", "/_security/_query/api_key");
        request1.setOptions(request1.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        request1.setJsonEntity("{\"from\":" + from + ",\"size\":" + size + ",\"sort\":[\"" + sortField + "\"]}");
        int actualSize = collectApiKeys(apiKeyInfos, request1, total, size);
        assertThat(actualSize, equalTo(size));  // first batch should be a full page

        while (apiKeyInfos.size() < remaining) {
            final Request request2 = new Request("GET", "/_security/_query/api_key");
            request2.setOptions(request2.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
            final StringBuilder searchAfter = new StringBuilder();
            final List<Object> sortValues = extractSortValues(apiKeyInfos.get(apiKeyInfos.size() - 1));
            if ("name".equals(sortField)) {
                searchAfter.append("\"").append(sortValues.get(0)).append("\"");
            } else {
                searchAfter.append(sortValues.get(0));
            }
            request2.setJsonEntity(Strings.format("""
                {"size":%s,"sort":["%s"],"search_after":[%s]}
                """, size, sortField, searchAfter));
            actualSize = collectApiKeys(apiKeyInfos, request2, total, size);
            if (actualSize == 0 && apiKeyInfos.size() < remaining) {
                fail("fail to retrieve all API keys, expect [" + remaining + "] keys, got [" + apiKeyInfos + "]");
            }
            // Before all keys are retrieved, each page should be a full page
            if (apiKeyInfos.size() < remaining) {
                assertThat(actualSize, equalTo(size));
            }
        }

        // assert sort values match the field of API key information
        if ("name".equals(sortField)) {
            assertThat(
                apiKeyInfos.stream().map(m -> (String) m.get("name")).toList(),
                equalTo(apiKeyInfos.stream().map(m -> (String) extractSortValues(m).get(0)).toList())
            );
        } else {
            assertThat(
                apiKeyInfos.stream().map(m -> (long) m.get("creation")).toList(),
                equalTo(apiKeyInfos.stream().map(m -> (long) extractSortValues(m).get(0)).toList())
            );
        }
        assertThat(apiKeyInfos.stream().map(m -> (String) m.get("id")).toList(), equalTo(apiKeyIds.subList(from, total)));
        assertThat(apiKeyInfos.stream().map(m -> (String) m.get("name")).toList(), equalTo(apiKeyNames.subList(from, total)));

        // size can be zero, but total should still reflect the number of keys matched
        final Request request2 = new Request("GET", "/_security/_query/api_key");
        request2.setOptions(request2.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        request2.setJsonEntity("{\"size\":0}");
        final Response response2 = client().performRequest(request2);
        assertOK(response2);
        final Map<String, Object> responseMap2 = responseAsMap(response2);
        assertThat(responseMap2.get("total"), equalTo(total));
        assertThat(responseMap2.get("count"), equalTo(0));
    }

    public void testTypeField() throws Exception {
        final List<String> allApiKeyIds = new ArrayList<>(7);
        for (int i = 0; i < 7; i++) {
            allApiKeyIds.add(
                createApiKey("typed_key_" + i, Map.of(), randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER)).v1()
            );
        }
        List<String> apiKeyIdsSubset = randomSubsetOf(allApiKeyIds);
        List<String> apiKeyIdsSubsetDifference = new ArrayList<>(allApiKeyIds);
        apiKeyIdsSubsetDifference.removeAll(apiKeyIdsSubset);

        List<String> apiKeyRestTypeQueries = List.of("""
            {"query": {"term": {"type": "rest" }}}""", """
            {"query": {"bool": {"must_not": [{"term": {"type": "cross_cluster"}}, {"term": {"type": "other"}}]}}}""", """
            {"query": {"prefix": {"type": "re" }}}""", """
            {"query": {"wildcard": {"type": "r*t" }}}""", """
            {"query": {"range": {"type": {"gte": "raaa", "lte": "rzzz"}}}}""");

        for (String query : apiKeyRestTypeQueries) {
            assertQuery(API_KEY_ADMIN_AUTH_HEADER, query, apiKeys -> {
                assertThat(
                    apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                    containsInAnyOrder(allApiKeyIds.toArray(new String[0]))
                );
            });
        }

        createSystemWriteRole("system_write");
        String systemWriteCreds = createUser("superuser_with_system_write", new String[] { "superuser", "system_write" });

        // test keys with no "type" field are still considered of type "rest"
        // this is so in order to accommodate pre-8.9 API keys which where all of type "rest" implicitly
        updateApiKeys(systemWriteCreds, "ctx._source.remove('type');", apiKeyIdsSubset);
        for (String query : apiKeyRestTypeQueries) {
            assertQuery(API_KEY_ADMIN_AUTH_HEADER, query, apiKeys -> {
                assertThat(
                    apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                    containsInAnyOrder(allApiKeyIds.toArray(new String[0]))
                );
            });
        }

        // but the same keys with type "other" are NOT of type "rest"
        updateApiKeys(systemWriteCreds, "ctx._source['type']='other';", apiKeyIdsSubset);
        for (String query : apiKeyRestTypeQueries) {
            assertQuery(API_KEY_ADMIN_AUTH_HEADER, query, apiKeys -> {
                assertThat(
                    apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                    containsInAnyOrder(apiKeyIdsSubsetDifference.toArray(new String[0]))
                );
            });
        }
        // the complement set is not of type "rest" if it is "cross_cluster"
        updateApiKeys(systemWriteCreds, "ctx._source['type']='rest';", apiKeyIdsSubset);
        updateApiKeys(systemWriteCreds, "ctx._source['type']='cross_cluster';", apiKeyIdsSubsetDifference);
        for (String query : apiKeyRestTypeQueries) {
            assertQuery(API_KEY_ADMIN_AUTH_HEADER, query, apiKeys -> {
                assertThat(
                    apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                    containsInAnyOrder(apiKeyIdsSubset.toArray(new String[0]))
                );
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testSort() throws IOException {
        final String authHeader = randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER);
        final List<String> apiKeyIds = new ArrayList<>(3);
        apiKeyIds.add(createApiKey("k2", Map.of("letter", "a", "symbol", "2"), authHeader).v1());
        apiKeyIds.add(createApiKey("k1", Map.of("letter", "b", "symbol", "2"), authHeader).v1());
        apiKeyIds.add(createApiKey("k0", Map.of("letter", "c", "symbol", "1"), authHeader).v1());

        assertQuery(authHeader, """
            {"sort":[{"creation":{"order":"desc"}}]}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(3));
            for (int i = 2, j = 0; i >= 0; i--, j++) {
                assertThat(apiKeys.get(i).get("id"), equalTo(apiKeyIds.get(j)));
                assertThat(apiKeys.get(i).get("creation"), equalTo(((List<Integer>) apiKeys.get(i).get("_sort")).get(0)));
            }
        });

        assertQuery(authHeader, """
            {"sort":[{"name":{"order":"asc"}}]}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(3));
            for (int i = 2, j = 0; i >= 0; i--, j++) {
                assertThat(apiKeys.get(i).get("id"), equalTo(apiKeyIds.get(j)));
                assertThat(apiKeys.get(i).get("name"), equalTo(((List<String>) apiKeys.get(i).get("_sort")).get(0)));
            }
        });

        assertQuery(authHeader, """
            {"sort":["metadata.letter"]}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(3));
            for (int i = 0; i < 3; i++) {
                assertThat(apiKeys.get(i).get("id"), equalTo(apiKeyIds.get(i)));
            }
        });

        assertQuery(authHeader, """
            {"sort":["metadata.symbol","metadata.letter"]}""", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(3));
            assertThat(apiKeys.get(0).get("id"), equalTo(apiKeyIds.get(2)));
            assertThat(apiKeys.get(1).get("id"), equalTo(apiKeyIds.get(0)));
            assertThat(apiKeys.get(2).get("id"), equalTo(apiKeyIds.get(1)));
            apiKeys.forEach(k -> {
                final Map<String, Object> metadata = (Map<String, Object>) k.get("metadata");
                assertThat(metadata.get("symbol"), equalTo(((List<Integer>) k.get("_sort")).get(0)));
                assertThat(metadata.get("letter"), equalTo(((List<Integer>) k.get("_sort")).get(1)));
            });
        });

        assertQuery(authHeader, "{\"sort\":[\"_doc\"]}", apiKeys -> {
            assertThat(apiKeys.size(), equalTo(3));
            final List<String> ids = new ArrayList<>(3);
            for (int i = 0; i < 3; i++) {
                ids.add((String) apiKeys.get(i).get("id"));
                assertThat(apiKeys.get(i).get("_sort"), notNullValue());
            }
            // There is no guarantee that _doc order is the same as creation order
            assertThat(ids, containsInAnyOrder(apiKeyIds.toArray()));
        });

        final String invalidFieldName = randomFrom("doc_type", "api_key_invalidated", "metadata_flattened.letter");
        assertQueryError(authHeader, 400, "{\"sort\":[\"" + invalidFieldName + "\"]}");
    }

    public void testSimpleQueryStringQuery() throws IOException {
        String batmanUserCredentials = createUser("batman", new String[] { "api_key_user_role" });
        final List<String> apiKeyIds = new ArrayList<>();
        apiKeyIds.add(createApiKey("key1-user", null, null, Map.of("label", "prod"), API_KEY_USER_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key1-admin", null, null, Map.of("label", "prod"), API_KEY_ADMIN_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key2-user", null, null, Map.of("value", 42, "label", "prod"), API_KEY_USER_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key2-admin", null, null, Map.of("value", 42, "label", "prod"), API_KEY_ADMIN_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key3-user", null, null, Map.of("value", 42, "hero", true), API_KEY_USER_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key3-admin", null, null, Map.of("value", 42, "hero", true), API_KEY_ADMIN_AUTH_HEADER).v1());
        apiKeyIds.add(createApiKey("key4-batman", null, null, Map.of("hero", true), batmanUserCredentials).v1());
        apiKeyIds.add(createApiKey("key5-batman", null, null, Map.of("hero", true), batmanUserCredentials).v1());

        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "key*", "fields": ["no_such_field_pattern*"]}}}""",
            apiKeys -> assertThat(apiKeys, is(empty()))
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "prod 42 true", "fields": ["metadata.*"]}}}""",
            apiKeys -> assertThat(apiKeys, is(empty()))
        );
        // disallowed fields are silently ignored for the simple query string query type
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "ke*", "fields": ["x*", "api_key_hash"]}}}""",
            apiKeys -> assertThat(apiKeys, is(empty()))
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "prod 42 true", "fields": ["wild*", "metadata"]}}}""",
            apiKeys -> assertThat(apiKeys.stream().map(k -> (String) k.get("id")).toList(), containsInAnyOrder(apiKeyIds.toArray()))
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "key* +rest" }}}""",
            apiKeys -> assertThat(apiKeys.stream().map(k -> (String) k.get("id")).toList(), containsInAnyOrder(apiKeyIds.toArray()))
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "-prod", "fields": ["metadata"]}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(apiKeyIds.get(4), apiKeyIds.get(5), apiKeyIds.get(6), apiKeyIds.get(7))
            )
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "-42", "fields": ["meta*", "whatever*"]}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(apiKeyIds.get(0), apiKeyIds.get(1), apiKeyIds.get(6), apiKeyIds.get(7))
            )
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "-rest term_which_does_not_exist"}}}""",
            apiKeys -> assertThat(apiKeys, is(empty()))
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "+default_file +api_key_user", "fields": ["us*", "rea*"]}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(apiKeyIds.get(0), apiKeyIds.get(2), apiKeyIds.get(4))
            )
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "default_fie~4", "fields": ["*"]}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(
                    apiKeyIds.get(0),
                    apiKeyIds.get(1),
                    apiKeyIds.get(2),
                    apiKeyIds.get(3),
                    apiKeyIds.get(4),
                    apiKeyIds.get(5)
                )
            )
        );
        assertQuery(
            API_KEY_ADMIN_AUTH_HEADER,
            """
                {"query": {"simple_query_string": {"query": "+prod +42",
                "fields": ["metadata.label", "metadata.value", "metadata.hero"]}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(apiKeyIds.get(2), apiKeyIds.get(3))
            )
        );
        assertQuery(batmanUserCredentials, """
            {"query": {"simple_query_string": {"query": "+prod key*", "fields": ["name", "username", "metadata"],
            "default_operator": "AND"}}}""", apiKeys -> assertThat(apiKeys, is(empty())));
        assertQuery(
            batmanUserCredentials,
            """
                {"query": {"simple_query_string": {"query": "+true +key*", "fields": ["name", "username", "metadata"],
                "default_operator": "AND"}}}""",
            apiKeys -> assertThat(
                apiKeys.stream().map(k -> (String) k.get("id")).toList(),
                containsInAnyOrder(apiKeyIds.get(6), apiKeyIds.get(7))
            )
        );
        assertQuery(
            batmanUserCredentials,
            """
                {"query": {"bool": {"must": [{"term": {"name": {"value":"key5-batman"}}},
                {"simple_query_string": {"query": "default_native"}}]}}}""",
            apiKeys -> assertThat(apiKeys.stream().map(k -> (String) k.get("id")).toList(), containsInAnyOrder(apiKeyIds.get(7)))
        );
    }

    public void testExistsQuery() throws IOException, InterruptedException {
        final String authHeader = randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER);

        // No expiration
        createApiKey("test-exists-1", null, null, Map.of("value", 42), authHeader);
        // A short-lived key
        createApiKey("test-exists-2", "1ms", null, Map.of("label", "prod"), authHeader);
        createApiKey("test-exists-3", "1d", null, Map.of("value", 42, "label", "prod"), authHeader);

        final long startTime = Instant.now().toEpochMilli();

        assertQuery(authHeader, """
            {"query": {"exists": {"field": "expiration" }}}""", apiKeys -> {
            assertThat(apiKeys.stream().map(k -> (String) k.get("name")).toList(), containsInAnyOrder("test-exists-2", "test-exists-3"));
        });

        assertQuery(authHeader, """
            {"query": {"exists": {"field": "metadata.value" }}}""", apiKeys -> {
            assertThat(apiKeys.stream().map(k -> (String) k.get("name")).toList(), containsInAnyOrder("test-exists-1", "test-exists-3"));
        });

        assertQuery(authHeader, """
            {"query": {"exists": {"field": "metadata.label" }}}""", apiKeys -> {
            assertThat(apiKeys.stream().map(k -> (String) k.get("name")).toList(), containsInAnyOrder("test-exists-2", "test-exists-3"));
        });

        // Create an invalidated API key
        createAndInvalidateApiKey("test-exists-4", authHeader);

        // Get the invalidated API key
        assertQuery(authHeader, """
            {"query": {"exists": {"field": "invalidation" }}}""", apiKeys -> {
            assertThat(apiKeys.stream().map(k -> (String) k.get("name")).toList(), containsInAnyOrder("test-exists-4"));
        });

        // Ensure the short-lived key is expired
        final long elapsed = Instant.now().toEpochMilli() - startTime;
        if (elapsed < 10) {
            Thread.sleep(10 - elapsed);
        }

        // Find valid API keys (not invalidated nor expired)
        assertQuery(authHeader, """
            {
              "query": {
                "bool": {
                  "must": {
                    "term": {
                      "invalidated": false
                    }
                  },
                  "must_not": {
                    "exists": {
                      "field": "invalidation"
                    }
                  },
                  "should": [
                    {
                      "range": {
                        "expiration": {
                          "gte": "now"
                        }
                      }
                    },
                    {
                      "bool": {
                        "must_not": {
                          "exists": {
                            "field": "expiration"
                          }
                        }
                      }
                    }
                  ],
                  "minimum_should_match": 1
                }
              }
            }""", apiKeys -> {
            assertThat(apiKeys.stream().map(k -> (String) k.get("name")).toList(), containsInAnyOrder("test-exists-1", "test-exists-3"));
        });
    }

    @SuppressWarnings("unchecked")
    private List<Object> extractSortValues(Map<String, Object> apiKeyInfo) {
        return (List<Object>) apiKeyInfo.get("_sort");
    }

    private int collectApiKeys(List<Map<String, Object>> apiKeyInfos, Request request, int total, int size) throws IOException {
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        final int before = apiKeyInfos.size();
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> apiKeysMap = (List<Map<String, Object>>) responseMap.get("api_keys");
        apiKeyInfos.addAll(apiKeysMap);
        assertThat(responseMap.get("total"), equalTo(total));
        final int actualSize = apiKeyInfos.size() - before;
        assertThat(responseMap.get("count"), equalTo(actualSize));
        return actualSize;
    }

    private ResponseException assertQueryError(String authHeader, int statusCode, String body) throws IOException {
        final Request request = new Request("GET", "/_security/_query/api_key");
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final ResponseException responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
        return responseException;
    }

    void assertQuery(String authHeader, String body, Consumer<List<Map<String, Object>>> apiKeysVerifier) throws IOException {
        final Request request = new Request("GET", "/_security/_query/api_key");
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> apiKeys = (List<Map<String, Object>>) responseMap.get("api_keys");
        apiKeysVerifier.accept(apiKeys);
    }

    private void createApiKeys() throws IOException {
        createApiKey(
            "my-org/ingest-key-1",
            Map.of(
                "application",
                "fleet-agent",
                "tags",
                List.of("prod", "east"),
                "environment",
                Map.of("os", "Cat", "level", 42, "system", false, "hostname", "my-org-host-1")
            ),
            API_KEY_ADMIN_AUTH_HEADER
        );

        createApiKey(
            "my-org/ingest-key-2",
            Map.of(
                "application",
                "fleet-server",
                "tags",
                List.of("staging", "east"),
                "environment",
                Map.of("os", "Dog", "level", 11, "system", true, "hostname", "my-org-host-2")
            ),
            API_KEY_ADMIN_AUTH_HEADER
        );

        createApiKey(
            "my-org/management-key-1",
            Map.of(
                "application",
                "fleet-agent",
                "tags",
                List.of("prod", "west"),
                "environment",
                Map.of("os", "Cat", "level", 11, "system", false, "hostname", "my-org-host-3")
            ),
            API_KEY_ADMIN_AUTH_HEADER
        );

        createApiKey(
            "my-org/alert-key-1",
            Map.of(
                "application",
                "siem",
                "tags",
                List.of("prod", "north", "upper"),
                "environment",
                Map.of("os", "Dog", "level", 3, "system", true, "hostname", "my-org-host-4")
            ),
            API_KEY_ADMIN_AUTH_HEADER
        );

        createApiKey(
            "my-ingest-key-1",
            Map.of("application", "cli", "tags", List.of("user", "test"), "notes", Map.of("sun", "hot", "earth", "blue")),
            API_KEY_USER_AUTH_HEADER
        );

        createApiKey(
            "my-alert-key-2",
            Map.of("application", "web", "tags", List.of("app", "prod"), "notes", Map.of("shared", false, "weather", "sunny")),
            API_KEY_USER_AUTH_HEADER
        );
    }

    static Tuple<String, String> createApiKey(String name, Map<String, Object> metadata, String authHeader) throws IOException {
        return createApiKey(name, null, metadata, authHeader);
    }

    static Tuple<String, String> createApiKey(
        String name,
        Map<String, Object> roleDescriptors,
        Map<String, Object> metadata,
        String authHeader
    ) throws IOException {
        return createApiKey(name, randomFrom("10d", null), roleDescriptors, metadata, authHeader);
    }

    static Tuple<String, String> createApiKey(
        String name,
        String expiration,
        Map<String, Object> roleDescriptors,
        Map<String, Object> metadata,
        String authHeader
    ) throws IOException {
        final Request request = new Request("POST", "/_security/api_key");
        final String roleDescriptorsString = XContentTestUtils.convertToXContent(
            roleDescriptors == null ? Map.of() : roleDescriptors,
            XContentType.JSON
        ).utf8ToString();
        final String metadataString = XContentTestUtils.convertToXContent(metadata == null ? Map.of() : metadata, XContentType.JSON)
            .utf8ToString();
        if (expiration == null) {
            request.setJsonEntity(Strings.format("""
                {"name":"%s", "role_descriptors":%s, "metadata":%s}""", name, roleDescriptorsString, metadataString));
        } else {
            request.setJsonEntity(Strings.format("""
                {"name":"%s", "expiration": "%s", "role_descriptors":%s,\
                "metadata":%s}""", name, expiration, roleDescriptorsString, metadataString));
        }
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> m = responseAsMap(response);
        return new Tuple<>((String) m.get("id"), (String) m.get("api_key"));
    }

    static Tuple<String, String> grantApiKey(
        String name,
        String expiration,
        Map<String, Object> metadata,
        String authHeader,
        String username
    ) throws IOException {
        return grantApiKey(name, expiration, null, metadata, authHeader, username);
    }

    static Tuple<String, String> grantApiKey(
        String name,
        String expiration,
        Map<String, Object> roleDescriptors,
        Map<String, Object> metadata,
        String authHeader,
        String username
    ) throws IOException {
        final Request request = new Request("POST", "/_security/api_key/grant");
        final String roleDescriptorsString = XContentTestUtils.convertToXContent(
            roleDescriptors == null ? Map.of() : roleDescriptors,
            XContentType.JSON
        ).utf8ToString();
        final String metadataString = XContentTestUtils.convertToXContent(metadata == null ? Map.of() : metadata, XContentType.JSON)
            .utf8ToString();
        final String apiKeyString;
        if (expiration == null) {
            apiKeyString = Strings.format("""
                {"name":"%s", "role_descriptors":%s, "metadata":%s}""", name, roleDescriptorsString, metadataString);
        } else {
            apiKeyString = Strings.format("""
                {"name":"%s", "expiration": "%s", "role_descriptors":%s,\
                "metadata":%s}""", name, expiration, roleDescriptorsString, metadataString);
        }
        request.setJsonEntity(Strings.format("""
            {
              "grant_type": "password",
              "username": "%s",
              "password": "super-strong-password",
              "api_key": %s
            }
            """, username, apiKeyString));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> m = responseAsMap(response);
        return new Tuple<>((String) m.get("id"), (String) m.get("api_key"));
    }

    static String createAndInvalidateApiKey(String name, String authHeader) throws IOException {
        final Tuple<String, String> tuple = createApiKey(name, null, authHeader);
        invalidateApiKey(tuple.v1(), true, authHeader);
        return tuple.v1();
    }

    static void invalidateApiKey(String id, boolean owner, String authHeader) throws IOException {
        final Request request = new Request("DELETE", "/_security/api_key");
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        request.setJsonEntity(Strings.format("""
            {"ids": ["%s"],"owner":%s}""", id, owner));
        assertOK(client().performRequest(request));
    }

    static String createUser(String username) throws IOException {
        return createUser(username, new String[0]);
    }

    static String createUser(String username, String[] roles) throws IOException {
        final Request request = new Request("POST", "/_security/user/" + username);
        Map<String, Object> body = Map.ofEntries(Map.entry("roles", roles), Map.entry("password", "super-strong-password".toString()));
        request.setJsonEntity(XContentTestUtils.convertToXContent(body, XContentType.JSON).utf8ToString());
        Response response = adminClient().performRequest(request);
        assertOK(response);
        return basicAuthHeaderValue(username, new SecureString("super-strong-password".toCharArray()));
    }

    static void createSystemWriteRole(String roleName) throws IOException {
        final Request addRole = new Request("POST", "/_security/role/" + roleName);
        addRole.setJsonEntity("""
            {
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": ["all"],
                  "allow_restricted_indices" : true
                }
              ]
            }""");
        Response response = adminClient().performRequest(addRole);
        assertOK(response);
    }

    static void expectWarnings(Request request, String... expectedWarnings) {
        final Set<String> expected = Set.of(expectedWarnings);
        RequestOptions options = request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expected) == false;
        }).build();
        request.setOptions(options);
    }

    static void updateApiKeys(String creds, String script, Collection<String> ids) throws IOException {
        if (ids.isEmpty()) {
            return;
        }
        final Request request = new Request("POST", "/.security/_update_by_query?refresh=true&wait_for_completion=true");
        request.setJsonEntity(Strings.format("""
            {
              "script": {
                "source": "%s",
                "lang": "painless"
              },
              "query": {
                "bool": {
                  "must": [
                    {"term": {"doc_type": "api_key"}},
                    {"ids": {"values": %s}}
                  ]
                }
              }
            }
            """, script, ids.stream().map(id -> "\"" + id + "\"").collect(Collectors.toList())));
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, creds));
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );
        Response response = client().performRequest(request);
        assertOK(response);
    }
}

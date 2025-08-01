/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EsqlSecurityIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("test-admin", "x-pack-test-password", "test-admin", true)
        .user("user1", "x-pack-test-password", "user1", false)
        .user("user2", "x-pack-test-password", "user2", false)
        .user("user3", "x-pack-test-password", "user3", false)
        .user("user4", "x-pack-test-password", "user4", false)
        .user("user5", "x-pack-test-password", "user5", false)
        .user("fls_user", "x-pack-test-password", "fls_user", false)
        .user("fls_user2", "x-pack-test-password", "fls_user2", false)
        .user("fls_user2_alias", "x-pack-test-password", "fls_user2_alias", false)
        .user("fls_user3", "x-pack-test-password", "fls_user3", false)
        .user("fls_user3_alias", "x-pack-test-password", "fls_user3_alias", false)
        .user("fls_user4_1", "x-pack-test-password", "fls_user4_1", false)
        .user("fls_user4_1_alias", "x-pack-test-password", "fls_user4_1_alias", false)
        .user("dls_user", "x-pack-test-password", "dls_user", false)
        .user("metadata1_read2", "x-pack-test-password", "metadata1_read2", false)
        .user("metadata1_alias_read2", "x-pack-test-password", "metadata1_alias_read2", false)
        .user("alias_user1", "x-pack-test-password", "alias_user1", false)
        .user("alias_user2", "x-pack-test-password", "alias_user2", false)
        .user("logs_foo_all", "x-pack-test-password", "logs_foo_all", false)
        .user("logs_foo_16_only", "x-pack-test-password", "logs_foo_16_only", false)
        .user("logs_foo_after_2021", "x-pack-test-password", "logs_foo_after_2021", false)
        .user("logs_foo_after_2021_pattern", "x-pack-test-password", "logs_foo_after_2021_pattern", false)
        .user("logs_foo_after_2021_alias", "x-pack-test-password", "logs_foo_after_2021_alias", false)
        .user("user_without_monitor_privileges", "x-pack-test-password", "user_without_monitor_privileges", false)
        .user("user_with_monitor_privileges", "x-pack-test-password", "user_with_monitor_privileges", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private void indexDocument(String index, int id, double value, String org) throws IOException {
        Request indexDoc = new Request("PUT", index + "/_doc/" + id);
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        builder.field("value", value);
        builder.field("org", org);
        builder.field("partial", org + value);
        indexDoc.setJsonEntity(Strings.toString(builder.endObject()));
        client().performRequest(indexDoc);
    }

    @Before
    public void indexDocuments() throws IOException {
        Settings lookupSettings = Settings.builder().put("index.mode", "lookup").build();
        String mapping = """
            "properties":{"value": {"type": "double"}, "org": {"type": "keyword"}, "other": {"type": "keyword"}}
            """;

        createIndex("index", Settings.EMPTY, mapping);
        indexDocument("index", 1, 10.0, "sales");
        indexDocument("index", 2, 20.0, "engineering");
        refresh("index");

        createIndex("index-user1", Settings.EMPTY, mapping);
        indexDocument("index-user1", 1, 12.0, "engineering");
        indexDocument("index-user1", 2, 31.0, "sales");
        refresh("index-user1");

        createIndex("index-user2", Settings.EMPTY, mapping);
        indexDocument("index-user2", 1, 32.0, "marketing");
        indexDocument("index-user2", 2, 40.0, "sales");
        refresh("index-user2");

        createIndex("indexpartial", Settings.EMPTY, mapping);
        indexDocument("indexpartial", 1, 32.0, "marketing");
        indexDocument("indexpartial", 2, 40.0, "sales");
        refresh("indexpartial");

        createIndex("lookup-user1", lookupSettings, mapping);
        indexDocument("lookup-user1", 1, 12.0, "engineering");
        indexDocument("lookup-user1", 2, 31.0, "sales");
        refresh("lookup-user1");

        createIndex("lookup-user2", lookupSettings, mapping);
        indexDocument("lookup-user2", 1, 32.0, "marketing");
        indexDocument("lookup-user2", 2, 40.0, "sales");
        refresh("lookup-user2");

        if (aliasExists("second-alias") == false) {
            Request aliasRequest = new Request("POST", "_aliases");
            aliasRequest.setJsonEntity("""
                {
                    "actions": [
                        {
                          "add": {
                            "alias": "first-alias",
                            "index": "index-user1",
                            "filter": {
                                "term": {
                                    "org": "sales"
                                }
                            }
                          }
                        },
                        {
                          "add": {
                            "alias": "lookup-first-alias",
                            "index": "lookup-user1",
                            "filter": {
                                "term": {
                                    "org": "sales"
                                }
                            }
                          }
                        },
                        {
                          "add": {
                            "alias": "lookup-second-alias",
                            "index": "lookup-user2"
                          }
                        },
                        {
                          "add": {
                            "alias": "second-alias",
                            "index": "index-user2"
                          }
                        }
                    ]
                }
                """);
            assertOK(client().performRequest(aliasRequest));
        }

        createMultiRoleUsers();
    }

    private void createMultiRoleUsers() throws IOException {
        Request request = new Request("POST", "_security/user/dls_user2");
        request.setJsonEntity("""
            {
              "password" : "x-pack-test-password",
              "roles" : [ "dls_user", "dls_user2" ],
              "full_name" : "Test Role",
              "email" : "test.role@example.com"
            }
            """);
        assertOK(client().performRequest(request));

        request = new Request("POST", "_security/user/fls_user4");
        request.setJsonEntity("""
            {
              "password" : "x-pack-test-password",
              "roles" : [ "fls_user4_1", "fls_user4_2" ],
              "full_name" : "Test Role",
              "email" : "test.role@example.com"
            }
            """);
        assertOK(client().performRequest(request));

        request = new Request("POST", "_security/user/fls_user4_alias");
        request.setJsonEntity("""
            {
              "password" : "x-pack-test-password",
              "roles" : [ "fls_user4_1_alias", "fls_user4_2_alias" ],
              "full_name" : "Test Role",
              "email" : "test.role@example.com"
            }
            """);
        assertOK(client().performRequest(request));
    }

    protected MapMatcher responseMatcher(Map<String, Object> result) {
        return getResultMatcher(result);
    }

    public void testAllowedIndices() throws Exception {
        for (String user : List.of("test-admin", "user1", "user2")) {
            Response resp = runESQLCommand(user, "from index | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> respMap = entityAsMap(resp);
            assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
            assertThat(respMap.get("values"), equalTo(List.of(List.of(30.0d))));
        }

        for (String user : List.of("test-admin", "user1")) {
            Response resp = runESQLCommand(user, "from index-user1 | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> responseMap = entityAsMap(resp);
            MapMatcher mapMatcher = responseMatcher(responseMap);
            MapMatcher matcher = mapMatcher.entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(43.0d)));
            assertMap(responseMap, matcher);
        }

        for (String user : List.of("test-admin", "user2")) {
            Response resp = runESQLCommand(user, "from index-user2 | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> responseMap = entityAsMap(resp);
            MapMatcher mapMatcher = responseMatcher(responseMap);
            MapMatcher matcher = mapMatcher.entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(72.0d)));
            assertMap(responseMap, matcher);
        }

        for (var index : List.of("index-user2", "index-user*", "index*")) {
            Response resp = runESQLCommand("metadata1_read2", "from " + index + " | stats sum=sum(value)");
            assertOK(resp);
            Map<String, Object> responseMap = entityAsMap(resp);
            MapMatcher mapMatcher = responseMatcher(responseMap);
            MapMatcher matcher = mapMatcher.entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(72.0d)));
            assertMap(responseMap, matcher);
        }
    }

    public void testAliases() throws Exception {
        for (var index : List.of("second-alias", "second-*", "second-*,index*")) {
            Response resp = runESQLCommand(
                "alias_user2",
                "from " + index + " METADATA _index" + "| stats sum=sum(value), index=VALUES(_index)"
            );
            assertOK(resp);
            Map<String, Object> responseMap = entityAsMap(resp);
            MapMatcher matcher = responseMatcher(responseMap).entry(
                "columns",
                List.of(Map.of("name", "sum", "type", "double"), Map.of("name", "index", "type", "keyword"))
            ).entry("values", List.of(List.of(72.0d, "index-user2")));
            assertMap(responseMap, matcher);
        }
    }

    public void testAliasFilter() throws Exception {
        for (var index : List.of("first-alias", "first-alias,index-*", "first-*,index-*")) {
            Response resp = runESQLCommand("alias_user1", "from " + index + " METADATA _index" + "| KEEP _index, org, value | LIMIT 10");
            assertOK(resp);
            Map<String, Object> responseMap = entityAsMap(resp);
            MapMatcher matcher = responseMatcher(responseMap).entry(
                "columns",
                List.of(
                    Map.of("name", "_index", "type", "keyword"),
                    Map.of("name", "org", "type", "keyword"),
                    Map.of("name", "value", "type", "double")
                )
            ).entry("values", List.of(List.of("index-user1", "sales", 31.0d)));
            assertMap(responseMap, matcher);
        }
    }

    public void testUnauthorizedIndices() throws IOException {
        ResponseException error;
        error = expectThrows(ResponseException.class, () -> runESQLCommand("user1", "from index-user2 | stats sum(value)"));
        assertThat(error.getMessage(), containsString("Unknown index [index-user2]"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("user2", "from index-user1 | stats sum(value)"));
        assertThat(error.getMessage(), containsString("Unknown index [index-user1]"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("alias_user2", "from index-user2 | stats sum(value)"));
        assertThat(error.getMessage(), containsString("Unknown index [index-user2]"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("metadata1_read2", "from index-user1 | stats sum(value)"));
        assertThat(error.getMessage(), containsString("Unknown index [index-user1]"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testInsufficientPrivilege() {
        ResponseException error = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_read2", "FROM index-user1 | STATS sum=sum(value)")
        );
        logger.info("error", error);
        assertThat(error.getMessage(), containsString("Unknown index [index-user1]"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    public void testIndexPatternErrorMessageComparison_ESQL_SearchDSL() throws Exception {
        // _search match_all query on the index-user1,index-user2 index pattern
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", QueryBuilders.matchAllQuery());
        json.endObject();
        Request searchRequest = new Request("GET", "/index-user1,index-user2/_search");
        searchRequest.setJsonEntity(Strings.toString(json));
        setUser(searchRequest, "metadata1_read2");

        // ES|QL query on the same index pattern
        var esqlResp = expectThrows(ResponseException.class, () -> runESQLCommand("metadata1_read2", "FROM index-user1,index-user2"));
        var srchResp = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));

        for (ResponseException r : List.of(esqlResp, srchResp)) {
            assertThat(
                EntityUtils.toString(r.getResponse().getEntity()),
                containsString(
                    "unauthorized for user [test-admin] run as [metadata1_read2] with effective roles [metadata1_read2] on indices [index-user1]"
                )
            );
        }
        assertThat(esqlResp.getResponse().getStatusLine().getStatusCode(), equalTo(srchResp.getResponse().getStatusLine().getStatusCode()));
    }

    public void testLimitedPrivilege() throws Exception {
        ResponseException resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand(
                "metadata1_read2",
                "FROM index-user1,index-user2 METADATA _index | STATS sum=sum(value), index=VALUES(_index)"
            )
        );
        assertThat(
            EntityUtils.toString(resp.getResponse().getEntity()),
            containsString(
                "unauthorized for user [test-admin] run as [metadata1_read2] with effective roles [metadata1_read2] on indices [index-user1]"
            )
        );
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_read2", "FROM index-user1,index-user2 METADATA _index | STATS index=VALUES(_index)")
        );
        assertThat(
            EntityUtils.toString(resp.getResponse().getEntity()),
            containsString(
                "unauthorized for user [test-admin] run as [metadata1_read2] with effective roles [metadata1_read2] on indices [index-user1]"
            )
        );
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_read2", "FROM index-user1,index-user2 | STATS sum=sum(value)")
        );
        assertThat(
            EntityUtils.toString(resp.getResponse().getEntity()),
            containsString(
                "unauthorized for user [test-admin] run as [metadata1_read2] with effective roles [metadata1_read2] on indices [index-user1]"
            )
        );
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("alias_user1", "FROM first-alias,index-user1 METADATA _index | KEEP _index, org, value | LIMIT 10")
        );
        assertThat(
            EntityUtils.toString(resp.getResponse().getEntity()),
            containsString(
                "unauthorized for user [test-admin] run as [alias_user1] with effective roles [alias_user1] on indices [index-user1]"
            )
        );
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand(
                "alias_user2",
                "from second-alias,index-user2 METADATA _index | stats sum=sum(value), index=VALUES(_index)"
            )
        );
        assertThat(
            EntityUtils.toString(resp.getResponse().getEntity()),
            containsString(
                "unauthorized for user [test-admin] run as [alias_user2] with effective roles [alias_user2] on indices [index-user2]"
            )
        );
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
    }

    public void testDocumentLevelSecurity() throws Exception {
        Response resp = runESQLCommand("user3", "from index | stats sum=sum(value)");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
        assertThat(respMap.get("values"), equalTo(List.of(List.of(10.0))));
    }

    public void testDocumentLevelSecurityFromStar() throws Exception {
        Response resp = runESQLCommand("user3", "from in*x | stats sum=sum(value)");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(respMap.get("columns"), equalTo(List.of(Map.of("name", "sum", "type", "double"))));
        assertThat(respMap.get("values"), equalTo(List.of(List.of(10.0))));
    }

    public void testFieldLevelSecurityAllow() throws Exception {
        Response resp = runESQLCommand("fls_user", "FROM index* | SORT value | LIMIT 1");
        assertOK(resp);
        assertMap(
            entityAsMap(resp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "partial").entry("type", "text"),
                        matchesMap().entry("name", "value").entry("type", "double")
                    )
                )
                .entry("values", List.of(List.of("sales10.0", 10.0)))
        );
    }

    public void testFieldLevelSecurityAllowPartial() throws Exception {
        Request request = new Request("GET", "/index*/_field_caps");
        setUser(request, "fls_user");
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.addParameter("fields", "*");

        request = new Request("GET", "/index*/_search");
        setUser(request, "fls_user");
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");

        Response resp = runESQLCommand("fls_user", "FROM index* | SORT partial | LIMIT 1");
        assertOK(resp);
        assertMap(
            entityAsMap(resp),
            matchesMap().extraOk()
                .entry(
                    "columns",
                    List.of(
                        matchesMap().entry("name", "partial").entry("type", "text"),
                        matchesMap().entry("name", "value").entry("type", "double")
                    )
                )
                .entry("values", List.of(List.of("engineering20.0", 20.0)))
        );
    }

    public void testFieldLevelSecuritySpellingMistake() throws Exception {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("fls_user", "FROM index* | SORT parial | LIMIT 1")
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("Unknown column [parial]"));
    }

    public void testFieldLevelSecurityNotAllowed() throws Exception {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("fls_user", "FROM index* | SORT org DESC | LIMIT 1")
        );
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("Unknown column [org]"));
    }

    public void testRowCommand() throws Exception {
        String user = randomFrom("test-admin", "user1", "user2");
        Response resp = runESQLCommand(user, "row a = 5, b = 2 | stats count=sum(b) by a");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "count", "type", "long"), Map.of("name", "a", "type", "integer")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(2, 5))));
    }

    public void testEnrich() throws Exception {
        createEnrichPolicy();
        try {
            createIndex("test-enrich", Settings.EMPTY, """
                "properties":{"timestamp": {"type": "long"}, "song_id": {"type": "keyword"}, "duration": {"type": "double"}}
                """);
            record Listen(long timestamp, String songId, double duration) {

            }
            var listens = List.of(
                new Listen(1, "s1", 1.0),
                new Listen(2, "s2", 2.0),
                new Listen(3, "s1", 3.0),
                new Listen(4, "s3", 1.0),
                new Listen(5, "s4", 1.5),
                new Listen(6, "s1", 2.5),
                new Listen(7, "s1", 3.5),
                new Listen(8, "s2", 5.0),
                new Listen(8, "s1", 0.5),
                new Listen(8, "s3", 0.25),
                new Listen(8, "s4", 1.25)
            );
            int numDocs = between(100, 1000);
            for (int i = 0; i < numDocs; i++) {
                final Listen listen;
                if (i < listens.size()) {
                    listen = listens.get(i);
                } else {
                    listen = new Listen(100 + i, "s" + between(1, 5), randomIntBetween(1, 10));
                }
                Request indexDoc = new Request("PUT", "/test-enrich/_doc/" + i);
                String doc = Strings.toString(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("timestamp", listen.timestamp)
                        .field("song_id", listen.songId)
                        .field("duration", listen.duration)
                        .endObject()
                );
                indexDoc.setJsonEntity(doc);
                client().performRequest(indexDoc);
            }
            refresh("test-enrich");

            var from = "FROM test-enrich ";
            var stats = " | stats total_duration = sum(duration) by artist | sort artist ";
            var enrich = " | ENRICH songs ON song_id ";
            var topN = " | sort timestamp | limit " + listens.size() + " ";
            var filter = " | where timestamp <= " + listens.size();

            var commands = List.of(
                from + enrich + filter + stats,
                from + filter + enrich + stats,
                from + topN + enrich + stats,
                from + enrich + topN + stats
            );
            for (String command : commands) {
                for (String user : List.of("user1", "user4")) {
                    Response resp = runESQLCommand(user, command);
                    Map<String, Object> respMap = entityAsMap(resp);
                    assertThat(
                        respMap.get("values"),
                        equalTo(List.of(List.of(2.75, "Disturbed"), List.of(10.5, "Eagles"), List.of(8.25, "Linkin Park")))
                    );
                }

                ResponseException resp = expectThrows(
                    ResponseException.class,
                    () -> runESQLCommand(
                        "user5",
                        "FROM test-enrich | ENRICH songs ON song_id | stats total_duration = sum(duration) by artist | sort artist"
                    )
                );
                assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_FORBIDDEN));
            }
        } finally {
            removeEnrichPolicy();
        }
    }

    public void testLookupJoinIndexAllowed() throws Exception {
        assumeTrue(
            "Requires LOOKUP JOIN capability",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName()))
        );

        Response resp = runESQLCommand(
            "metadata1_read2",
            "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org"
        );
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(40.0, "sales"))));

        // user is not allowed to use the alias (but is allowed to use the index)
        expectThrows(
            ResponseException.class,
            () -> runESQLCommand(
                "metadata1_read2",
                "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value | KEEP x, org"
            )
        );

        // user is not allowed to use the index (but is allowed to use the alias)
        expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_alias_read2", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org")
        );

        // user has permission on the alias, and can read the key
        resp = runESQLCommand(
            "metadata1_alias_read2",
            "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value | KEEP x, org"
        );
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(40.0, "sales"))));

        // user has permission on the alias, but can't read the key (doc level security at role level)
        resp = runESQLCommand(
            "metadata1_alias_read2",
            "ROW x = 32.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value | KEEP x, org"
        );
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        List<?> values = (List<?>) respMap.get("values");
        assertThat(values.size(), is(1));
        List<?> row = (List<?>) values.get(0);
        assertThat(row.size(), is(2));
        assertThat(row.get(0), is(32.0));
        assertThat(row.get(1), is(nullValue()));

        // user has permission on the alias, the alias has a filter that doesn't allow to see the value
        resp = runESQLCommand("alias_user1", "ROW x = 12.0 | EVAL value = x | LOOKUP JOIN lookup-first-alias ON value | KEEP x, org");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        values = (List<?>) respMap.get("values");
        assertThat(values.size(), is(1));
        row = (List<?>) values.get(0);
        assertThat(row.size(), is(2));
        assertThat(row.get(0), is(12.0));
        assertThat(row.get(1), is(nullValue()));

        // user has permission on the alias, the alias has a filter that allows to see the value
        resp = runESQLCommand("alias_user1", "ROW x = 31.0 | EVAL value = x | LOOKUP JOIN lookup-first-alias ON value | KEEP x, org");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(31.0, "sales"))));
    }

    @SuppressWarnings("unchecked")
    public void testLookupJoinDocLevelSecurity() throws Exception {
        assumeTrue(
            "Requires LOOKUP JOIN capability",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName()))
        );

        Response resp = runESQLCommand("dls_user", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );

        assertThat(respMap.get("values"), equalTo(List.of(Arrays.asList(40.0, null))));

        resp = runESQLCommand("dls_user", "ROW x = 32.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(32.0, "marketing"))));

        // same, but with a user that has two dls roles that allow him more visibility

        resp = runESQLCommand("dls_user2", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );

        assertThat(respMap.get("values"), equalTo(List.of(Arrays.asList(40.0, "sales"))));

        resp = runESQLCommand("dls_user2", "ROW x = 32.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value | KEEP x, org");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(List.of(Map.of("name", "x", "type", "double"), Map.of("name", "org", "type", "keyword")))
        );
        assertThat(respMap.get("values"), equalTo(List.of(List.of(32.0, "marketing"))));

    }

    @SuppressWarnings("unchecked")
    public void testLookupJoinFieldLevelSecurity() throws Exception {
        assumeTrue(
            "Requires LOOKUP JOIN capability",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName()))
        );

        Response resp = runESQLCommand("fls_user2", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword")
                )
            )
        );

        resp = runESQLCommand("fls_user3", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword"),
                    Map.of("name", "other", "type", "keyword")
                )
            )

        );

        resp = runESQLCommand("fls_user4", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword")
                )
            )
        );

        ResponseException error = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("fls_user4_1", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-user2 ON value")
        );
        assertThat(error.getMessage(), containsString("Unknown column [value] in right side of join"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    public void testLookupJoinFieldLevelSecurityOnAlias() throws Exception {
        assumeTrue(
            "Requires LOOKUP JOIN capability",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName()))
        );

        Response resp = runESQLCommand("fls_user2_alias", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value");
        assertOK(resp);
        Map<String, Object> respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword")
                )
            )
        );

        resp = runESQLCommand("fls_user3_alias", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword"),
                    Map.of("name", "other", "type", "keyword")
                )
            )

        );

        resp = runESQLCommand("fls_user4_alias", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value");
        assertOK(resp);
        respMap = entityAsMap(resp);
        assertThat(
            respMap.get("columns"),
            equalTo(
                List.of(
                    Map.of("name", "x", "type", "double"),
                    Map.of("name", "value", "type", "double"),
                    Map.of("name", "org", "type", "keyword")
                )
            )
        );

        ResponseException error = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("fls_user4_1_alias", "ROW x = 40.0 | EVAL value = x | LOOKUP JOIN lookup-second-alias ON value")
        );
        assertThat(error.getMessage(), containsString("Unknown column [value] in right side of join"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    public void testLookupJoinIndexForbidden() throws Exception {
        assumeTrue(
            "Requires LOOKUP JOIN capability",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.JOIN_LOOKUP_V12.capabilityName()))
        );

        var resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_read2", "FROM lookup-user2 | EVAL value = 10.0 | LOOKUP JOIN lookup-user1 ON value | KEEP x")
        );
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-user1]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand(
                "metadata1_read2",
                "FROM lookup-user2 | EVAL value = 10.0 | LOOKUP JOIN lookup-first-alias ON value | KEEP x"
            )
        );
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-first-alias]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("metadata1_read2", "ROW x = 10.0 | EVAL value = x | LOOKUP JOIN lookup-user1 ON value | KEEP x")
        );
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-user1]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

        resp = expectThrows(
            ResponseException.class,
            () -> runESQLCommand("alias_user1", "ROW x = 10.0 | EVAL value = x | LOOKUP JOIN lookup-user1 ON value | KEEP x")
        );
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-user1]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    public void testFromLookupIndexForbidden() throws Exception {
        var resp = expectThrows(ResponseException.class, () -> runESQLCommand("metadata1_read2", "FROM lookup-user1"));
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-user1]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

        resp = expectThrows(ResponseException.class, () -> runESQLCommand("metadata1_read2", "FROM lookup-first-alias"));
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-first-alias]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));

        resp = expectThrows(ResponseException.class, () -> runESQLCommand("alias_user1", "FROM lookup-user1"));
        assertThat(resp.getMessage(), containsString("Unknown index [lookup-user1]"));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(HttpStatus.SC_BAD_REQUEST));
    }

    public void testListQueryAllowed() throws Exception {
        Request request = new Request("GET", "_query/queries");
        setUser(request, "user_with_monitor_privileges");
        var resp = client().performRequest(request);
        assertOK(resp);
    }

    public void testListQueryForbidden() throws Exception {
        Request request = new Request("GET", "_query/queries");
        setUser(request, "user_without_monitor_privileges");
        var resp = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(resp.getMessage(), containsString("this action is granted by the cluster privileges [monitor_esql,monitor,manage,all]"));
    }

    public void testGetQueryAllowed() throws Exception {
        // This is a bit tricky, since there is no such running query. We just make sure it didn't fail on forbidden privileges.
        setUser(GET_QUERY_REQUEST, "user_with_monitor_privileges");
        var resp = expectThrows(ResponseException.class, () -> client().performRequest(GET_QUERY_REQUEST));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), not(equalTo(403)));
    }

    public void testGetQueryForbidden() throws Exception {
        setUser(GET_QUERY_REQUEST, "user_without_monitor_privileges");
        var resp = expectThrows(ResponseException.class, () -> client().performRequest(GET_QUERY_REQUEST));
        assertThat(resp.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(resp.getMessage(), containsString("this action is granted by the cluster privileges [monitor_esql,monitor,manage,all]"));
    }

    private static final Request GET_QUERY_REQUEST = new Request(
        "GET",
        "_query/queries/FmJKWHpFRi1OU0l5SU1YcnpuWWhoUWcZWDFuYUJBeW1TY0dKM3otWUs2bDJudzo1Mg=="
    );

    private void createEnrichPolicy() throws Exception {
        createIndex("songs", Settings.EMPTY, """
            "properties":{"song_id": {"type": "keyword"}, "title": {"type": "keyword"}, "artist": {"type": "keyword"} }
            """);
        record Song(String id, String title, String artist) {

        }

        var songs = List.of(
            new Song("s1", "Hotel California", "Eagles"),
            new Song("s2", "In The End", "Linkin Park"),
            new Song("s3", "Numb", "Linkin Park"),
            new Song("s4", "The Sound Of Silence", "Disturbed")
        );
        for (int i = 0; i < songs.size(); i++) {
            var song = songs.get(i);
            Request indexDoc = new Request("PUT", "/songs/_doc/" + i);
            String doc = Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("song_id", song.id)
                    .field("title", song.title)
                    .field("artist", song.artist)
                    .endObject()
            );
            indexDoc.setJsonEntity(doc);
            client().performRequest(indexDoc);
        }
        refresh("songs");

        Request createEnrich = new Request("PUT", "/_enrich/policy/songs");
        createEnrich.setJsonEntity("""
            {
                "match": {
                    "indices": "songs",
                    "match_field": "song_id",
                    "enrich_fields": ["title", "artist"]
                }
            }
            """);
        client().performRequest(createEnrich);
        client().performRequest(new Request("PUT", "_enrich/policy/songs/_execute"));
    }

    private void removeEnrichPolicy() throws Exception {
        client().performRequest(new Request("DELETE", "_enrich/policy/songs"));
    }

    public void testDataStream() throws IOException {
        createDataStream();
        MapMatcher twoResults = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(2)));
        MapMatcher oneResult = matchesMap().extraOk().entry("values", matchesList().item(matchesList().item(1)));
        assertMap(entityAsMap(runESQLCommand("logs_foo_all", "FROM logs-foo | STATS COUNT(*)")), twoResults);
        assertMap(entityAsMap(runESQLCommand("logs_foo_16_only", "FROM logs-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021", "FROM logs-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_pattern", "FROM logs-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_alias", "FROM alias-foo | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_all", "FROM logs-* | STATS COUNT(*)")), twoResults);
        assertMap(entityAsMap(runESQLCommand("logs_foo_16_only", "FROM logs-* | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021", "FROM logs-* | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_pattern", "FROM logs-* | STATS COUNT(*)")), oneResult);
        assertMap(entityAsMap(runESQLCommand("logs_foo_after_2021_alias", "FROM alias-* | STATS COUNT(*)")), oneResult);
    }

    protected Response runESQLCommand(String user, String command) throws IOException {
        if (command.toLowerCase(Locale.ROOT).contains("limit") == false) {
            // add a (high) limit to avoid warnings on default limit
            command += " | limit 10000000";
        }
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", command);
        addRandomPragmas(json);
        json.endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(Strings.toString(json));
        setUser(request, user);
        request.addParameter("error_trace", "true");
        return client().performRequest(request);
    }

    private static void setUser(Request request, String user) {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));

    }

    static void addRandomPragmas(XContentBuilder builder) throws IOException {
        if (Build.current().isSnapshot()) {
            Settings pragmas = randomPragmas();
            if (pragmas != Settings.EMPTY) {
                builder.startObject("pragma");
                builder.value(pragmas);
                builder.endObject();
            }
        }
    }

    private static Settings randomPragmas() {
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
        if (randomBoolean()) {
            settings.put("node_level_reduction", randomBoolean());
        }
        return settings.build();
    }

    private void createDataStream() throws IOException {
        createDataStreamPolicy();
        createDataStreamComponentTemplate();
        createDataStreamIndexTemplate();
        createDataStreamDocuments();
        createDataStreamAlias();
    }

    private void createDataStreamPolicy() throws IOException {
        Request request = new Request("PUT", "_ilm/policy/my-lifecycle-policy");
        request.setJsonEntity("""
            {
              "policy": {
                "phases": {
                  "hot": {
                    "actions": {
                      "rollover": {
                        "max_primary_shard_size": "50gb"
                      }
                    }
                  },
                  "delete": {
                    "min_age": "735d",
                    "actions": {
                      "delete": {}
                    }
                  }
                }
              }
            }""");
        client().performRequest(request);
    }

    private void createDataStreamComponentTemplate() throws IOException {
        Request request = new Request("PUT", "_component_template/my-template");
        request.setJsonEntity("""
            {
                "template": {
                   "settings": {
                        "index.lifecycle.name": "my-lifecycle-policy"
                   },
                   "mappings": {
                       "properties": {
                           "@timestamp": {
                               "type": "date",
                               "format": "date_optional_time||epoch_millis"
                           },
                           "data_stream": {
                               "properties": {
                                   "namespace": {"type": "keyword"}
                               }
                           }
                       }
                   }
                }
            }""");
        client().performRequest(request);
    }

    private void createDataStreamIndexTemplate() throws IOException {
        Request request = new Request("PUT", "_index_template/my-index-template");
        request.setJsonEntity("""
            {
                "index_patterns": ["logs-*"],
                "data_stream": {},
                "composed_of": ["my-template"],
                "priority": 500
            }""");
        client().performRequest(request);
    }

    private void createDataStreamDocuments() throws IOException {
        Request request = new Request("POST", "logs-foo/_bulk");
        request.addParameter("refresh", "");
        request.setJsonEntity("""
            { "create" : {} }
            { "@timestamp": "2099-05-06T16:21:15.000Z", "data_stream": {"namespace": "16"} }
            { "create" : {} }
            { "@timestamp": "2001-05-06T16:21:15.000Z", "data_stream": {"namespace": "17"} }
            """);
        assertMap(entityAsMap(client().performRequest(request)), matchesMap().extraOk().entry("errors", false));
    }

    private void createDataStreamAlias() throws IOException {
        Request request = new Request("POST", "_aliases");
        request.setJsonEntity("""
            {
              "actions": [
                {
                  "add": {
                    "index": "logs-foo",
                    "alias": "alias-foo"
                  }
                }
              ]
            }""");
        assertMap(entityAsMap(client().performRequest(request)), matchesMap().extraOk().entry("errors", false));
    }
}

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
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        .user("metadata1_read2", "x-pack-test-password", "metadata1_read2", false)
        .user("alias_user1", "x-pack-test-password", "alias_user1", false)
        .user("alias_user2", "x-pack-test-password", "alias_user2", false)
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
        String mapping = """
            "properties":{"value": {"type": "double"}, "org": {"type": "keyword"}}
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
                            "alias": "second-alias",
                            "index": "index-user2"
                          }
                        }
                    ]
                }
                """);
            assertOK(client().performRequest(aliasRequest));
        }
    }

    protected MapMatcher responseMatcher() {
        return matchesMap();
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
            MapMatcher matcher = responseMatcher().entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(43.0d)));
            assertMap(entityAsMap(resp), matcher);
        }

        for (String user : List.of("test-admin", "user2")) {
            Response resp = runESQLCommand(user, "from index-user2 | stats sum=sum(value)");
            assertOK(resp);
            MapMatcher matcher = responseMatcher().entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(72.0d)));
            assertMap(entityAsMap(resp), matcher);
        }
        for (var index : List.of("index-user2", "index-user*", "index*")) {
            Response resp = runESQLCommand("metadata1_read2", "from " + index + " | stats sum=sum(value)");
            assertOK(resp);
            MapMatcher matcher = responseMatcher().entry("columns", List.of(Map.of("name", "sum", "type", "double")))
                .entry("values", List.of(List.of(72.0d)));
            assertMap(entityAsMap(resp), matcher);
        }
    }

    public void testAliases() throws Exception {
        for (var index : List.of("second-alias", "second-*", "second-*,index*")) {
            Response resp = runESQLCommand(
                "alias_user2",
                "from " + index + " METADATA _index" + "| stats sum=sum(value), index=VALUES(_index)"
            );
            assertOK(resp);
            MapMatcher matcher = responseMatcher().entry(
                "columns",
                List.of(Map.of("name", "sum", "type", "double"), Map.of("name", "index", "type", "keyword"))
            ).entry("values", List.of(List.of(72.0d, "index-user2")));
            assertMap(entityAsMap(resp), matcher);
        }
    }

    public void testAliasFilter() throws Exception {
        for (var index : List.of("first-alias", "first-alias,index-*", "first-*,index-*")) {
            Response resp = runESQLCommand("alias_user1", "from " + index + " METADATA _index" + "| KEEP _index, org, value | LIMIT 10");
            assertOK(resp);
            MapMatcher matcher = responseMatcher().entry(
                "columns",
                List.of(
                    Map.of("name", "_index", "type", "keyword"),
                    Map.of("name", "org", "type", "keyword"),
                    Map.of("name", "value", "type", "double")
                )
            ).entry("values", List.of(List.of("index-user1", "sales", 31.0d)));
            assertMap(entityAsMap(resp), matcher);
        }
    }

    public void testUnauthorizedIndices() throws IOException {
        ResponseException error;
        error = expectThrows(ResponseException.class, () -> runESQLCommand("user1", "from index-user2 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("user2", "from index-user1 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("alias_user2", "from index-user2 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));

        error = expectThrows(ResponseException.class, () -> runESQLCommand("metadata1_read2", "from index-user1 | stats sum(value)"));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
    }

    public void testInsufficientPrivilege() {
        Exception error = expectThrows(Exception.class, () -> runESQLCommand("metadata1_read2", "FROM index-user1 | STATS sum=sum(value)"));
        logger.info("error", error);
        assertThat(error.getMessage(), containsString("Unknown index [index-user1]"));
    }

    public void testIndexPatternErrorMessageComparison_ESQL_SearchDSL() throws Exception {
        // _search match_all query on the index-user1,index-user2 index pattern
        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("query", QueryBuilders.matchAllQuery());
        json.endObject();
        Request searchRequest = new Request("GET", "/index-user1,index-user2/_search");
        searchRequest.setJsonEntity(Strings.toString(json));
        searchRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", "metadata1_read2"));

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
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", "fls_user"));
        request.addParameter("error_trace", "true");
        request.addParameter("pretty", "true");
        request.addParameter("fields", "*");

        request = new Request("GET", "/index*/_search");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", "fls_user"));
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
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("es-security-runas-user", user));
        request.addParameter("error_trace", "true");
        return client().performRequest(request);
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

    static Settings randomPragmas() {
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
}

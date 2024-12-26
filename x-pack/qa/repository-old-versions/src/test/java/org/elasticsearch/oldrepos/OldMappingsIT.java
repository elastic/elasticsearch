/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class OldMappingsIT extends ESRestTestCase {

    static final Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

    static boolean setupDone;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void setupIndex() throws IOException {
        final boolean afterRestart = Booleans.parseBoolean(System.getProperty("tests.after_restart"));
        if (afterRestart) {
            return;
        }

        // The following is bit of a hack. While we wish we could make this an @BeforeClass, it does not work because the client() is only
        // initialized later, so we do it when running the first test
        if (setupDone) {
            return;
        }

        setupDone = true;

        String repoLocation = PathUtils.get(System.getProperty("tests.repo.location"))
            .resolve(RandomizedTest.getContext().getTargetClass().getName())
            .toString();

        String repoName = "old_mappings_repo";
        String snapshotName = "snap";
        List<String> indices;
        if (oldVersion.before(Version.fromString("6.0.0"))) {
            indices = Arrays.asList("filebeat", "winlogbeat", "custom", "nested");
        } else {
            indices = Arrays.asList("filebeat", "custom", "nested");
        }

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {

            assertOK(oldEs.performRequest(createIndex("filebeat", "filebeat.json")));
            if (oldVersion.before(Version.fromString("6.0.0"))) {
                assertOK(oldEs.performRequest(createIndex("winlogbeat", "winlogbeat.json")));
            }
            assertOK(oldEs.performRequest(createIndex("custom", "custom.json")));
            assertOK(oldEs.performRequest(createIndex("nested", "nested.json")));

            Request doc1 = new Request("PUT", "/" + "custom" + "/" + "doc" + "/" + "1");
            doc1.addParameter("refresh", "true");
            XContentBuilder bodyDoc1 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl1")
                .field("agent", "agent1")
                .endObject()
                .endObject()
                .endObject();
            doc1.setJsonEntity(Strings.toString(bodyDoc1));
            assertOK(oldEs.performRequest(doc1));

            Request doc2 = new Request("PUT", "/" + "custom" + "/" + "doc" + "/" + "2");
            doc2.addParameter("refresh", "true");
            XContentBuilder bodyDoc2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl2")
                .field("agent", "agent2 agent2")
                .endObject()
                .endObject()
                .field("completion", "some_value")
                .endObject();
            doc2.setJsonEntity(Strings.toString(bodyDoc2));
            assertOK(oldEs.performRequest(doc2));

            Request doc3 = new Request("PUT", "/" + "nested" + "/" + "doc" + "/" + "1");
            doc3.addParameter("refresh", "true");
            XContentBuilder bodyDoc3 = XContentFactory.jsonBuilder()
                .startObject()
                .field("group", "fans")
                .startArray("user")
                .startObject()
                .field("first", "John")
                .field("last", "Smith")
                .endObject()
                .startObject()
                .field("first", "Alice")
                .field("last", "White")
                .endObject()
                .endArray()
                .endObject();
            doc3.setJsonEntity(Strings.toString(bodyDoc3));
            assertOK(oldEs.performRequest(doc3));

            // register repo on old ES and take snapshot
            Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest.setJsonEntity(Strings.format("""
                {"type":"fs","settings":{"location":"%s"}}
                """, repoLocation));
            assertOK(oldEs.performRequest(createRepoRequest));

            Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
            assertOK(oldEs.performRequest(createSnapshotRequest));
        }

        // register repo on new ES and restore snapshot
        Request createRepoRequest2 = new Request("PUT", "/_snapshot/" + repoName);
        createRepoRequest2.setJsonEntity(Strings.format("""
            {"type":"fs","settings":{"location":"%s"}}
            """, repoLocation));
        assertOK(client().performRequest(createRepoRequest2));

        final Request createRestoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        createRestoreRequest.addParameter("wait_for_completion", "true");
        createRestoreRequest.setJsonEntity("{\"indices\":\"" + indices.stream().collect(Collectors.joining(",")) + "\"}");
        createRestoreRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Response response = client().performRequest(createRestoreRequest);
        // check deprecation warning for "_field_name" disabling
        assertTrue(response.getWarnings().stream().filter(s -> s.contains("Disabling _field_names is not necessary")).count() > 0);
        assertOK(response);
    }

    private Request createIndex(String indexName, String file) throws IOException {
        Request createIndex = new Request("PUT", "/" + indexName);
        int numberOfShards = randomIntBetween(1, 3);

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("index.number_of_shards", numberOfShards)
            .endObject()
            .startObject("mappings");
        builder.rawValue(OldMappingsIT.class.getResourceAsStream(file), XContentType.JSON);
        builder.endObject().endObject();

        createIndex.setJsonEntity(Strings.toString(builder));
        return createIndex;
    }

    public void testMappingOk() throws IOException {
        Request mappingRequest = new Request("GET", "/" + "filebeat" + "/_mapping");
        Map<String, Object> mapping = entityAsMap(client().performRequest(mappingRequest));
        assertNotNull(XContentMapValues.extractValue(mapping, "filebeat", "mappings", "properties", "apache2"));

        if (oldVersion.before(Version.fromString("6.0.0"))) {
            mappingRequest = new Request("GET", "/" + "winlogbeat" + "/_mapping");
            mapping = entityAsMap(client().performRequest(mappingRequest));
            assertNotNull(XContentMapValues.extractValue(mapping, "winlogbeat", "mappings", "properties", "message"));
        }
    }

    public void testSearchKeyword() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.url")
            .field("query", "myurl2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
    }

    public void testSearchOnPlaceHolderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("completion")
            .field("query", "some-agent")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(
            re.getMessage(),
            containsString("Field [completion] of type [completion] in legacy index does not support match queries")
        );
    }

    public void testAggregationOnPlaceholderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("aggs")
            .startObject("agents")
            .startObject("terms")
            .field("field", "completion")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(re.getMessage(), containsString("can't run aggregation or sorts on field type completion of legacy index"));
    }

    public void testConstantScoringOnTextField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.agent")
            .field("query", "agent2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> hit = (Map<String, Object>) hits.get(0);
        assertThat(hit, hasKey("_score"));
        assertEquals(1.0d, (double) hit.get("_score"), 0.01d);
    }

    public void testFieldsExistQueryOnTextField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("exists")
            .field("field", "apache2.access.agent")
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(2));
    }

    public void testSearchFieldsOnPlaceholderField() throws IOException {
        Request search = new Request("POST", "/" + "custom" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.url")
            .field("query", "myurl2")
            .endObject()
            .endObject()
            .endObject()
            .startArray("fields")
            .value("completion")
            .endArray()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        logger.info(hits);
        Map<?, ?> fields = (Map<?, ?>) (XContentMapValues.extractValue("fields", (Map<?, ?>) hits.get(0)));
        assertEquals(List.of("some_value"), fields.get("completion"));
    }

    public void testNestedDocuments() throws IOException {
        Request search = new Request("POST", "/" + "nested" + "/_search");
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        logger.info(response);
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        Map<?, ?> source = (Map<?, ?>) (XContentMapValues.extractValue("_source", (Map<?, ?>) hits.get(0)));
        assertEquals("fans", source.get("group"));

        search = new Request("POST", "/" + "nested" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("nested")
            .field("path", "user")
            .startObject("query")
            .startObject("bool")
            .startArray("must")
            .startObject()
            .startObject("match")
            .field("user.first", "Alice")
            .endObject()
            .endObject()
            .startObject()
            .startObject("match")
            .field("user.last", "White")
            .endObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        response = entityAsMap(client().performRequest(search));
        logger.info(response);
        hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        source = (Map<?, ?>) (XContentMapValues.extractValue("_source", (Map<?, ?>) hits.get(0)));
        assertEquals("fans", source.get("group"));
    }

}

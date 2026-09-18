/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.test.SkipInFIPSMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DLMFrozenTransitionWithSecurityIT extends ESRestTestCase {

    private static final String USER_TEST_ADMIN = "test_admin";
    private static final String PASSWORD = "secret-test-password";
    private static final String jsonDoc = """
        {"@timestamp": "2026-01-01T00:00:00Z", "name" : "elasticsearch", "body": "foo bar" }""";

    public static TemporaryFolder repoDir = new TemporaryFolder();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .module("analysis-common")
        .module("data-streams")
        .module("searchable-snapshots")
        .module("dlm-frozen-transition")
        .setting("path.repo", DLMFrozenTransitionWithSecurityIT::getAbsoluteRepoPath)
        .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
        .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting(DLMFrozenTransitionService.POLL_INTERVAL_SETTING.getKey(), "1s")
        .user(USER_TEST_ADMIN, PASSWORD, "superuser", false)
        .build();

    @ClassRule
    public static RuleChain ruleChain = RuleChain.outerRule(new SkipInFIPSMode()).around(repoDir).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // Use admin for entire test.
        return restAdminSettings();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(USER_TEST_ADMIN, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @SuppressForbidden(reason = "TemporaryFolder uses java.io.File")
    protected static String getAbsoluteRepoPath() {
        return repoDir.getRoot().getAbsolutePath();
    }

    @Before
    public void init() throws Exception {
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder pollIntervalEntity = JsonXContent.contentBuilder();
        pollIntervalEntity.startObject();
        pollIntervalEntity.startObject("persistent");
        pollIntervalEntity.field(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        pollIntervalEntity.field(DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.getKey(), "1");
        pollIntervalEntity.endObject();
        pollIntervalEntity.endObject();
        request.setJsonEntity(Strings.toString(pollIntervalEntity));
        assertOK(adminClient().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    public void testCanTransitionIndex() throws Exception {
        createSnapshotRepo("test-repository");
        setDefaultRepo("test-repository");
        try {
            createTemplate("foo-template", "foo-logs-*");
            createDataStream("foo-logs-1");
            indexDocs("foo-logs-1", randomIntBetween(5, 50));
            refresh(adminClient(), "foo-logs-1");

            // Capture the write index
            ObjectPath preRolloverDataStream = getDataStream("foo-logs-1");
            String candidateIndex = preRolloverDataStream.evaluate("data_streams.0.indices.0.index_name");
            String cloneIndexName = DLMConvertToFrozen.CLONE_INDEX_PREFIX + candidateIndex;
            String expectedFrozenIndexName = DLMConvertToFrozen.SNAPSHOT_NAME_PREFIX + candidateIndex;

            // Rollover and wait for two indices
            rollover("foo-logs-1");

            // Wait for transition
            assertBusy(() -> {
                ObjectPath dataStream = getDataStream("foo-logs-1");
                assertEquals(1, dataStream.evaluateArraySize("data_streams"));
                assertEquals("foo-logs-1", dataStream.evaluate("data_streams.0.name"));
                List<Map<String, Object>> indices = dataStream.evaluate("data_streams.0.indices");
                boolean hasFrozenIndex = indices.stream().anyMatch(idx -> idx.get("index_name").equals(expectedFrozenIndexName));
                assertTrue("frozen index [" + expectedFrozenIndexName + "] is not present on data stream", hasFrozenIndex);
            }, 1, TimeUnit.MINUTES);

            // Verify cleanup
            assertBusy(() -> assertFalse("index [" + candidateIndex + "] still exists", indexExists(candidateIndex)));
            assertBusy(() -> assertFalse("index [" + cloneIndexName + "] still exists", indexExists(cloneIndexName)));

            // Ensure frozen index is marked correctly
            Map<String, Object> frozenIndexInfo = getIndex(expectedFrozenIndexName);
            // Get the index name from the map directly because the name is full of period characters
            Object object = frozenIndexInfo.get(expectedFrozenIndexName);
            assertNotNull(object);
            // Then use the object path
            String dlmCreated = ObjectPath.evaluate(object, "settings." + DataStreamLifecycleService.DLM_CREATED_SETTING_KEY);
            assertEquals("true", dlmCreated);
        } finally {
            // Be a good citizen and unset the default repo or else we cannot delete the repository
            unsetDefaultRepo();
        }
    }

    public static void createSnapshotRepo(String repoName) throws IOException {
        String location = getAbsoluteRepoPath() + "/" + randomAlphaOfLengthBetween(4, 10);
        Settings settings = Settings.builder().put("location", location).put("max_snapshot_bytes_per_sec", "100m").build();
        registerRepository(adminClient(), repoName, "fs", true, settings);
    }

    public static void setDefaultRepo(String repoName) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.format("""
            {
              "persistent": {
                "repositories.default_repository": "%s"
              }
            }
            """, repoName));
        assertOK(adminClient().performRequest(request));
    }

    public static void unsetDefaultRepo() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.format("""
            {
              "persistent": {
                "repositories.default_repository": null
              }
            }
            """));
        assertOK(adminClient().performRequest(request));
    }

    private void createTemplate(String name, String pattern) throws IOException {
        Request request = new Request("PUT", "/_index_template/" + name);
        request.setJsonEntity(Strings.format("""
            {
              "index_patterns": ["%s"],
              "priority": 300,
              "template": {
                "settings": {
                  "number_of_shards": 1,
                  "number_of_replicas": 1
                },
                "lifecycle": {
                  "data_retention": "90d",
                  "frozen_after": "1s",
                  "enabled": true
                }
              },
              "data_stream": {}
            }
            """, pattern));
        assertOK(adminClient().performRequest(request));
    }

    private void createDataStream(String name) throws IOException {
        Request request = new Request("PUT", "/_data_stream/" + name);
        assertOK(adminClient().performRequest(request));
    }

    private static ObjectPath getDataStream(String name) throws IOException {
        Request request = new Request("GET", "/_data_stream/" + name);
        Response response = adminClient().performRequest(request);
        return ObjectPath.createFromResponse(response);
    }

    private static Map<String, Object> getIndex(String name) throws IOException {
        Request request = new Request("GET", "/" + name);
        Response response = adminClient().performRequest(request);
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), false);
    }

    private void indexDocs(String index, int numDocs) throws IOException {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            body.append("{\"create\":{}}\n").append(jsonDoc).append("\n");
        }
        body.append("\r\n");
        Request request = new Request("POST", "/" + index + "/_bulk");
        request.setJsonEntity(body.toString());
        request.addParameter("refresh", "true");
        assertOK(adminClient().performRequest(request));
    }

    private void rollover(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_rollover");
        assertOK(adminClient().performRequest(request));
    }
}

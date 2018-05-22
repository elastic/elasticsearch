/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class FollowIndexSecurityIT extends ESRestTestCase {

    private final boolean runningAgainstLeaderCluster = Booleans.parseBoolean(System.getProperty("tests.is_leader_cluster"));

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_ccr", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 16;
        final String allowedIndex = "allowed-index";
        final String unallowedIndex  = "unallowed-index";
        if (runningAgainstLeaderCluster) {
            logger.info("Running against leader cluster");
            Settings indexSettings = Settings.builder().put("index.soft_deletes.enabled", true).build();
            createIndex(allowedIndex, indexSettings);
            createIndex(unallowedIndex, indexSettings);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(allowedIndex, Integer.toString(i), "field", i);
            }
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(unallowedIndex, Integer.toString(i), "field", i);
            }
            refresh(allowedIndex);
            verifyDocuments(adminClient(), allowedIndex, numDocs);
        } else {
            createAndFollowIndex("leader_cluster:" + allowedIndex, allowedIndex);
            assertBusy(() -> verifyDocuments(client(), allowedIndex, numDocs));
            assertThat(countCcrNodeTasks(), equalTo(1));
            assertOK(client().performRequest("POST", "/" + allowedIndex + "/_xpack/ccr/_unfollow"));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest("GET", "/_cluster/state"));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });
    
            followIndex("leader_cluster:" + allowedIndex, allowedIndex);
            assertThat(countCcrNodeTasks(), equalTo(1));
            assertOK(client().performRequest("POST", "/" + allowedIndex + "/_xpack/ccr/_unfollow"));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest("GET", "/_cluster/state"));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });
    
            createAndFollowIndex("leader_cluster:" + unallowedIndex, unallowedIndex);
            // Verify that nothing has been replicated and no node tasks are running
            // These node tasks should have been failed due to the fact that the user
            // has no sufficient priviledges.
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));
            verifyDocuments(adminClient(), unallowedIndex, 0);
            
            followIndex("leader_cluster:" + unallowedIndex, unallowedIndex);
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));
            verifyDocuments(adminClient(), unallowedIndex, 0);
        }
    }

    private int countCcrNodeTasks() throws IOException {
        Map<String, Object> rsp1 = toMap(adminClient().performRequest("GET", "/_tasks",
            Collections.singletonMap("detailed", "true")));
        Map<?, ?> nodes = (Map<?, ?>) rsp1.get("nodes");
        assertThat(nodes.size(), equalTo(1));
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        Map<?, ?> nodeTasks = (Map<?, ?>) node.get("tasks");
        int numNodeTasks = 0;
        for (Map.Entry<?, ?> entry : nodeTasks.entrySet()) {
            Map<?, ?> nodeTask = (Map<?, ?>) entry.getValue();
            String action = (String) nodeTask.get("action");
            if (action.startsWith("shard_follow")) {
                numNodeTasks++;
            }
        }
        return numNodeTasks;
    }

    private static void index(String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        assertOK(adminClient().performRequest("POST", "/" + index + "/doc/" + id, emptyMap(),
                new StringEntity(Strings.toString(document), ContentType.APPLICATION_JSON)));
    }

    private static void refresh(String index) throws IOException {
        assertOK(adminClient().performRequest("POST", "/" + index + "/_refresh"));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        Map<String, String> params = Collections.singletonMap("leader_index", leaderIndex);
        assertOK(client().performRequest("POST", "/" + followIndex + "/_xpack/ccr/_follow", params));
    }

    private static void createAndFollowIndex(String leaderIndex, String followIndex) throws IOException {
        Map<String, String> params = Collections.singletonMap("leader_index", leaderIndex);
        assertOK(client().performRequest("POST", "/" + followIndex + "/_xpack/ccr/_create_and_follow", params));
    }

    void verifyDocuments(RestClient client, String index, int expectedNumDocs) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("size", Integer.toString(expectedNumDocs));
        params.put("sort", "field:asc");
        params.put("pretty", "true");
        Map<String, ?> response = toMap(client.performRequest("GET", "/" + index + "/_search", params));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(numDocs, equalTo(expectedNumDocs));

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), equalTo(expectedNumDocs));
        for (int i = 0; i < expectedNumDocs; i++) {
            int value = (int) XContentMapValues.extractValue("_source.field", (Map<?, ?>) hits.get(i));
            assertThat(i, equalTo(value));
        }
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    protected static void createIndex(String name, Settings settings) throws IOException {
        createIndex(name, settings, "");
    }

    protected static void createIndex(String name, Settings settings, String mapping) throws IOException {
        assertOK(adminClient().performRequest(HttpPut.METHOD_NAME, name, Collections.emptyMap(),
            new StringEntity("{ \"settings\": " + Strings.toString(settings)
                + ", \"mappings\" : {" + mapping + "} }", ContentType.APPLICATION_JSON)));
    }

}

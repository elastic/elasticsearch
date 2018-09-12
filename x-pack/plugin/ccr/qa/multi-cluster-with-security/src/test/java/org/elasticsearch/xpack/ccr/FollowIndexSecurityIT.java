/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_ccr/unfollow")));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });

            followIndex("leader_cluster:" + allowedIndex, allowedIndex);
            assertThat(countCcrNodeTasks(), equalTo(1));
            assertOK(client().performRequest(new Request("POST", "/" + allowedIndex + "/_ccr/unfollow")));
            // Make sure that there are no other ccr relates operations running:
            assertBusy(() -> {
                Map<String, Object> clusterState = toMap(adminClient().performRequest(new Request("GET", "/_cluster/state")));
                List<?> tasks = (List<?>) XContentMapValues.extractValue("metadata.persistent_tasks.tasks", clusterState);
                assertThat(tasks.size(), equalTo(0));
                assertThat(countCcrNodeTasks(), equalTo(0));
            });

            Exception e = expectThrows(ResponseException.class,
                () -> createAndFollowIndex("leader_cluster:" + unallowedIndex, unallowedIndex));
            assertThat(e.getMessage(),
                containsString("action [indices:admin/xpack/ccr/create_and_follow_index] is unauthorized for user [test_ccr]"));
            // Verify that the follow index has not been created and no node tasks are running
            assertThat(indexExists(adminClient(), unallowedIndex), is(false));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));

            e = expectThrows(ResponseException.class,
                () -> followIndex("leader_cluster:" + unallowedIndex, unallowedIndex));
            assertThat(e.getMessage(), containsString("follow index [" + unallowedIndex + "] does not exist"));
            assertThat(indexExists(adminClient(), unallowedIndex), is(false));
            assertBusy(() -> assertThat(countCcrNodeTasks(), equalTo(0)));
        }
    }

    private int countCcrNodeTasks() throws IOException {
        final Request request = new Request("GET", "/_tasks");
        request.addParameter("detailed", "true");
        Map<String, Object> rsp1 = toMap(adminClient().performRequest(request));
        Map<?, ?> nodes = (Map<?, ?>) rsp1.get("nodes");
        assertThat(nodes.size(), equalTo(1));
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        Map<?, ?> nodeTasks = (Map<?, ?>) node.get("tasks");
        int numNodeTasks = 0;
        for (Map.Entry<?, ?> entry : nodeTasks.entrySet()) {
            Map<?, ?> nodeTask = (Map<?, ?>) entry.getValue();
            String action = (String) nodeTask.get("action");
            if (action.startsWith("xpack/ccr/shard_follow_task")) {
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
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(adminClient().performRequest(request));
    }

    private static void refresh(String index) throws IOException {
        assertOK(adminClient().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("POST", "/" + followIndex + "/_ccr/follow");
        request.setJsonEntity("{\"leader_index\": \"" + leaderIndex + "\", \"idle_shard_retry_delay\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void createAndFollowIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("POST", "/" + followIndex + "/_ccr/create_and_follow");
        request.setJsonEntity("{\"leader_index\": \"" + leaderIndex + "\", \"idle_shard_retry_delay\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    void verifyDocuments(RestClient client, String index, int expectedNumDocs) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("pretty", "true");
        request.addParameter("size", Integer.toString(expectedNumDocs));
        request.addParameter("sort", "field:asc");
        Map<String, ?> response = toMap(client.performRequest(request));

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
        final Request request = new Request("PUT", "/" + name);
        request.setJsonEntity("{ \"settings\": " + Strings.toString(settings) + ", \"mappings\" : {" + mapping + "} }");
        assertOK(adminClient().performRequest(request));
    }

    private static boolean indexExists(RestClient client, String index) throws IOException {
        Response response = client.performRequest(new Request("HEAD", "/" + index));
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.common;

import org.apache.logging.log4j.Level;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class TransformCommonRestTestCase extends ESRestTestCase {

    protected static final String TRANSFORM_ENDPOINT = TransformField.REST_BASE_PATH_TRANSFORMS;
    protected static final String AUTH_KEY = "Authorization";
    protected static final String SECONDARY_AUTH_KEY = "es-secondary-authorization";

    protected static String getTransformEndpoint() {
        return TRANSFORM_ENDPOINT;
    }

    /**
     * Returns the list of transform tasks as reported by the _tasks API.
     */
    @SuppressWarnings("unchecked")
    protected List<String> getTransformTasks() throws IOException {
        Request tasksRequest = new Request("GET", "/_tasks");
        tasksRequest.addParameter("actions", TransformField.TASK_NAME + "*");
        Map<String, Object> tasksResponse = entityAsMap(client().performRequest(tasksRequest));

        Map<String, Object> nodes = (Map<String, Object>) tasksResponse.get("nodes");
        if (nodes == null) {
            return List.of();
        }

        List<String> foundTasks = new ArrayList<>();
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> tasks = (Map<String, Object>) nodeInfo.get("tasks");
            if (tasks != null) {
                foundTasks.addAll(tasks.keySet());
            }
        }
        return foundTasks;
    }

    /**
     * Returns the list of transform tasks for the given transform as reported by the _cluster/state API.
     */
    @SuppressWarnings("unchecked")
    protected List<String> getTransformTasksFromClusterState(String transformId) throws IOException {
        Request request = new Request("GET", "_cluster/state");
        Map<String, Object> response = entityAsMap(adminClient().performRequest(request));

        List<Map<String, Object>> tasks = (List<Map<String, Object>>) XContentMapValues.extractValue(
            response,
            "metadata",
            "persistent_tasks",
            "tasks"
        );

        return tasks.stream().map(t -> (String) t.get("id")).filter(transformId::equals).toList();
    }

    protected Response getNodeStats() throws IOException {
        return adminClient().performRequest(new Request("GET", "/_transform/_node_stats"));
    }

    protected int getTotalRegisteredTransformCount() throws IOException {
        Response response = getNodeStats();
        return (int) XContentMapValues.extractValue(entityAsMap(response), "total", "scheduler", "registered_transform_count");
    }

    @SuppressWarnings("unchecked")
    protected void logAudits() throws Exception {
        logger.info("writing audit messages to the log");
        Request searchRequest = new Request("GET", TransformInternalIndexConstants.AUDIT_INDEX + "/_search?ignore_unavailable=true");
        searchRequest.setJsonEntity("""
            {
              "size": 100,
              "sort": [ { "timestamp": { "order": "asc" } } ]
            }""");

        assertBusy(() -> {
            try {
                refreshIndex(TransformInternalIndexConstants.AUDIT_INDEX_PATTERN);
                Response searchResponse = client().performRequest(searchRequest);

                Map<String, Object> searchResult = entityAsMap(searchResponse);
                List<Map<String, Object>> searchHits = (List<Map<String, Object>>) XContentMapValues.extractValue(
                    "hits.hits",
                    searchResult
                );

                for (Map<String, Object> hit : searchHits) {
                    Map<String, Object> source = (Map<String, Object>) XContentMapValues.extractValue("_source", hit);
                    String level = (String) source.getOrDefault("level", "info");
                    logger.log(
                        Level.getLevel(level.toUpperCase(Locale.ROOT)),
                        "Transform audit: [{}] [{}] [{}] [{}]",
                        Instant.ofEpochMilli((long) source.getOrDefault("timestamp", 0)),
                        source.getOrDefault("transform_id", "n/a"),
                        source.getOrDefault("message", "n/a"),
                        source.getOrDefault("node_name", "n/a")
                    );
                }
            } catch (ResponseException e) {
                // see gh#54810, wrap temporary 503's as assertion error for retry
                if (e.getResponse().getStatusLine().getStatusCode() != 503) {
                    throw e;
                }
                throw new AssertionError("Failed to retrieve audit logs", e);
            }
        }, 5, TimeUnit.SECONDS);
    }

    protected void refreshIndex(String index) throws IOException {
        Request refreshRequest = new Request("POST", index + "/_refresh");
        assertOKAndConsume(adminClient().performRequest(refreshRequest));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Downsampling through the system index in a multi-project cluster: each project has its own state index, and operations of
 * different projects, even on indices with the same names, are independent.
 */
public class DownsampleMultiProjectIT extends ESRestTestCase {

    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String STATE_INDEX = ".downsample-tasks";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .module("x-pack-downsample")
        .module("x-pack-analytics")
        .module("test-multi-project")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.downsample.store.poll_interval", "1s")
        .setting("xpack.downsample.store.reconcile_interval", "100ms")
        .build();

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @After
    public void cleanup() throws Exception {
        if (preserveClusterUponCompletion() == false) {
            cleanUpProjects();
        }
    }

    /**
     * Two projects downsample indices with the same names, with different data, at the same time: each target holds exactly the
     * buckets of its own project's source, and each project has its own state index (a project without any downsample has none).
     */
    public void testProjectsDownsampleIndependently() throws Exception {
        String first = randomUniqueProjectId().id();
        String second = randomValueOtherThan(first, () -> randomUniqueProjectId().id());
        String untouched = randomValueOtherThanMany(p -> p.equals(first) || p.equals(second), () -> randomUniqueProjectId().id());
        createProject(first);
        createProject(second);
        createProject(untouched);
        // different numbers of hours, so the targets cannot be confused
        int firstHours = randomIntBetween(1, 5);
        int secondHours = randomValueOtherThan(firstHours, () -> randomIntBetween(1, 5));
        int dimensions = randomIntBetween(1, 3);
        createSource(first, firstHours, dimensions);
        createSource(second, secondHours, dimensions);

        CompletableFuture<Response> firstDownsample = downsampleAsync(first);
        CompletableFuture<Response> secondDownsample = downsampleAsync(second);
        assertOK(firstDownsample.get(2, TimeUnit.MINUTES));
        assertOK(secondDownsample.get(2, TimeUnit.MINUTES));

        assertThat(targetDocCount(first), equalTo(firstHours * dimensions));
        assertThat(targetDocCount(second), equalTo(secondHours * dimensions));
        Map<String, List<String>> indices = indicesPerProject();
        assertThat(indices.get(first), hasItem(STATE_INDEX));
        assertThat(indices.get(second), hasItem(STATE_INDEX));
        assertThat(indices.get(untouched), not(hasItem(STATE_INDEX)));
    }

    /**
     * A project is deleted while one of its downsample operations is in progress: its state goes with it, and the operations of
     * other projects are not affected.
     */
    public void testDeletingAProjectDoesNotAffectOthers() throws Exception {
        String deleted = randomUniqueProjectId().id();
        String other = randomValueOtherThan(deleted, () -> randomUniqueProjectId().id());
        createProject(deleted);
        createProject(other);
        createSource(deleted, 3, 2);
        createSource(other, 3, 2);

        CompletableFuture<Response> deletedDownsample = downsampleAsync(deleted);
        assertOK(adminClient().performRequest(new Request("DELETE", "/_project/" + deleted)));
        // the request of the deleted project ends, one way or the other, with its project
        try {
            deletedDownsample.get(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.info("downsample of the deleted project ended with [{}]", e.toString());
        }

        assertOK(downsampleAsync(other).get(2, TimeUnit.MINUTES));
        assertThat(targetDocCount(other), equalTo(3 * 2));
        assertThat(indicesPerProject().keySet(), not(hasItem(deleted)));
    }

    // ---- helpers ----

    private static void setRequestProjectId(Request request, String projectId) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        request.setOptions(options);
    }

    /**
     * A time series index in the project with one document per (dimension, hour), so the target has the same number of documents.
     */
    private void createSource(String project, int hours, int dimensions) throws IOException {
        Request create = new Request("PUT", "/" + SOURCE);
        setRequestProjectId(create, project);
        create.setJsonEntity("""
            {
              "settings": {
                "index": {
                  "mode": "time_series",
                  "routing_path": ["dim"],
                  "time_series": { "start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-02T00:00:00Z" },
                  "number_of_replicas": 0
                }
              },
              "mappings": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "dim": { "type": "keyword", "time_series_dimension": true },
                  "value": { "type": "double", "time_series_metric": "gauge" }
                }
              }
            }""");
        assertOK(client().performRequest(create));

        StringBuilder bulk = new StringBuilder();
        for (int hour = 0; hour < hours; hour++) {
            for (int dimension = 0; dimension < dimensions; dimension++) {
                bulk.append("{\"index\":{}}\n");
                bulk.append(
                    Strings.format("{\"@timestamp\":\"2024-01-01T%02d:30:00Z\",\"dim\":\"d%d\",\"value\":%d}%n", hour, dimension, hour)
                );
            }
        }
        Request index = new Request("POST", "/" + SOURCE + "/_bulk");
        setRequestProjectId(index, project);
        index.addParameter("refresh", "true");
        index.setJsonEntity(bulk.toString());
        Map<String, Object> indexed = entityAsMap(assertOK(client().performRequest(index)));
        assertThat("bulk has no errors", indexed.get("errors"), equalTo(false));

        Request block = new Request("PUT", "/" + SOURCE + "/_block/write");
        setRequestProjectId(block, project);
        assertOK(client().performRequest(block));
    }

    private CompletableFuture<Response> downsampleAsync(String project) {
        Request request = new Request("POST", "/" + SOURCE + "/_downsample/" + TARGET);
        setRequestProjectId(request, project);
        request.setJsonEntity("""
            { "fixed_interval": "1h" }""");
        CompletableFuture<Response> future = new CompletableFuture<>();
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                future.complete(response);
            }

            @Override
            public void onFailure(Exception exception) {
                future.completeExceptionally(exception);
            }
        });
        return future;
    }

    private int targetDocCount(String project) throws IOException {
        Request count = new Request("GET", "/" + TARGET + "/_count");
        setRequestProjectId(count, project);
        return ObjectPath.<Integer>evaluate(entityAsMap(assertOK(client().performRequest(count))), "count");
    }

    /**
     * The indices of every project, from the routing table: the state index is a system index, not readable through REST.
     */
    private Map<String, List<String>> indicesPerProject() throws IOException {
        ObjectPath state = ObjectPath.createFromResponse(
            adminClient().performRequest(new Request("GET", "/_cluster/state/routing_table?multi_project=true"))
        );
        List<Map<String, Object>> projects = state.evaluate("routing_table.projects");
        Map<String, List<String>> indices = new HashMap<>();
        for (Map<String, Object> project : projects) {
            @SuppressWarnings("unchecked")
            Map<String, Object> projectIndices = (Map<String, Object>) project.getOrDefault("indices", Map.of());
            indices.put((String) project.get("id"), List.copyOf(projectIndices.keySet()));
        }
        return indices;
    }
}

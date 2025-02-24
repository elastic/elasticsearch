/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class MultiProjectRestTestCase extends ESRestTestCase {
    protected static Request setRequestProjectId(Request request, String projectId) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        request.setOptions(options);
        return request;
    }

    protected static void clearRequestProjectId(Request request) {
        RequestOptions options = request.getOptions();
        if (options.containsHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER)) {
            request.setOptions(options.toBuilder().removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER).build());
        }
    }

    protected void createProject(String projectId) throws IOException {
        Request request = new Request("PUT", "/_project/" + projectId);
        try {
            Response response = adminClient().performRequest(request);
            assertOK(response);
            logger.info("Created project {} : {}", projectId, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("Failed to create project: {}", projectId);
            throw e;
        }
    }

    protected void deleteProject(String projectId) throws IOException {
        final Request request = new Request("DELETE", "/_project/" + projectId);
        try {
            final Response response = adminClient().performRequest(request);
            logger.info("Deleted project {} : {}", projectId, response.getStatusLine());
        } catch (ResponseException e) {
            logger.error("Failed to delete project: {}", projectId);
            throw e;
        }
    }

    protected Set<String> listProjects() throws IOException {
        final Request request = new Request("GET", "/_cluster/state/metadata?multi_project");
        final Response response = adminClient().performRequest(request);
        final List<Map<String, ?>> projects = ObjectPath.eval("metadata.projects", entityAsMap(response));
        return projects.stream().map(m -> String.valueOf(m.get("id"))).collect(Collectors.toSet());
    }

    @After
    public void removeNonDefaultProjects() throws IOException {
        if (preserveClusterUponCompletion() == false) {
            final Set<String> projects = listProjects();
            logger.info("Removing non-default projects from {}", projects);
            for (String projectId : projects) {
                if (projectId.equals(Metadata.DEFAULT_PROJECT_ID.id()) == false) {
                    deleteProject(projectId);
                }
            }
        }
    }

}

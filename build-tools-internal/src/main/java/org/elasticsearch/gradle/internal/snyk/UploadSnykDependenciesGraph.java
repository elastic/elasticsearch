/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.net.HttpURLConnection;

import javax.inject.Inject;

public class UploadSnykDependenciesGraph extends DefaultTask {

    public static final String DEFAULT_SERVER = "https://snyk.io";
    public static final String GRADLE_GRAPH_ENDPOINT = "/api/v1/monitor/gradle/graph";

    // This is new `experimental` api endpoint we might want to support in the future. For now it
    // does not allow grouping projects and adding custom metadata to the graph data. Therefore
    // we do not support this yet but keep it here for documentation purposes
    public static final String SNYK_DEP_GRAPH_API_ENDPOINT = "/api/v1/monitor/dep-graph";

    private final RegularFileProperty inputFile;
    private final Property<String> token;
    private final Property<String> url;
    private final Property<String> projectId;

    @Inject
    public UploadSnykDependenciesGraph(ObjectFactory objectFactory) {
        url = objectFactory.property(String.class).convention(DEFAULT_SERVER + GRADLE_GRAPH_ENDPOINT);
        projectId = objectFactory.property(String.class);
        token = objectFactory.property(String.class);
        inputFile = objectFactory.fileProperty();
    }

    @TaskAction
    void upload() {
        String endpoint = calculateEffectiveEndpoint();
        CloseableHttpResponse response;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPut putRequest = new HttpPut(endpoint);
            putRequest.addHeader("Authorization", "token " + token.get());
            putRequest.addHeader("Content-Type", "application/json");
            putRequest.setEntity(new FileEntity(inputFile.getAsFile().get()));
            response = client.execute(putRequest);
            int statusCode = response.getStatusLine().getStatusCode();
            String responseString = EntityUtils.toString(response.getEntity());
            getLogger().info("Snyk API call response status: " + statusCode);
            if (statusCode != HttpURLConnection.HTTP_CREATED) {
                throw new GradleException("Uploading Snyk Graph failed with http code " + statusCode + ": " + responseString);
            }
            getLogger().info(responseString);
        } catch (IOException e) {
            throw new GradleException("Failed to call API endpoint to submit updated dependency graph", e);
        }
    }

    private String calculateEffectiveEndpoint() {
        String url = this.url.get();
        return url.endsWith(GRADLE_GRAPH_ENDPOINT) ? url : projectId.map(id -> url + "?org=" + id).getOrElse(url);
    }

    @Input
    public Property<String> getToken() {
        return token;
    }

    @Input
    public Property<String> getUrl() {
        return url;
    }

    @Input
    @Optional
    public Property<String> getProjectId() {
        return projectId;
    }

    @InputFile
    public RegularFileProperty getInputFile() {
        return inputFile;
    }
}

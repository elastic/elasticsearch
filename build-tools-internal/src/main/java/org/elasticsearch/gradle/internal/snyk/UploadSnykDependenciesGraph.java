/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.snyk;

import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.FileEntity;
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
    private final Property<String> snykOrganisation;

    @Inject
    public UploadSnykDependenciesGraph(ObjectFactory objectFactory) {
        url = objectFactory.property(String.class).convention(DEFAULT_SERVER + GRADLE_GRAPH_ENDPOINT);
        snykOrganisation = objectFactory.property(String.class);
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
            putRequest.setEntity(new FileEntity(inputFile.getAsFile().get(), ContentType.APPLICATION_JSON));
            response = client.execute(putRequest);
            int statusCode = response.getCode();
            String responseString = EntityUtils.toString(response.getEntity());
            getLogger().info("Snyk API call response status: " + statusCode);
            if (statusCode != HttpURLConnection.HTTP_CREATED) {
                throw new GradleException("Uploading Snyk Graph failed with http code " + statusCode + ": " + responseString);
            }
            getLogger().info(responseString);
        } catch (IOException | ParseException e) {
            throw new GradleException("Failed to call API endpoint to submit updated dependency graph", e);
        }
    }

    private String calculateEffectiveEndpoint() {
        String url = this.url.get();
        return snykOrganisation.map(id -> url + "?org=" + id).getOrElse(url);
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
    public Property<String> getSnykOrganisation() {
        return snykOrganisation;
    }

    @InputFile
    public RegularFileProperty getInputFile() {
        return inputFile;
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cloud.gce;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link GCEFixture} is a fixture that emulates an GCE service.
 */
public class GCEFixture extends AbstractHttpFixture {

    public static final String PROJECT_ID = "discovery-gce-test";
    public static final String ZONE = "test-zone";

    private final PathTrie<RequestHandler> handlers;

    private final Path nodes;

    private GCEFixture(final String workingDir, final String nodesUriPath) {
        super(workingDir);
        this.nodes = toPath(Objects.requireNonNull(nodesUriPath));
        this.handlers = defaultHandlers();
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("GCEFixture <working directory> <nodes transport uri file>");
        }

        final GCEFixture fixture = new GCEFixture(args[0], args[1]);
        fixture.listen();
    }

    private static String nonAuthPath(Request request) {
        return nonAuthPath(request.getMethod(), request.getPath());
    }

    private static String nonAuthPath(String method, String path) {
        return "NONAUTH " + method + " " + path;
    }

    private static String authPath(Request request) {
        return authPath(request.getMethod(), request.getPath());
    }

    private static String authPath(String method, String path) {
        return "AUTH " + method + " " + path;
    }

    /** Builds the default request handlers **/
    private PathTrie<RequestHandler> defaultHandlers() {
        final PathTrie<RequestHandler> handlers = new PathTrie<>(RestUtils.REST_DECODER);

        final Consumer<Map<String, String>> commonHeaderConsumer = headers -> headers.put("Metadata-Flavor", "Google");

        final Function<String, Response> simpleValue = value -> {
            final Map<String, String> headers = new HashMap<>(TEXT_PLAIN_CONTENT_TYPE);
            commonHeaderConsumer.accept(headers);

            final byte[] responseAsBytes = value.getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.OK.getStatus(), headers, responseAsBytes);
        };

        final Function<String, Response> jsonValue = value -> {
            final Map<String, String> headers = new HashMap<>(JSON_CONTENT_TYPE);
            commonHeaderConsumer.accept(headers);

            final byte[] responseAsBytes = value.getBytes(StandardCharsets.UTF_8);
            return new Response(RestStatus.OK.getStatus(), headers, responseAsBytes);
        };

        // https://cloud.google.com/compute/docs/storing-retrieving-metadata
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/project/project-id"),
            request -> simpleValue.apply(PROJECT_ID));
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/project/attributes/google-compute-default-zone"),
            request -> simpleValue.apply(ZONE));
        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/instance/service-accounts/default/token"),
            request -> jsonValue.apply("{" +
                "\"access_token\":\"1/fFAGRNJru1FTz70BzhT3Zg\", " +
                "\"expires_in\":3268, " +
                "\"token_type\":\"Bearer\"}"));

        handlers.insert(nonAuthPath(HttpGet.METHOD_NAME, "/compute/v1/projects/{project}/zones/{zone}/instances"),
            request -> {
                final Map<String, String> headers = new HashMap<>(JSON_CONTENT_TYPE);
                commonHeaderConsumer.accept(headers);

                final StringBuilder builder = new StringBuilder();
                builder.append("{\"id\": \"test-instances\",\"items\":[");
                int count = 0;
                for (String address : Files.readAllLines(nodes)) {
                    if (count > 0) {
                        builder.append(",");
                    }
                    count++;
                    builder.append("{\"description\": \"ES Node ").append(count).append("\",")
                        .append("\"id\": \"").append(9309873766405L + count).append("\",\n")
                        .append("\"kind\": \"compute#instance\",\n")
                        .append("\"machineType\": \"n1-standard-1\",\n")
                        .append("\"name\": \"test").append(count).append("\",\n")
                        .append("\"networkInterfaces\": [{\n")
                        .append("  \"accessConfigs\": [],\n")
                        .append("  \"name\": \"nic0\",\n")
                        .append("  \"network\": \"default\",\n")
                        .append("  \"networkIP\": \"").append(address).append("\"\n")
                        .append("  }\n")
                        .append("],\n")
                        .append("\"status\": \"RUNNING\",\"zone\": \"").append(ZONE).append("\"")
                        .append("}");
                }
                builder.append("]}");

                final byte[] responseAsBytes = builder.toString().getBytes(StandardCharsets.UTF_8);
                return new Response(RestStatus.OK.getStatus(), headers, responseAsBytes);
        });
        return handlers;
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        //System.out.println(request.getMethod() + " " + request.getPath() + " " +request.getParameters());
        final String nonAuthorizedPath = nonAuthPath(request);
        final RequestHandler nonAuthorizedHandler = handlers.retrieve(nonAuthorizedPath, request.getParameters());
        if (nonAuthorizedHandler != null) {
            return nonAuthorizedHandler.handle(request);
        }
        return null;
    }

    @SuppressForbidden(reason = "Paths#get is fine - we don't have environment here")
    private static Path toPath(final String dir) {
        return Paths.get(dir);
    }
}

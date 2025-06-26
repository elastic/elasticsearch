/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.discovery.gce;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.path.PathTrie;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.fixture.AbstractHttpFixture;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/**
 * {@link GCEFixture} is a fixture that emulates a GCE service.
 */
public class GCEFixture extends AbstractHttpFixture {

    public static final String PROJECT_ID = "discovery-gce-test";
    public static final String ZONE = "test-zone";
    public static final String TOKEN = "1/fFAGRNJru1FTz70BzhT3Zg";
    public static final String TOKEN_TYPE = "Bearer";
    private final Supplier<Path> nodes;
    private final PathTrie<RequestHandler> handlers;

    public GCEFixture(Supplier<Path> nodesUriPath) {
        this.handlers = defaultHandlers();
        this.nodes = nodesUriPath;
    }

    @Override
    protected void before() throws Throwable {
        InetSocketAddress inetSocketAddress = resolveAddress("0.0.0.0", 0);
        listen(inetSocketAddress, false);
    }

    @Override
    protected void after() {
        stop();
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
        handlers.insert(
            nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/project/project-id"),
            request -> simpleValue.apply(PROJECT_ID)
        );
        handlers.insert(
            nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/project/attributes/google-compute-default-zone"),
            request -> simpleValue.apply(ZONE)
        );
        // https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances
        handlers.insert(
            nonAuthPath(HttpGet.METHOD_NAME, "/computeMetadata/v1/instance/service-accounts/default/token"),
            request -> jsonValue.apply(
                Strings.toString(
                    jsonBuilder().startObject()
                        .field("access_token", TOKEN)
                        .field("expires_in", TimeUnit.HOURS.toSeconds(1))
                        .field("token_type", TOKEN_TYPE)
                        .endObject()
                )
            )
        );

        // https://cloud.google.com/compute/docs/reference/rest/v1/instances
        handlers.insert(authPath(HttpGet.METHOD_NAME, "/compute/v1/projects/{project}/zones/{zone}/instances"), request -> {
            final var items = new ArrayList<Map<String, Object>>();
            int count = 0;
            if (Files.exists(nodes.get())) {
                for (String address : Files.readAllLines(nodes.get())) {
                    count++;
                    items.add(
                        Map.of(
                            "id",
                            Long.toString(9309873766405L + count),
                            "description",
                            "ES node" + count,
                            "name",
                            "test" + count,
                            "kind",
                            "compute#instance",
                            "machineType",
                            "n1-standard-1",
                            "networkInterfaces",
                            List.of(
                                Map.of("accessConfigs", Collections.emptyList(), "name", "nic0", "network", "default", "networkIP", address)
                            ),
                            "status",
                            "RUNNING",
                            "zone",
                            ZONE
                        )
                    );
                }
            }

            final String json = Strings.toString(
                jsonBuilder().startObject().field("id", "test-instances").field("items", items).endObject()
            );

            final byte[] responseAsBytes = json.getBytes(StandardCharsets.UTF_8);
            final Map<String, String> headers = new HashMap<>(JSON_CONTENT_TYPE);
            commonHeaderConsumer.accept(headers);
            return new Response(RestStatus.OK.getStatus(), headers, responseAsBytes);
        });
        return handlers;
    }

    @Override
    protected Response handle(final Request request) throws IOException {
        final String nonAuthorizedPath = nonAuthPath(request);
        final RequestHandler nonAuthorizedHandler = handlers.retrieve(nonAuthorizedPath, request.getParameters());
        if (nonAuthorizedHandler != null) {
            return nonAuthorizedHandler.handle(request);
        }

        final String authorizedPath = authPath(request);
        final RequestHandler authorizedHandler = handlers.retrieve(authorizedPath, request.getParameters());
        if (authorizedHandler != null) {
            final String authorization = request.getHeader("Authorization");
            if ((TOKEN_TYPE + " " + TOKEN).equals(authorization) == false) {
                return newError(RestStatus.UNAUTHORIZED, "Authorization", "Login Required");
            }
            return authorizedHandler.handle(request);
        }

        return null;
    }

    private static Response newError(final RestStatus status, final String code, final String message) throws IOException {
        final String response = Strings.toString(
            jsonBuilder().startObject()
                .field(
                    "error",
                    Map.<String, Object>ofEntries(
                        Map.entry(
                            "errors",
                            List.of(
                                Map.<String, Object>ofEntries(
                                    Map.entry("domain", "global"),
                                    Map.entry("reason", "required"),
                                    Map.entry("message", message),
                                    Map.entry("locationType", "header"),
                                    Map.entry("location", code)
                                )
                            )
                        ),
                        Map.entry("code", status.getStatus()),
                        Map.entry("message", message)
                    )
                )
                .endObject()
        );

        return new Response(status.getStatus(), JSON_CONTENT_TYPE, response.getBytes(UTF_8));
    }

    private static InetSocketAddress resolveAddress(String address, int port) {
        try {
            return new InetSocketAddress(InetAddress.getByName(address), port);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}

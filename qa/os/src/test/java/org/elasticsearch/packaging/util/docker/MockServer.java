/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.packaging.test.PackagingTestCase;
import org.elasticsearch.packaging.util.Shell;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Providers an interface to <a href="https://org.mock-server.com/">Mockserver</a>, where a proxy
 * server is needed for testing in Docker tests.
 * <p>
 * To use the server, link the container under test with the mockserver using the <code>--link</code>
 * CLI option, using the {@link #getContainerId()} option. By aliasing the ID, you will know what
 * hostname to use to connect to the proxy. For example:
 *
 * <pre>"--link " + mockserver.getContainerId() + ":mockserver"</pre>
 *
 * <p>All requests will result in a 404, but those requests are recorded and can be retried with
 * {@link #getInteractions()}. These can can be reset with {@link #clearExpectations()}.
 */
public class MockServer {
    protected final Logger logger = LogManager.getLogger(getClass());

    private static final int CONTAINER_PORT = 1080; // default for image

    private final Shell shell;
    private final HttpClient client;
    private ExecutorService executorService;
    private String containerId;

    /**
     * Create a new mockserver, and execute the supplied {@code runnable}. The mockserver will
     * be cleaned up afterwards.
     * @param runnable the code to run e.g. the test case
     */
    public static void withMockServer(CheckedConsumer<MockServer, Exception> runnable) {
        final MockServer mockServer = new MockServer();
        try {
            mockServer.start();
            runnable.accept(mockServer);
            mockServer.close();
        } catch (Throwable e) {
            mockServer.close();
        }
    }

    private MockServer() {
        this.shell = new Shell();
        this.executorService = Executors.newSingleThreadExecutor();
        this.client = HttpClient.newBuilder().executor(executorService).build();
    }

    private void start() throws Exception {
        final String command = "docker run -t --detach --rm -p " + CONTAINER_PORT + ":" + CONTAINER_PORT + " mockserver/mockserver:latest";
        this.containerId = this.shell.run(command).stdout().trim();

        // It's a Java app, so give it a chance to wake up. I'd add a healthcheck to the above command,
        // but the image doesn't have any CLI utils at all.
        PackagingTestCase.assertBusy(() -> {
            try {
                this.reset();
            } catch (Exception e) {
                // Only assertions are retried.
                throw new AssertionError(e);
            }
        }, 20, TimeUnit.SECONDS);
    }

    public void clearExpectations() throws Exception {
        doRequest("http://localhost:" + CONTAINER_PORT + "/mockserver/clear?type=EXPECTATIONS", "{ \"path\": \"/*\" }");
    }

    public void reset() throws Exception {
        doRequest("http://localhost:" + CONTAINER_PORT + "/mockserver/reset", null);
    }

    /**
     * Returns all interactions with the mockserver since startup, the last call to {@link #reset()} or the
     * last call to {@link #clearExpectations()}. The JSON returned by the mockserver is flattened, so that
     * the period-seperated keys in each map represent the structure of the JSON.
     *
     * @return a list of interactions
     * @throws Exception if anything goes wrong
     */
    public List<Map<String, String>> getInteractions() throws Exception {
        final String url = "http://localhost:" + CONTAINER_PORT + "/mockserver/retrieve?type=REQUEST_RESPONSES";

        final String result = doRequest(url, null);

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(result);

        assertThat("Response from mockserver is not a JSON array", jsonNode.isArray(), is(true));

        final List<Map<String, String>> interactions = new ArrayList<>();

        for (JsonNode node : jsonNode) {
            final Map<String, String> interaction = new HashMap<>();
            addKeys("", node, interaction);
            interactions.add(interaction);
        }

        return interactions;
    }

    private void close() {
        if (this.containerId != null) {
            this.shell.run("docker rm -f " + this.containerId);
            this.containerId = null;
        }

        if (this.executorService != null) {
            this.executorService.shutdown();
            this.executorService = null;
        }
    }

    public String getContainerId() {
        return containerId;
    }

    public int getPort() {
        return CONTAINER_PORT;
    }

    /**
     * Recursively flattens a JsonNode into a map, to make it easier to pick out entries and make assertions.
     * Keys are concatenated with periods.
     *
     * @param currentPath used recursively to construct the key
     * @param jsonNode the current node to flatten
     * @param map entries are added into this map
     */
    private void addKeys(String currentPath, JsonNode jsonNode, Map<String, String> map) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + ".";

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                addKeys(pathPrefix + entry.getKey(), entry.getValue(), map);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                addKeys(currentPath + "[" + i + "]", arrayNode.get(i), map);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            map.put(currentPath, valueNode.asText());
        }
    }

    private String doRequest(String urlString, String body) throws Exception {
        final HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(urlString));

        if (body == null) {
            request.method("PUT", BodyPublishers.noBody());
        } else {
            request.method("PUT", BodyPublishers.ofString(body)).header("Content-Type", "application/json");
        }

        final HttpResponse<String> response = client.send(request.build(), BodyHandlers.ofString());

        return response.body();
    }
}

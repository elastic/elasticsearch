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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MockServer {
    protected final Logger logger = LogManager.getLogger(getClass());

    private static final int CONTAINER_PORT = 1080; // default for image

    private final Shell shell;
    private String containerId;

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
    }

    private void start() throws Exception {
        final String command = "docker run -t --detach --rm -p " + CONTAINER_PORT + ":" + CONTAINER_PORT + " mockserver/mockserver:latest";
        this.containerId = this.shell.run(command).stdout.trim();

        // It's a Java app, so give it a chance to wake up. I'd add a healthcheck to the above command,
        // but the image doesn't have any CLI utils at all.
        PackagingTestCase.assertBusy(this::reset, 20, TimeUnit.SECONDS);

        this.setExpectation();
    }

    public void reset() {
        assertTrue(doRequest("PUT", "http://localhost:" + CONTAINER_PORT + "/mockserver/reset", null).isSuccess());
    }

    public void setExpectation() {
        // https://org.mock-server.com/mock_server/clearing_and_resetting.html

        final String url = "http://localhost:" + CONTAINER_PORT + "/mockserver/expectation";

        final String payload = "{"
            + "  \"httpRequest\": {"
            + "    \"path\": \"/*\""
            + "  },"
            + "  \"httpResponse\": {"
            + "    \"statusCode\": 404"
            + "  }"
            + "}";

        doRequest("PUT", url, payload);
    }

    public List<Map<String, String>> getInteractions() throws Exception {
        final String url = "http://localhost:" + CONTAINER_PORT + "/mockserver/retrieve?type=REQUEST_RESPONSES";

        final Shell.Result result = doRequest("PUT", url, null);
        assertTrue(result.isSuccess());

        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(result.stdout);

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
        shell.run("docker rm -f " + this.containerId);
    }

    public String getContainerId() {
        return containerId;
    }

    public int getPort() {
        return CONTAINER_PORT;
    }

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

    private Shell.Result doRequest(String method, String urlString, String body) {
        final List<String> command = new ArrayList<>();
        command.add("curl");
        command.add("-s");
        command.add("-S");
        command.add("-f");
        command.add("-X");
        command.add(method);

        if (body != null) {
            command.add("-H");
            command.add("'Content-Type: application/json'");
            command.add("--data");
            command.add("'" + body + "'");
        }

        command.add("'" + urlString + "'");

        return this.shell.runIgnoreExitCode(String.join(" ", command));
    }
}

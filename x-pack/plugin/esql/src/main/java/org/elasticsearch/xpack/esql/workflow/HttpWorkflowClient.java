/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * HTTP-based client for executing Kibana workflows synchronously.
 * POC implementation - uses simple HttpURLConnection for synchronous HTTP calls.
 *
 * In production, this would use proper service-to-service authentication
 * and likely a more robust HTTP client (Apache HttpClient, etc.).
 */
public class HttpWorkflowClient implements WorkflowClient {

    private static final Logger logger = LogManager.getLogger(HttpWorkflowClient.class);

    private final String kibanaBaseUrl;
    private final Duration timeout;
    private final Map<String, String> authHeaders;

    /**
     * Create a new HttpWorkflowClient.
     *
     * @param kibanaBaseUrl Base URL for Kibana (e.g., "http://localhost:5601")
     * @param timeout Timeout for HTTP requests
     * @param authHeaders Authentication headers to pass to Kibana (e.g., Authorization)
     */
    public HttpWorkflowClient(String kibanaBaseUrl, Duration timeout, Map<String, String> authHeaders) {
        this.kibanaBaseUrl = kibanaBaseUrl.endsWith("/") ? kibanaBaseUrl.substring(0, kibanaBaseUrl.length() - 1) : kibanaBaseUrl;
        this.timeout = timeout;
        this.authHeaders = authHeaders;
    }

    @Override
    public String executeWorkflowSync(String workflowId, String inputsJson) throws WorkflowExecutionException {
        String url = kibanaBaseUrl + "/api/workflows/execute_sync";

        logger.debug("Executing workflow {} with inputs: {}", workflowId, inputsJson);

        try {
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            try {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json");
                connection.setRequestProperty("kbn-xsrf", "true");
                connection.setConnectTimeout((int) timeout.toMillis());
                connection.setReadTimeout((int) timeout.toMillis());
                connection.setDoOutput(true);

                // Add auth headers
                for (Map.Entry<String, String> header : authHeaders.entrySet()) {
                    connection.setRequestProperty(header.getKey(), header.getValue());
                }

                // Write request body - workflowId goes in body, not URL
                String requestBody = "{\"workflowId\":\"" + workflowId + "\",\"inputs\":" + inputsJson + "}";
                try (OutputStream os = connection.getOutputStream()) {
                    os.write(requestBody.getBytes(StandardCharsets.UTF_8));
                }

                int responseCode = connection.getResponseCode();

                // Read response
                StringBuilder response = new StringBuilder();
                try (
                    BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                            responseCode >= 400 ? connection.getErrorStream() : connection.getInputStream(),
                            StandardCharsets.UTF_8
                        )
                    )
                ) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        response.append(line);
                    }
                }

                if (responseCode >= 400) {
                    throw new WorkflowExecutionException("Workflow execution failed with status " + responseCode + ": " + response);
                }

                // Extract output from response
                // Expected response format: {"output": <workflow_output>}
                String responseStr = response.toString();
                logger.debug("Workflow {} completed with response: {}", workflowId, responseStr);

                return extractOutput(responseStr);

            } finally {
                connection.disconnect();
            }
        } catch (IOException e) {
            throw new WorkflowExecutionException("Failed to execute workflow " + workflowId, e);
        }
    }

    /**
     * Extract the "output" field from the response JSON.
     * This is a simple implementation - production would use proper JSON parsing.
     */
    private String extractOutput(String responseJson) {
        // Simple extraction - look for "output": and extract until the end
        // Production would use proper JSON parsing (Jackson, etc.)
        int outputStart = responseJson.indexOf("\"output\":");
        if (outputStart == -1) {
            // If no output field, return the whole response
            return responseJson;
        }

        outputStart += "\"output\":".length();

        // Skip whitespace
        while (outputStart < responseJson.length() && Character.isWhitespace(responseJson.charAt(outputStart))) {
            outputStart++;
        }

        // Find the value - handle different JSON types
        char firstChar = responseJson.charAt(outputStart);
        if (firstChar == '"') {
            // String value - find matching close quote
            int outputEnd = findStringEnd(responseJson, outputStart + 1);
            return responseJson.substring(outputStart, outputEnd + 1);
        } else if (firstChar == '{') {
            // Object value - find matching close brace
            int outputEnd = findObjectEnd(responseJson, outputStart);
            return responseJson.substring(outputStart, outputEnd + 1);
        } else if (firstChar == '[') {
            // Array value - find matching close bracket
            int outputEnd = findArrayEnd(responseJson, outputStart);
            return responseJson.substring(outputStart, outputEnd + 1);
        } else if (firstChar == 'n') {
            // null
            return "null";
        } else if (firstChar == 't' || firstChar == 'f') {
            // boolean
            return firstChar == 't' ? "true" : "false";
        } else {
            // Number - find end
            int outputEnd = outputStart;
            while (outputEnd < responseJson.length()) {
                char c = responseJson.charAt(outputEnd);
                if (c == ',' || c == '}' || Character.isWhitespace(c)) {
                    break;
                }
                outputEnd++;
            }
            return responseJson.substring(outputStart, outputEnd);
        }
    }

    private int findStringEnd(String json, int start) {
        for (int i = start; i < json.length(); i++) {
            if (json.charAt(i) == '"' && json.charAt(i - 1) != '\\') {
                return i;
            }
        }
        return json.length() - 1;
    }

    private int findObjectEnd(String json, int start) {
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inString = !inString;
            } else if (!inString) {
                if (c == '{') depth++;
                else if (c == '}') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
        }
        return json.length() - 1;
    }

    private int findArrayEnd(String json, int start) {
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inString = !inString;
            } else if (!inString) {
                if (c == '[') depth++;
                else if (c == ']') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
        }
        return json.length() - 1;
    }

    /**
     * Factory method to create a WorkflowClient with default settings.
     */
    public static WorkflowClient create(String kibanaUrl, String authorizationHeader) {
        return new HttpWorkflowClient(
            kibanaUrl,
            Duration.ofSeconds(30),
            authorizationHeader != null ? Map.of("Authorization", authorizationHeader) : Map.of()
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.clickhouse;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Issues bounded, read-only SQL requests without putting credentials in URLs or errors. */
final class ClickHouseClient implements Closeable {
    private static final int MAX_LINE_LENGTH = 1_048_576;
    private final URI endpoint;
    private final String authorization;
    private volatile HttpURLConnection connection;
    private volatile boolean closed;

    ClickHouseClient(Map<String, Object> config) {
        endpoint = ClickHouseConfiguration.endpoint(Objects.toString(config.get("endpoint"), null));
        String username = Objects.toString(config.get("username"), "");
        String password = Objects.toString(config.get("password"), "");
        if (username.isBlank() || password.isEmpty()) {
            throw new IllegalArgumentException("ClickHouse requires a username and password");
        }
        authorization = "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    @SuppressForbidden(
        reason = "This connector reads its configured HTTP endpoint with explicit socket timeouts and outbound_network entitlement"
    )
    Response query(String sql) throws IOException {
        if (closed) {
            throw new IOException("ClickHouse connection is closed");
        }
        URI uri = URI.create(
            endpoint
                + (endpoint.toString().endsWith("/") ? "" : "/")
                + "?readonly=2&max_execution_time=60&max_result_rows=1000000&result_overflow_mode=throw"
                + "&output_format_json_quote_64bit_integers=1&output_format_json_quote_decimals=1"
                + "&output_format_json_quote_denormals=1&http_write_exception_in_output_format=0"
        );
        HttpURLConnection current = (HttpURLConnection) uri.toURL().openConnection();
        connection = current;
        if (closed) {
            current.disconnect();
            throw new IOException("ClickHouse connection is closed");
        }
        current.setInstanceFollowRedirects(false);
        current.setConnectTimeout(10_000);
        current.setReadTimeout(30_000);
        current.setRequestMethod("POST");
        current.setRequestProperty("Authorization", authorization);
        current.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        current.setDoOutput(true);
        byte[] body = (sql + " FORMAT JSONCompactEachRowWithNamesAndTypes").getBytes(StandardCharsets.UTF_8);
        current.setFixedLengthStreamingMode(body.length);
        try {
            try (var output = current.getOutputStream()) {
                output.write(body);
            }
            int status = current.getResponseCode();
            if (status != 200) {
                throw new IOException("ClickHouse query failed with HTTP status [" + status + "]");
            }
            return new Response(current, new BufferedReader(new InputStreamReader(current.getInputStream(), StandardCharsets.UTF_8)));
        } catch (IOException | RuntimeException e) {
            current.disconnect();
            throw e;
        }
    }

    @Override
    public void close() {
        closed = true;
        HttpURLConnection current = connection;
        if (current != null) {
            current.disconnect();
        }
    }

    /** Parses one bounded JSON row, including ClickHouse's two schema header rows. */
    record Response(HttpURLConnection connection, BufferedReader reader) implements Closeable {
        List<Object> readRow() throws IOException {
            StringBuilder line = new StringBuilder();
            int next;
            while ((next = reader.read()) != -1 && next != '\n') {
                if (line.length() >= MAX_LINE_LENGTH) {
                    throw new IOException("ClickHouse result row exceeds the PoC limit of 1 MiB");
                }
                line.append((char) next);
            }
            if (next == -1 && line.isEmpty()) {
                return null;
            }
            // ClickHouse can append a plain-text error after sending HTTP 200.
            if (line.isEmpty() || line.charAt(0) != '[') {
                throw new IOException("ClickHouse returned an error or an invalid result row");
            }
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line.toString())) {
                List<Object> values = parser.list();
                if (parser.nextToken() != null) {
                    throw new IOException("ClickHouse returned trailing data after a result row");
                }
                return values;
            } catch (RuntimeException e) {
                throw new IOException("ClickHouse returned an invalid JSON result row");
            }
        }

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                connection.disconnect();
            }
        }
    }
}

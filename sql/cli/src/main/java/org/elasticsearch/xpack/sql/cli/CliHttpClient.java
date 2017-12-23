/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.sql.client.shared.ClientException;
import org.elasticsearch.xpack.sql.client.shared.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.plugin.SqlAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.plugin.SqlRequest;
import org.joda.time.DateTimeZone;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.function.Function;

public class CliHttpClient {
    private static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    private final ConnectionConfiguration cfg;

    public CliHttpClient(ConnectionConfiguration cfg) {
        this.cfg = cfg;
    }

    private NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;

    public MainResponse serverInfo() throws SQLException {
        return get("/", MainResponse::fromXContent);
    }

    public PlainResponse queryInit(String query, int fetchSize) throws SQLException {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        SqlRequest sqlRequest = new SqlRequest(query, null, DateTimeZone.UTC, fetchSize, TimeValue.timeValueMillis(cfg.queryTimeout()),
                TimeValue.timeValueMillis(cfg.pageTimeout()), "");
        return postPlain(SqlAction.REST_ENDPOINT, sqlRequest);
    }

    public PlainResponse nextPage(String cursor) throws SQLException {
        SqlRequest sqlRequest = new SqlRequest();
        sqlRequest.cursor(cursor);
        return postPlain(SqlAction.REST_ENDPOINT, sqlRequest);
    }

    public boolean queryClose(String cursor) throws SQLException {
        SqlClearCursorResponse response = post(SqlClearCursorAction.REST_ENDPOINT,
                new SqlClearCursorRequest(cursor),
                SqlClearCursorResponse::fromXContent);
        return response.isSucceeded();
    }

    private <Request extends ToXContent, Response> Response post(String path, Request request,
                                                                 CheckedFunction<XContentParser, Response, IOException> responseParser)
            throws SQLException {
        return JreHttpUrlConnection.http(path, "error_trace", cfg, con ->
                con.request(
                        outputStream -> writeTo(request, outputStream),
                        (in, headers) -> readFrom(in, headers, responseParser),
                        "POST"
                )
        ).getResponseOrThrowException();
    }

    private <Response> Response get(String path, CheckedFunction<XContentParser, Response, IOException> responseParser)
            throws SQLException {
        return JreHttpUrlConnection.http(path, "error_trace", cfg, con ->
                con.request(
                        null,
                        (in, headers) -> readFrom(in, headers, responseParser),
                        "GET"
                )
        ).getResponseOrThrowException();
    }

    private <Request extends ToXContent> PlainResponse postPlain(String path, Request request) throws SQLException {
        return JreHttpUrlConnection.http(path, "error_trace", cfg, con ->
                con.request(
                        outputStream -> {
                            writeTo(request, outputStream);
                        },
                        (in, headers) -> {
                            String cursor = headers.apply("Cursor");
                            long tookNanos;
                            try {
                                tookNanos = Long.parseLong(headers.apply("Took-nanos"));
                            } catch (NumberFormatException ex) {
                                throw new ClientException("Cannot parse Took-nanos header [" + headers.apply("Took-nanos") + "]");
                            }
                            return new PlainResponse(tookNanos, cursor == null ? "" : cursor, readData(in));
                        },
                        "POST"
                )
        ).getResponseOrThrowException();
    }

    private static <Request extends ToXContent> void writeTo(Request xContent, OutputStream outputStream) {
        try {
            BytesRef source = XContentHelper.toXContent(xContent, REQUEST_BODY_CONTENT_TYPE, false).toBytesRef();
            outputStream.write(source.bytes, source.offset, source.length);
        } catch (IOException ex) {
            throw new ClientException("Cannot serialize request", ex);
        }

    }

    private <Response> Response readFrom(InputStream inputStream, Function<String, String> headers,
                                         CheckedFunction<XContentParser, Response, IOException> responseParser) {
        String contentType = headers.apply("Content-Type");
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + contentType);
        }
        try (XContentParser parser = xContentType.xContent().createParser(registry, inputStream)) {
            return responseParser.apply(parser);
        } catch (IOException ex) {
            throw new ClientException("Cannot parse response", ex);
        }
    }

    private static String readData(InputStream in) throws IOException {
        return Streams.copyToString(new InputStreamReader(new BufferedInputStream(in), StandardCharsets.UTF_8));
    }

}

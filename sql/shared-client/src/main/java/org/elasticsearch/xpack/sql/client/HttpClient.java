/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.sql.client.shared.CheckedFunction;
import org.elasticsearch.xpack.sql.client.shared.ClientException;
import org.elasticsearch.xpack.sql.client.shared.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection.ResponseOrException;
import org.elasticsearch.xpack.sql.plugin.SqlAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.plugin.SqlRequest;
import org.elasticsearch.xpack.sql.plugin.SqlResponse;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.function.Function;

/**
 * A specialized high-level REST client with support for SQL-related functions
 */
public class HttpClient {

    private static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    private final ConnectionConfiguration cfg;

    public HttpClient(ConnectionConfiguration cfg) throws SQLException {
        this.cfg = cfg;
    }

    private NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;

    public MainResponse serverInfo() throws SQLException {
        return get("/", MainResponse::fromXContent);
    }

    public SqlResponse queryInit(String query, int fetchSize) throws SQLException {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        SqlRequest sqlRequest = new SqlRequest(query, null, DateTimeZone.UTC, fetchSize, TimeValue.timeValueMillis(cfg.queryTimeout()),
                TimeValue.timeValueMillis(cfg.pageTimeout()), "");
        return post(SqlAction.REST_ENDPOINT, sqlRequest, SqlResponse::fromXContent);
    }

    public SqlResponse nextPage(String cursor) throws SQLException {
        SqlRequest sqlRequest = new SqlRequest();
        sqlRequest.cursor(cursor);
        return post(SqlAction.REST_ENDPOINT, sqlRequest, SqlResponse::fromXContent);
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
        BytesReference requestBytes = toXContent(request);
        Tuple<XContentType, BytesReference> response =
                AccessController.doPrivileged((PrivilegedAction<ResponseOrException<Tuple<XContentType, BytesReference>>>) () ->
                JreHttpUrlConnection.http(path, "error_trace", cfg, con ->
                        con.request(
                                requestBytes::writeTo,
                                this::readFrom,
                                "POST"
                        )
                )).getResponseOrThrowException();
        return fromXContent(response.v1(), response.v2(), responseParser);
    }

    private <Response> Response get(String path, CheckedFunction<XContentParser, Response, IOException> responseParser)
            throws SQLException {
        Tuple<XContentType, BytesReference> response =
                AccessController.doPrivileged((PrivilegedAction<ResponseOrException<Tuple<XContentType, BytesReference>>>) () ->
                JreHttpUrlConnection.http(path, "error_trace", cfg, con ->
                        con.request(
                                null,
                                this::readFrom,
                                "GET"
                        )
                )).getResponseOrThrowException();
        return fromXContent(response.v1(), response.v2(), responseParser);
    }

    private static <Request extends ToXContent> BytesReference toXContent(Request xContent) {
        try {
            return XContentHelper.toXContent(xContent, REQUEST_BODY_CONTENT_TYPE, false);
        } catch (IOException ex) {
            throw new ClientException("Cannot serialize request", ex);
        }
    }

    private Tuple<XContentType, BytesReference> readFrom(InputStream inputStream, Function<String, String> headers) {
        String contentType = headers.apply("Content-Type");
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + contentType);
        }
        BytesStreamOutput out = new BytesStreamOutput();
        try {
            Streams.copy(inputStream, out);
        } catch (IOException ex) {
            throw new ClientException("Cannot deserialize response", ex);
        }
        return new Tuple<>(xContentType, out.bytes());

    }

    private <Response> Response fromXContent(XContentType xContentType, BytesReference bytesReference,
                                             CheckedFunction<XContentParser, Response, IOException> responseParser) {
        try (XContentParser parser = xContentType.xContent().createParser(registry, bytesReference)) {
            return responseParser.apply(parser);
        } catch (IOException ex) {
            throw new ClientException("Cannot parse response", ex);
        }
    }

}

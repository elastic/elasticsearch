/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.xpack.sql.client.shared.CheckedFunction;
import org.elasticsearch.xpack.sql.client.shared.ClientException;
import org.elasticsearch.xpack.sql.client.shared.ConnectionConfiguration;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection.ResponseOrException;
import org.elasticsearch.xpack.sql.proto.AbstractSqlRequest;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;

import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Collections;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * A specialized high-level REST client with support for SQL-related functions.
 * Similar to JDBC and the underlying HTTP connection, this class is not thread-safe
 * and follows a request-response flow.
 */
public class HttpClient {

    private static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;

    private final ConnectionConfiguration cfg;

    public HttpClient(ConnectionConfiguration cfg) {
        this.cfg = cfg;
    }

    private NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;

    public boolean ping(long timeoutInMs) throws SQLException {
        return head("/", timeoutInMs);
    }

    public MainResponse serverInfo() throws SQLException {
        return get("/", MainResponse::fromXContent);
    }

    public SqlQueryResponse queryInit(String query, int fetchSize) throws SQLException {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.PLAIN, query, Collections.emptyList(), null,
                TimeZone.getTimeZone("UTC"), fetchSize, TimeValue.timeValueMillis(cfg.queryTimeout()),
                TimeValue.timeValueMillis(cfg.pageTimeout()));
        return query(sqlRequest);
    }

    public SqlQueryResponse query(SqlQueryRequest sqlRequest) throws SQLException {
        return post(Protocol.SQL_QUERY_REST_ENDPOINT, sqlRequest, SqlQueryResponse::fromXContent);
    }

    public SqlQueryResponse nextPage(String cursor) throws SQLException {
        SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.PLAIN, cursor, TimeValue.timeValueMillis(cfg.queryTimeout()),
            TimeValue.timeValueMillis(cfg.pageTimeout()));
        return post(Protocol.SQL_QUERY_REST_ENDPOINT, sqlRequest, SqlQueryResponse::fromXContent);
    }

    public boolean queryClose(String cursor) throws SQLException {
        SqlClearCursorResponse response = post(Protocol.CLEAR_CURSOR_REST_ENDPOINT,
                new SqlClearCursorRequest(Mode.PLAIN, cursor),
                SqlClearCursorResponse::fromXContent);
        return response.isSucceeded();
    }

    private <Request extends AbstractSqlRequest, Response> Response post(String path, Request request,
            CheckedFunction<XContentParser, Response, IOException> responseParser)
            throws SQLException {
        BytesReference requestBytes = toXContent(request);
        String query = "error_trace&mode=" + request.mode();
        Tuple<XContentType, BytesReference> response =
                AccessController.doPrivileged((PrivilegedAction<ResponseOrException<Tuple<XContentType, BytesReference>>>) () ->
                JreHttpUrlConnection.http(path, query, cfg, con ->
                        con.request(
                                requestBytes::writeTo,
                                this::readFrom,
                                "POST"
                        )
                )).getResponseOrThrowException();
        return fromXContent(response.v1(), response.v2(), responseParser);
    }

    private boolean head(String path, long timeoutInMs) throws SQLException {
        ConnectionConfiguration pingCfg = new ConnectionConfiguration(cfg.baseUri(), cfg.connectionString(),
                cfg.connectTimeout(), timeoutInMs, cfg.queryTimeout(), cfg.pageTimeout(), cfg.pageSize(),
                cfg.authUser(), cfg.authPass(), cfg.sslConfig(), cfg.proxyConfig());
        try {
            return AccessController.doPrivileged((PrivilegedAction<Boolean>) () ->
                    JreHttpUrlConnection.http(path, "error_trace", pingCfg, JreHttpUrlConnection::head));
        } catch (ClientException ex) {
            throw new SQLException("Cannot ping server", ex);
        }
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
        try (InputStream stream = bytesReference.streamInput();
             XContentParser parser = xContentType.xContent().createParser(registry,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
            return responseParser.apply(parser);
        } catch (IOException ex) {
            throw new ClientException("Cannot parse response", ex);
        }
    }
}

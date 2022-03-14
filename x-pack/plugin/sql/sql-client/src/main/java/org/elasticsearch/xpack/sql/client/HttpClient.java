/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xpack.sql.client.JreHttpUrlConnection.ResponseOrException;
import org.elasticsearch.xpack.sql.proto.AbstractSqlRequest;
import org.elasticsearch.xpack.sql.proto.CoreProtocol;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Payloads;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.proto.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType;
import org.elasticsearch.xpack.sql.proto.core.CheckedFunction;
import org.elasticsearch.xpack.sql.proto.core.Streams;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;
import org.elasticsearch.xpack.sql.proto.core.Tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

/**
 * A specialized high-level REST client with support for SQL-related functions.
 * Similar to JDBC and the underlying HTTP connection, this class is not thread-safe
 * and follows a request-response flow.
 */
public class HttpClient {

    public static class ResponseWithWarnings<R> {
        private final R response;
        private final List<String> warnings;

        ResponseWithWarnings(R response, List<String> warnings) {
            this.response = response;
            this.warnings = warnings;
        }

        public R response() {
            return response;
        }

        public List<String> warnings() {
            return warnings;
        }
    }

    private final ConnectionConfiguration cfg;
    private final ContentType requestBodyContentType;

    public HttpClient(ConnectionConfiguration cfg) {
        this.cfg = cfg;
        this.requestBodyContentType = cfg.binaryCommunication() ? ContentType.CBOR : ContentType.JSON;
    }

    public boolean ping(long timeoutInMs) throws SQLException {
        return head("/", timeoutInMs);
    }

    public MainResponse serverInfo() throws SQLException {
        return get("/", Payloads::parseMainResponse);
    }

    public SqlQueryResponse basicQuery(String query, int fetchSize) throws SQLException {
        return basicQuery(query, fetchSize, CoreProtocol.FIELD_MULTI_VALUE_LENIENCY);
    }

    public SqlQueryResponse basicQuery(String query, int fetchSize, boolean fieldMultiValueLeniency) throws SQLException {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        // method called only from CLI
        SqlQueryRequest sqlRequest = new SqlQueryRequest(
            query,
            emptyList(),
            CoreProtocol.TIME_ZONE,
            null,
            fetchSize,
            TimeValue.timeValueMillis(cfg.queryTimeout()),
            TimeValue.timeValueMillis(cfg.pageTimeout()),
            Boolean.FALSE,
            null,
            new RequestInfo(Mode.CLI, ClientVersion.CURRENT),
            fieldMultiValueLeniency,
            false,
            cfg.binaryCommunication()
        );
        return query(sqlRequest).response();
    }

    public ResponseWithWarnings<SqlQueryResponse> query(SqlQueryRequest sqlRequest) throws SQLException {
        return post(CoreProtocol.SQL_QUERY_REST_ENDPOINT, sqlRequest, Payloads::parseQueryResponse);
    }

    public SqlQueryResponse nextPage(String cursor) throws SQLException {
        // method called only from CLI
        SqlQueryRequest sqlRequest = new SqlQueryRequest(
            cursor,
            TimeValue.timeValueMillis(cfg.queryTimeout()),
            TimeValue.timeValueMillis(cfg.pageTimeout()),
            new RequestInfo(Mode.CLI),
            cfg.binaryCommunication()
        );
        return post(CoreProtocol.SQL_QUERY_REST_ENDPOINT, sqlRequest, Payloads::parseQueryResponse).response();
    }

    public boolean queryClose(String cursor, Mode mode) throws SQLException {
        ResponseWithWarnings<SqlClearCursorResponse> response = post(
            CoreProtocol.CLEAR_CURSOR_REST_ENDPOINT,
            new SqlClearCursorRequest(cursor, new RequestInfo(mode)),
            Payloads::parseClearCursorResponse
        );
        return response.response().isSucceeded();
    }

    @SuppressWarnings({ "removal" })
    private <Request extends AbstractSqlRequest, Response> ResponseWithWarnings<Response> post(
        String path,
        Request request,
        CheckedFunction<JsonParser, Response, IOException> responseParser
    ) throws SQLException {
        byte[] requestBytes = toContent(request);
        String query = "error_trace";
        Tuple<Function<String, List<String>>, byte[]> response = java.security.AccessController.doPrivileged(
            (PrivilegedAction<ResponseOrException<Tuple<Function<String, List<String>>, byte[]>>>) () -> JreHttpUrlConnection.http(
                path,
                query,
                cfg,
                con -> con.request(
                    (out) -> out.write(requestBytes),
                    this::readFrom,
                    "POST",
                    requestBodyContentType.mediaTypeWithoutParameters() // "application/cbor" or "application/json"
                )
            )
        ).getResponseOrThrowException();
        List<String> warnings = response.v1().apply("Warning");
        return new ResponseWithWarnings<>(
            fromContent(contentType(response.v1()), response.v2(), responseParser),
            warnings == null ? Collections.emptyList() : warnings
        );
    }

    @SuppressWarnings({ "removal" })
    private boolean head(String path, long timeoutInMs) throws SQLException {
        ConnectionConfiguration pingCfg = new ConnectionConfiguration(
            cfg.baseUri(),
            cfg.connectionString(),
            cfg.validateProperties(),
            cfg.binaryCommunication(),
            cfg.connectTimeout(),
            timeoutInMs,
            cfg.queryTimeout(),
            cfg.pageTimeout(),
            cfg.pageSize(),
            cfg.authUser(),
            cfg.authPass(),
            cfg.sslConfig(),
            cfg.proxyConfig()
        );
        try {
            return java.security.AccessController.doPrivileged(
                (PrivilegedAction<Boolean>) () -> JreHttpUrlConnection.http(path, "error_trace", pingCfg, JreHttpUrlConnection::head)
            );
        } catch (ClientException ex) {
            throw new SQLException("Cannot ping server", ex);
        }
    }

    @SuppressWarnings({ "removal" })
    private <Response> Response get(String path, CheckedFunction<JsonParser, Response, IOException> responseParser) throws SQLException {
        Tuple<Function<String, List<String>>, byte[]> response = java.security.AccessController.doPrivileged(
            (PrivilegedAction<ResponseOrException<Tuple<Function<String, List<String>>, byte[]>>>) () -> JreHttpUrlConnection.http(
                path,
                "error_trace",
                cfg,
                con -> con.request(null, this::readFrom, "GET")
            )
        ).getResponseOrThrowException();
        return fromContent(contentType(response.v1()), response.v2(), responseParser);
    }

    private <Request extends AbstractSqlRequest> byte[] toContent(Request request) {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            try (JsonGenerator generator = ContentFactory.generator(requestBodyContentType, buffer)) {
                Payloads.generate(generator, request);
            }
            return buffer.toByteArray();
        } catch (Exception ex) {
            throw new ClientException("Cannot serialize request", ex);
        }
    }

    private Tuple<Function<String, List<String>>, byte[]> readFrom(InputStream inputStream, Function<String, List<String>> headers) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Streams.copy(inputStream, out);
        } catch (Exception ex) {
            throw new ClientException("Cannot deserialize response", ex);
        }
        return new Tuple<>(headers, out.toByteArray());

    }

    private ContentType contentType(Function<String, List<String>> headers) {
        List<String> contentTypeHeaders = headers.apply("Content-Type");

        String contentType = contentTypeHeaders == null || contentTypeHeaders.isEmpty() ? null : contentTypeHeaders.get(0);
        ContentType type = ContentFactory.parseMediaType(contentType);
        if (type == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + contentType);
        } else {
            return type;
        }
    }

    private <Response> Response fromContent(
        ContentType type,
        byte[] bytesReference,
        CheckedFunction<JsonParser, Response, IOException> responseParser
    ) {
        try (InputStream stream = new ByteArrayInputStream(bytesReference); JsonParser parser = ContentFactory.parser(type, stream)) {
            return responseParser.apply(parser);
        } catch (Exception ex) {
            throw new ClientException("Cannot parse response", ex);
        }
    }
}

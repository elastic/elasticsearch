/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryResponse;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection.ResponseOrException;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

public class CliHttpClient {
    private final CliConfiguration cfg;

    public CliHttpClient(CliConfiguration cfg) {
        this.cfg = cfg;
    }

    public InfoResponse serverInfo() throws SQLException {
        InfoRequest request = new InfoRequest();
        return (InfoResponse) sendRequest(request);
    }

    public QueryResponse queryInit(String query, int fetchSize) throws SQLException {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        QueryInitRequest request = new QueryInitRequest(query, fetchSize, TimeZone.getTimeZone("UTC"), timeout());
        return (QueryResponse) sendRequest(request);
    }

    public QueryResponse nextPage(byte[] cursor) throws SQLException {
        QueryPageRequest request = new QueryPageRequest(cursor, timeout());
        return (QueryResponse) sendRequest(request);
    }

    private TimeoutInfo timeout() {
        long clientTime = Instant.now().toEpochMilli();
        return new TimeoutInfo(clientTime, cfg.queryTimeout(), cfg.pageTimeout());
    }

    private Response sendRequest(Request request) throws SQLException {
        return AccessController.doPrivileged((PrivilegedAction<ResponseOrException<Response>>) () ->
            JreHttpUrlConnection.http(cfg.asUrl(), cfg, con ->
                con.post(
                    out -> Proto.INSTANCE.writeRequest(request, out),
                    in -> Proto.INSTANCE.readResponse(request, in)
                )
            )
        ).getResponseOrThrowException();
    }
}

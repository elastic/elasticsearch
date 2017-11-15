/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.TimeZone;

public class CliHttpClient {
    private final CliConfiguration cfg;

    public CliHttpClient(CliConfiguration cfg) {
        this.cfg = cfg;
    }

    public InfoResponse serverInfo() {
        InfoRequest request = new InfoRequest();
        // TODO think about error handling here
        return (InfoResponse) sendRequest(request);
    }

    public Response queryInit(String query, int fetchSize) {
        // TODO allow customizing the time zone - this is what session set/reset/get should be about
        QueryInitRequest request = new QueryInitRequest(query, fetchSize, TimeZone.getTimeZone("UTC"), timeout());
        return sendRequest(request);
    }

    public Response nextPage(byte[] cursor) {
        QueryPageRequest request = new QueryPageRequest(cursor, timeout());
        return sendRequest(request);
    }

    private TimeoutInfo timeout() {
        long clientTime = Instant.now().toEpochMilli();
        return new TimeoutInfo(clientTime, cfg.queryTimeout(), cfg.pageTimeout());
    }

    private Response sendRequest(Request request) {
        return AccessController.doPrivileged((PrivilegedAction<Response>) () ->
            JreHttpUrlConnection.http(cfg.asUrl(), cfg, con ->
                con.post(
                    out -> Proto.INSTANCE.writeRequest(request, out),
                    in -> Proto.INSTANCE.readResponse(request, in),
                    (status, failure) -> {
                        if (status >= 500) {
                            return new ErrorResponse((RequestType) request.requestType(), failure.reason(),
                                failure.type(), failure.remoteTrace());
                        }
                        return new ExceptionResponse((RequestType) request.requestType(), failure.reason(),
                            failure.type(), SqlExceptionType.fromRemoteFailureType(failure.type()));
                    }
                )
            )
        );
    }
}

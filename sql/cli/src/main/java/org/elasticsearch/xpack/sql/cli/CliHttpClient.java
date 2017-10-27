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
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.TimeZone;

public class CliHttpClient implements AutoCloseable {
    private final HttpClient http;
    private final CliConfiguration cfg;

    public CliHttpClient(CliConfiguration cfg) {
        this.cfg = cfg;
        this.http = new HttpClient(cfg);
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
        Bytes ba = http.post(out -> Proto.INSTANCE.writeRequest(request, out));
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.bytes(), 0, ba.size()))) {
            return Proto.INSTANCE.readResponse(request, in);
        } catch (IOException ex) {
            throw new CliException(ex, "Cannot read response");
        }
    }

    public void close() {}
}



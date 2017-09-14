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
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.TimeZone;

public class CliHttpClient implements AutoCloseable {
    private final HttpClient http;

    public CliHttpClient(CliConfiguration cfg) {
        http = new HttpClient(cfg);
    }

    public InfoResponse serverInfo() {
        InfoRequest request = new InfoRequest();
        return (InfoResponse) sendRequest(request);
    }

    public QueryInitResponse queryInit(String query, int fetchSize) {
        // TODO allow customizing the time zone
        // NOCOMMIT figure out Timeouts....
        QueryInitRequest request = new QueryInitRequest(query, fetchSize, TimeZone.getTimeZone("UTC"), new TimeoutInfo(0, 0, 0));
        return (QueryInitResponse) sendRequest(request);
    }

    public QueryPageResponse nextPage(byte[] cursor) {
        // NOCOMMIT figure out Timeouts....
        QueryPageRequest request = new QueryPageRequest(cursor, new TimeoutInfo(0, 0, 0));
        return (QueryPageResponse) sendRequest(request);
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



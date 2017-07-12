/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.CliException;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class CliHttpClient implements AutoCloseable {
    private final HttpClient http;

    public CliHttpClient(CliConfiguration cfg) {
        http = new HttpClient(cfg);
    }

    public Response serverInfo() {
        InfoRequest request = new InfoRequest();
        Bytes ba = http.post(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> Proto.INSTANCE.readResponse(request, in));
    }

    public Response command(String command, String requestId) {
        CommandRequest request = new CommandRequest(command);
        Bytes ba = http.post(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> Proto.INSTANCE.readResponse(request, in));
    }

    private static <T> T doIO(Bytes ba, DataInputFunction<T> action) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.bytes(), 0, ba.size()))) {
            return action.apply(in);
        } catch (IOException ex) {
            throw new CliException(ex, "Cannot read response");
        }
    }

    public void close() {}

    @FunctionalInterface
    private interface DataInputFunction<R> {
        R apply(DataInput in) throws IOException;
    }
}



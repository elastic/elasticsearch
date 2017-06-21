/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.CliException;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;

public class HttpCliClient implements AutoCloseable {
    @FunctionalInterface
    interface DataInputFunction<R> {
        R apply(DataInput in) throws IOException;
    }

    private final HttpClient http;
    private final CliConfiguration cfg;

    public HttpCliClient(CliConfiguration cfg) {
        http = new HttpClient(cfg);
        this.cfg = cfg;
    }

    public InfoResponse serverInfo() {
        Bytes ba = http.put(out -> ProtoUtils.write(out, new InfoRequest()));
        return doIO(ba, in -> readResponse(in, Action.INFO));
    }

    public CommandResponse command(String command, String requestId) {
        Bytes ba = http.put(out -> ProtoUtils.write(out, new CommandRequest(command)));
        return doIO(ba, in -> {
            CommandResponse response = readResponse(in, Action.COMMAND);
            // read data
            String result = in.readUTF();
            return new CommandResponse(response.serverTimeQueryReceived, response.serverTimeResponseSent, response.requestId, result);
        });
    }

    private static <T> T doIO(Bytes ba, DataInputFunction<T> action) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(ba.bytes(), 0, ba.size()))) {
            return action.apply(in);
        } catch (IOException ex) {
            throw new CliException(ex, "Cannot read response");
        }
    }

    @SuppressWarnings("unchecked")
    private static <R extends Response> R readResponse(DataInput in, Action expected) throws IOException {
        String errorMessage = ProtoUtils.readHeader(in);
        if (errorMessage != null) {
            throw new CliException(errorMessage);
        }

        int header = in.readInt();

        Action action = Action.from(header);
        if (expected != action) {
            throw new CliException("Expected response for %s, found %s", expected, action);
        }

        Response response = ProtoUtils.readResponse(in, header);

        if (response instanceof ExceptionResponse) {
            ExceptionResponse ex = (ExceptionResponse) response;
            throw new CliException(ex.message);
        }
        if (response instanceof ErrorResponse) {
            ErrorResponse error = (ErrorResponse) response;
            throw new CliException("%s", error.stack);
        }
        if (response instanceof Response) {
            return (R) response;
        }

        throw new CliException("Invalid response status %08X", header);
    }

    public void close() {}
}
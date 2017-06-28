/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.client;

import org.elasticsearch.xpack.sql.cli.CliConfiguration;
import org.elasticsearch.xpack.sql.cli.CliException;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.net.client.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class CliHttpClient implements AutoCloseable {

    private final HttpClient http;
    private final CliConfiguration cfg;

    public CliHttpClient(CliConfiguration cfg) {
        http = new HttpClient(cfg);
        this.cfg = cfg;
    }

    public Response serverInfo() {
        Bytes ba = http.put(out -> ProtoUtils.write(out, new InfoRequest(System.getProperties())));
        return doIO(ba, in -> readResponse(in, Action.INFO));
    }

    public Response command(String command, String requestId) {
        Bytes ba = http.put(out -> ProtoUtils.write(out, new CommandRequest(command)));
        return doIO(ba, in -> {
            Response response = readResponse(in, Action.COMMAND);
            // read data
            if (response instanceof CommandResponse) {
                String result = in.readUTF();
                CommandResponse cr = (CommandResponse) response;
                return new CommandResponse(cr.serverTimeQueryReceived, cr.serverTimeResponseSent, cr.requestId, result);
            }
            return response;
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

        return (R) ProtoUtils.readResponse(in, header);
    }

    public void close() {}

    @FunctionalInterface
    private interface DataInputFunction<R> {
        R apply(DataInput in) throws IOException;
    }
}



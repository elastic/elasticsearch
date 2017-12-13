/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Base implementation for the binary protocol for the CLI and JDBC.
 * All backwards compatibility is done on the server side using the
 * version number sent in the header.
 */
public abstract class AbstractProto {
    private static final int MAGIC_NUMBER = 0x0C0DEC110;
    public static final int CURRENT_VERSION = 000_000_001;

    public void writeRequest(Request request, DataOutput out) throws IOException {
        writeHeader(CURRENT_VERSION, out);
        request.requestType().writeTo(out);
        request.writeTo(new SqlDataOutput(out, CURRENT_VERSION));
    }

    public SqlDataInput clientStream(DataInput in) throws IOException {
        int clientVersion = readHeader(in);
        if (clientVersion > CURRENT_VERSION) {
            throw new IOException("Unknown client version [" + clientVersion + "]. Always upgrade client last.");
        }
        return new SqlDataInput(in, clientVersion);
    }

    public Request readRequest(SqlDataInput in) throws IOException {
        return readRequestType(in).reader().read(in);
    }

    public Request readRequest(DataInput in) throws IOException {
        SqlDataInput client = clientStream(in);
        return readRequest(client);
    }

    public void writeResponse(Response response, int clientVersion, DataOutput out) throws IOException {
        writeHeader(clientVersion, out);
        response.responseType().writeTo(out);
        response.writeTo(new SqlDataOutput(out, clientVersion));
    }

    public Response readResponse(Request request, DataInput in) throws IOException {
        int version = readHeader(in);
        if (version != CURRENT_VERSION) {
            throw new IOException("Response version [" + version + "] does not match client version ["
                    + CURRENT_VERSION + "]. Server is busted.");
        }
        // TODO why do I need the response type at all? Just a byte for err/exception/normal, then get response type from request.
        Response response = readResponseType(in).reader().read(request, new SqlDataInput(in, version));
        if (response.requestType() != request.requestType()) {
            throw new IOException("Expected request type to be [" + request.requestType()
                    + "] but was [" + response.requestType() + "]. Server is busted.");
        }
        return response;
    }

    protected abstract RequestType readRequestType(DataInput in) throws IOException;
    protected abstract ResponseType readResponseType(DataInput in) throws IOException;
    @FunctionalInterface
    protected interface RequestReader {
        Request read(SqlDataInput in) throws IOException;
    }
    protected interface RequestType {
        void writeTo(DataOutput out) throws IOException;
        RequestReader reader();
    }
    @FunctionalInterface
    protected interface ResponseReader {
        Response read(Request request, SqlDataInput in) throws IOException;
    }
    protected interface ResponseType {
        void writeTo(DataOutput out) throws IOException;
        ResponseReader reader();
    }

    private static void writeHeader(int clientVersion, DataOutput out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(clientVersion);
    }

    /**
     * Read the protocol header.
     * @return the version
     * @throws IOException if there is an underlying {@linkplain IOException} or if the protocol is malformed
     */
    private static int readHeader(DataInput in) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Unknown protocol magic number [" + Integer.toHexString(magic) + "]");
        }
        int version = in.readInt();
        return version;
    }
}

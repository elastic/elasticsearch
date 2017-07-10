/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Binary protocol for the CLI. All backwards compatibility is done using the
 * version number sent in the header. 
 */
public abstract class Proto {
    private static final int MAGIC_NUMBER = 0x0C0DEC110;
    public static final int CURRENT_VERSION = 000_000_001;

    private Proto() {
        // Static utilities
    }

    public static void writeRequest(Request request, DataOutput out) throws IOException {
        writeHeader(CURRENT_VERSION, out);
        request.requestType().write(out);
        request.write(out);
    }

    public static Request readRequest(DataInput in) throws IOException {
        int clientVersion = readHeader(in);
        if (clientVersion > CURRENT_VERSION) {
            throw new IOException("Unknown client version [" + clientVersion + "]. Always upgrade sql last.");
            // NOCOMMIT I believe we usually advise upgrading the clients *first* so this might be backwards.....
        }
        return RequestType.read(in).reader.read(clientVersion, in);
    }

    public static void writeResponse(Response response, int clientVersion, DataOutput out) throws IOException {
        writeHeader(clientVersion, out);
        response.responseType().write(out);
        response.write(clientVersion, out);
    }

    public static Response readResponse(RequestType expectedRequestType, DataInput in) throws IOException {
        int version = readHeader(in);
        if (version != CURRENT_VERSION) {
            throw new IOException("Response version [" + version + "] does not match client version ["
                    + CURRENT_VERSION + "]. Server is busted.");
        }
        Response response = ResponseType.read(in).reader.read(in);
        if (response.requestType() != expectedRequestType) {
            throw new IOException("Expected request type to be [" + expectedRequestType
                    + "] but was [" + response.requestType() + "]. Server is busted.");
        }
        return response;
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

    @FunctionalInterface
    interface RequestReader {
        Request read(int clientVersion, DataInput in) throws IOException;
    }
    public enum RequestType {
        INFO(InfoRequest::new),
        COMMAND(CommandRequest::new);

        private final RequestReader reader;

        RequestType(RequestReader reader) {
            this.reader = reader;
        }

        void write(DataOutput out) throws IOException {
            out.writeByte(ordinal());
        }

        static RequestType read(DataInput in) throws IOException {
            byte b = in.readByte();
            try {
                return values()[b];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Unknown response type [" + b + "]", e);
            }
        }
    }

    @FunctionalInterface
    interface ResponseReader {
        Response read(DataInput in) throws IOException;
    }
    enum ResponseType {
        EXCEPTION(ExceptionResponse::new),
        ERROR(ErrorResponse::new),
        INFO(InfoResponse::new),
        COMMAND(CommandResponse::new);

        private final ResponseReader reader;

        ResponseType(ResponseReader reader) {
            this.reader = reader;
        }

        void write(DataOutput out) throws IOException {
            out.writeByte(ordinal());
        }

        static ResponseType read(DataInput in) throws IOException {
            byte b = in.readByte();
            try {
                return values()[b];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Unknown response type [" + b + "]", e);
            }
        }
    }
}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Binary protocol for the CLI. All backwards compatibility is done using the
 * version number sent in the header. 
 */
public final class Proto extends AbstractProto {
    public static final Proto INSTANCE = new Proto();

    private Proto() {}

    @Override
    protected RequestType readRequestType(DataInput in) throws IOException {
        return RequestType.read(in);
    }

    @Override
    protected ResponseType readResponseType(DataInput in) throws IOException {
        return ResponseType.read(in);
    }

    public enum RequestType implements AbstractProto.RequestType {
        INFO(InfoRequest::new),
        COMMAND(CommandRequest::new);

        private final RequestReader reader;

        RequestType(RequestReader reader) {
            this.reader = reader;
        }

        static RequestType read(DataInput in) throws IOException {
            byte b = in.readByte();
            try {
                return values()[b];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Unknown request type [" + b + "]", e);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(ordinal());
        }

        @Override
        public RequestReader reader() {
            return reader;
        }
    }

    enum ResponseType implements AbstractProto.ResponseType {
        EXCEPTION(ExceptionResponse::new),
        ERROR(ErrorResponse::new),
        INFO(InfoResponse::new),
        COMMAND(CommandResponse::new);

        private final ResponseReader reader;

        ResponseType(ResponseReader reader) {
            this.reader = reader;
        }

        static ResponseType read(DataInput in) throws IOException {
            byte b = in.readByte();
            try {
                return values()[b];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException("Unknown response type [" + b + "]", e);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(ordinal());
        }

        @Override
        public ResponseReader reader() {
            return reader;
        }
    }
}
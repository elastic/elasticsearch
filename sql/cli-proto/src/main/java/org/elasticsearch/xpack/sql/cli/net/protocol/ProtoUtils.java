/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Status;

import static java.lang.String.format;

import static org.elasticsearch.xpack.sql.cli.net.protocol.Proto.MAGIC_NUMBER;
import static org.elasticsearch.xpack.sql.cli.net.protocol.Proto.VERSION;

public abstract class ProtoUtils {

    public static void write(DataOutput out, Message m) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(VERSION);
        m.encode(out);
    }

    public static Request readRequest(DataInput in) throws IOException {
        switch (Action.from(in.readInt())) {
            case INFO:
                return InfoRequest.decode(in);
            case COMMAND:
                return CommandRequest.decode(in);
            default:
                // cannot find action type
                return null;
        }
    }

    public static Response readResponse(DataInput in, int header) throws IOException {
        Action action = Action.from(header);

        switch (Status.from(header)) {
            case EXCEPTION:
                return ExceptionResponse.decode(in, action);
            case ERROR:
                return ErrorResponse.decode(in, action);
            case SUCCESS:
                switch (action) {
                    case INFO:
                        return InfoResponse.decode(in);
                    case COMMAND:
                        return CommandResponse.decode(in);
                    default:
                        // cannot find action type
                        return null;
                }
            default:
                return null;
        }
    }

    public static String readHeader(DataInput in) throws IOException {
        if (MAGIC_NUMBER != in.readInt()) {
            return "Invalid protocol";
        }
        int ver = in.readInt();
        if (VERSION != ver) {
            return format(Locale.ROOT, "Expected JDBC protocol version %s, found %s", VERSION, ver);
        }

        return null;
    }
}
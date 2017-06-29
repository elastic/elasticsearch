/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Status;

public class ErrorResponse extends Response {

    public final String message, cause, stack;

    public ErrorResponse(Action requestedAction, String message, String cause, String stack) {
        super(requestedAction);
        this.message = message;
        this.cause = cause;
        this.stack = stack;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toError(action));
        out.writeUTF(message);
        out.writeUTF(cause);
        out.writeUTF(stack);
    }

    public static ErrorResponse decode(DataInput in, Action action) throws IOException {
        String message = in.readUTF();
        String cause = in.readUTF();
        String stack = in.readUTF();
        return new ErrorResponse(action, message, cause, stack);
    }
}

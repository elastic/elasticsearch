/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.SqlExceptionType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Status;

public class ExceptionResponse extends Response {

    public final SqlExceptionType asSql;
    public final String message, cause;

    public ExceptionResponse(Action requestedAction, String message, String cause, SqlExceptionType asSql) {
        super(requestedAction);
        this.message = message;
        this.cause = cause;
        this.asSql = asSql;
    }

    @Override
    public void encode(DataOutput out) throws IOException {
        out.writeInt(Status.toException(action));
        out.writeUTF(message);
        out.writeUTF(cause);
        out.writeInt(asSql.value());
    }

    public static ExceptionResponse decode(DataInput in, Action action) throws IOException {
        String message = in.readUTF();
        String cause = in.readUTF();
        int sqlType = in.readInt();
        return new ExceptionResponse(action, message, cause, SqlExceptionType.from(sqlType));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.http;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class CliServerProtoUtils {

    public static BytesReference write(Response response) throws IOException {
        try (BytesStreamOutput array = new BytesStreamOutput();
             DataOutputStream out = new DataOutputStream(array)) {
            ProtoUtils.write(out, response);

            // serialize payload (if present)
            if (response instanceof CommandResponse) {
                RowSetCursor cursor = (RowSetCursor) ((CommandResponse) response).data;

                if (cursor != null) {
                    out.writeUTF(CliUtils.toString(cursor));
                }
            }

            out.flush();
            return array.bytes();
        }
    }

    public static Response exception(Throwable cause, Action action) {
        String message = EMPTY;
        String cs = EMPTY;
        if (cause != null) {
            if (Strings.hasText(cause.getMessage())) {
                message = cause.getMessage();
            }
            cs = cause.getClass().getName();
        }

        if (expectedException(cause)) {
            return new ExceptionResponse(action, message, cs);
        }
        else {
            // TODO: might want to 'massage' this
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            return new ErrorResponse(action, message, cs, sw.toString());
        }
    }

    private static boolean expectedException(Throwable cause) {
        return (cause instanceof SqlException || cause instanceof ResourceNotFoundException);
    }
}
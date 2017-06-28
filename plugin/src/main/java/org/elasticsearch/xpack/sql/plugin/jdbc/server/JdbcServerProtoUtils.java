/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.server;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Types;
import java.util.concurrent.TimeoutException;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.DataResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.SqlExceptionType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.joda.time.ReadableDateTime;

import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public abstract class JdbcServerProtoUtils {

    public static BytesReference write(Response response) throws IOException {
        try (BytesStreamOutput array = new BytesStreamOutput();
            DataOutputStream out = new DataOutputStream(array)) {
            ProtoUtils.write(out, response);

            // serialize payload (if present)
            if (response instanceof DataResponse) { // NOCOMMIT why not implement an interface?
                RowSetCursor cursor = (RowSetCursor) ((QueryInitResponse) response).data;

                if (cursor != null) {
                    JdbcServerProtoUtils.write(out, cursor);
                }
            }
            out.flush();
            return array.bytes();
        }
    }
    
    private static void write(DataOutput out, RowSet rowSet) throws IOException {
        out.writeInt(rowSet.size());
        int[] jdbcTypes = rowSet.schema().types().stream()
                .mapToInt(dt -> dt.sqlType().getVendorTypeNumber())
                .toArray();

        // unroll forEach manually to avoid a Consumer + try/catch for each value...
        for (boolean hasRows = rowSet.hasCurrentRow(); hasRows; hasRows = rowSet.advanceRow()) {
            for (int i = 0; i < rowSet.rowSize(); i++) {
                Object value = rowSet.column(i);
                // unpack Joda classes on the server-side to not 'pollute' the common project and thus the client 
                if (jdbcTypes[i] == Types.TIMESTAMP_WITH_TIMEZONE && value instanceof ReadableDateTime) {
                    value = ((ReadableDateTime) value).getMillis();
                }
                ProtoUtils.writeValue(out, value, jdbcTypes[i]);
            }
        }
    }
    
    public static Response exception(Throwable cause, Action action) {
        SqlExceptionType sqlExceptionType = sqlExceptionType(cause);

        String message = EMPTY;
        String cs = EMPTY;
        if (cause != null) {
            if (Strings.hasText(cause.getMessage())) {
                message = cause.getMessage();
            }
            cs = cause.getClass().getName();
        }

        if (sqlExceptionType != null) {
            return new ExceptionResponse(action, message, cs, sqlExceptionType);
        }
        else {
            // TODO: might want to 'massage' this
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            return new ErrorResponse(action, message, cs, sw.toString());
        }
    }

    private static SqlExceptionType sqlExceptionType(Throwable cause) {
        if (cause instanceof AnalysisException || cause instanceof ResourceNotFoundException) {
            return SqlExceptionType.DATA;
        }
        if (cause instanceof ParsingException) {
            return SqlExceptionType.SYNTAX;
        }
        if (cause instanceof TimeoutException) {
            return SqlExceptionType.TIMEOUT;
        }

        return null;
    }
}
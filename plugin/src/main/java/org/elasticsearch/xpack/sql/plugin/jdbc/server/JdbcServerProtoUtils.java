/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.server;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.DataResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.RowSetCursor;

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
        for (boolean hasRows = rowSet.hasCurrent(); hasRows; hasRows = rowSet.advance()) {
            for (int i = 0; i < rowSet.rowSize(); i++) {
                ProtoUtils.writeValue(out, rowSet.column(i), jdbcTypes[i]);
            }
        }
    }
}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Payload;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.ReadableInstant;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.JDBCType;
import java.util.List;

public class RowSetPayload implements Payload {
    private final RowSet rowSet;

    public RowSetPayload(RowSet rowSet) {
        this.rowSet = rowSet;
    }

    @Override
    public void read(DataInput in) throws IOException {
        throw new UnsupportedOperationException("This class can only be serialized");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(rowSet.size());
        List<DataType> types = rowSet.schema().types();

        // unroll forEach manually to avoid a Consumer + try/catch for each value...
        for (boolean hasRows = rowSet.hasCurrentRow(); hasRows; hasRows = rowSet.advanceRow()) {
            for (int i = 0; i < rowSet.rowSize(); i++) {
                Object value = rowSet.column(i);
                // unpack Joda classes on the server-side to not 'pollute' the common project and thus the client 
                if (types.get(i).sqlType() == JDBCType.TIMESTAMP && value instanceof ReadableInstant) {
                    // NOCOMMIT feels like a hack that'd be better cleaned up another way.
                    value = ((ReadableInstant) value).getMillis();
                }
                ProtoUtils.writeValue(out, value, types.get(i).sqlType());
            }
        }
    }
}

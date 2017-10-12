/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Payload;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataInput;
import org.elasticsearch.xpack.sql.protocol.shared.SqlDataOutput;
import org.joda.time.ReadableInstant;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.List;
import java.util.Objects;

/**
 * Implementation {@link Payload} that adapts it to data from
 * {@link SqlResponse}.
 */
class SqlResponsePayload implements Payload {
    private final List<JDBCType> typeLookup;
    private final List<List<Object>> rows;

    SqlResponsePayload(List<JDBCType> typeLookup, List<List<Object>> rows) {
        this.typeLookup = typeLookup;
        this.rows = rows;
    }

    @Override
    public void readFrom(SqlDataInput in) throws IOException {
        throw new UnsupportedOperationException("This class can only be serialized");
    }

    @Override
    public void writeTo(SqlDataOutput out) throws IOException {
        out.writeInt(rows.size());

        // unroll forEach manually to avoid a Consumer + try/catch for each value...
        for (List<Object> row : rows) {
            for (int c = 0; c < row.size(); c++) {
                JDBCType type = typeLookup.get(c);
                Object value = row.get(c);
                // unpack Joda classes on the server-side to not 'pollute' the common project and thus the client 
                if (type == JDBCType.TIMESTAMP && value instanceof ReadableInstant) {
                    // NOCOMMIT feels like a hack that'd be better cleaned up another way.
                    value = ((ReadableInstant) value).getMillis();
                }
                ProtoUtils.writeValue(out, value, type);
            }
        }
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(typeLookup, rows);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        SqlResponsePayload other = (SqlResponsePayload) obj;
        return Objects.equals(typeLookup, other.typeLookup)
                && Objects.equals(rows, other.rows);
    }
}

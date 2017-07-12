/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.JDBCType;

/**
 * Abstract base class for a page of results. The canonical implementation in {@link Page}
 * and implementation must write usings the same format as {@linkplain Page}.
 */
public abstract class ResultPage {
    public abstract void write(DataOutput out) throws IOException;

    // See Jdbc spec, appendix B
    @SuppressWarnings("unchecked")
    protected static <T> T readValue(DataInput in, JDBCType type) throws IOException {
        // NOCOMMIT <T> feels slippery here
        Object result;
        byte hasNext = in.readByte();
        if (hasNext == 0) { // NOCOMMIT feels like a bitmask at the start of the row would be better.
            return null;
        }
        // NOCOMMIT we ought to make sure we use all of these
        switch (type) {
            case NULL:
                // used to move the stream forward
                // NOCOMMIT why serialize NULL types at all?
                in.readBoolean();
                return null;
            case BIT:
            case BOOLEAN:
                result = Boolean.valueOf(in.readBoolean());
                break;
            case TINYINT:
                result = Byte.valueOf(in.readByte());
                break;
            case SMALLINT:
                result = Short.valueOf(in.readShort());
                break;
            case INTEGER:
                result = Integer.valueOf(in.readInt());
                break;
            case BIGINT:
                result = Long.valueOf(in.readLong());
                break;
            case FLOAT:
            case DOUBLE:
                result = Double.valueOf(in.readDouble());
                break;
            case REAL:
                result = Float.valueOf(in.readFloat());
                break;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                int size = in.readInt();
                byte[] ar = new byte[size];
                in.readFully(ar, 0, size);
                result = ar;
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                result = in.readUTF();
                break;
            // NB: date/time is kept in its raw form since the JdbcDriver has to do calendar/timezone conversion anyway and thus the long value is relevant
            case TIMESTAMP:
                result = in.readLong();
                break;
            default:
                throw new IOException("Don't know how to read type [" + type + "]");
        }
        return (T) result;
    }

    protected static void writeValue(DataOutput out, Object o, JDBCType type) throws IOException {
        if (o == null) {
            out.writeByte(0);
            return;
        }
        out.writeByte(1);

        switch (type) {
            // NOCOMMIT we ought to make sure we use all of these
            case NULL:
                // used to move the stream forward
                out.writeBoolean(false);
                return;
            case BIT:
            case BOOLEAN:
                out.writeBoolean((Boolean) o);
                return;
            case TINYINT:
                out.writeByte(((Number) o).byteValue());
                return;
            case SMALLINT:
                out.writeShort(((Number) o).shortValue());
                return;
            case INTEGER:
                out.writeInt(((Number) o).intValue());
                return;
            case BIGINT:
                out.writeLong(((Number) o).longValue());
                return;
            case FLOAT:
            case DOUBLE:
                out.writeDouble(((Number) o).doubleValue());
                return;
            case REAL:
                out.writeFloat(((Number) o).floatValue());
                return;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                byte[] a = (byte[]) o;
                if (a == null || a.length == 0) {
                    out.writeInt(0);
                    return;
                }
                out.writeInt(a.length);
                out.write(a);
                return;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                out.writeUTF(o.toString());
                return;
            case TIMESTAMP:
                out.writeLong(((Number) o).longValue());
                return;
            default:
                throw new IOException("Don't know how to write type [" + type + "]");
        }
    }

    /**
     * The type of the array used to store columns of this type.
     */
    protected static Class<?> classOf(JDBCType jdbcType) {
        switch (jdbcType) {
            case NUMERIC:
            case DECIMAL:
                return BigDecimal.class;
            case BOOLEAN:
            case BIT:
                return Boolean.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                // NOCOMMIT should we be using primitives instead?
                return Integer.class;
            case BIGINT:
                return Long.class;
            case REAL:
                return Float.class; 
            case FLOAT:
            case DOUBLE:
                return Double.class;
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
                return byte[].class;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                return String.class;
            case DATE:
            case TIME:
            case TIMESTAMP:
                return Long.class;
            case BLOB:
                return Blob.class;
            case CLOB:
                return Clob.class;
            default:
                throw new IllegalArgumentException("Unsupported JDBC type [" + jdbcType + "]");
        }
    }
}

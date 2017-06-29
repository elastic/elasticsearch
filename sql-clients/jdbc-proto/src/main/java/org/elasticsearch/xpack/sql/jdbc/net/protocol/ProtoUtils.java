/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Status;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Locale;

import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NULL;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.MAGIC_NUMBER;
import static org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.VERSION;

public abstract class ProtoUtils {

    private static final byte[] EMPTY_BYTES = new byte[0];

    public static void write(DataOutput out, Message m) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(VERSION);
        m.encode(out);
    }

    public static Request readRequest(DataInput in) throws IOException {
        switch (Action.from(in.readInt())) {
            case INFO:
                return InfoRequest.decode(in);
            case META_TABLE:
                return MetaTableRequest.decode(in);
            case META_COLUMN:
                return MetaColumnRequest.decode(in);
            case QUERY_INIT:
                return new QueryInitRequest(in);
            case QUERY_PAGE:
                return QueryPageRequest.decode(in);
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
                    case META_TABLE:
                        return MetaTableResponse.decode(in);
                    case META_COLUMN:
                        return MetaColumnResponse.decode(in);
                    case QUERY_INIT:
                        return QueryInitResponse.decode(in);
                    case QUERY_PAGE:
                        return QueryPageResponse.decode(in);
                    default:
                        // cannot find action type
                        // NOCOMMIT it feels like this should throw *something*
                        return null;
                }
            default:
                return null;
        }
    }

    public static String readHeader(DataInput in) throws IOException {
        // NOCOMMIT why not just throw?
        int magic = in.readInt();
        if (MAGIC_NUMBER != magic) {
            return "Invalid protocol";
        }
        int ver = in.readInt();
        if (VERSION != ver) {
            return format(Locale.ROOT, "Expected JDBC protocol version %s, found %s", VERSION, ver);
        }

        return null;
    }

    //
    // value read
    //

    // See Jdbc spec, appendix B
    @SuppressWarnings("unchecked")
    public static <T> T readValue(DataInput in, int type) throws IOException {
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
                if (size == 0) {
                    result = EMPTY_BYTES;
                }
                else {
                    byte[] ar = new byte[size];
                    in.readFully(ar, 0, size);
                    result = ar;
                }
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                result = in.readUTF();
                break;
            case TIMESTAMP:
                result = new Timestamp(in.readLong());
                break;
            default:
                throw new IOException("Don't know how to read type [" + type + " / " + JDBCType.valueOf(type) + "]");
        }
        return (T) result;
    }

    public static void writeValue(DataOutput out, Object o, int type) throws IOException {
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
        }
    }
}
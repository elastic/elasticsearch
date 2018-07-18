/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.joda.time.DateTime;

public abstract class DataTypes {

    public static boolean isNull(DataType from) {
        return from == DataType.NULL;
    }

    public static boolean isUnsupported(DataType from) {
        return from == DataType.UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return DataType.NULL;
        }
        if (value instanceof Integer) {
            return DataType.INTEGER;
        }
        if (value instanceof Long) {
            return DataType.LONG;
        }
        if (value instanceof Boolean) {
            return DataType.BOOLEAN;
        }
        if (value instanceof Double) {
            return DataType.DOUBLE;
        }
        if (value instanceof Float) {
            return DataType.FLOAT;
        }
        if (value instanceof Byte) {
            return DataType.BYTE;
        }
        if (value instanceof Short) {
            return DataType.SHORT;
        }
        if (value instanceof DateTime) {
            return DataType.DATE;
        }
        if (value instanceof String || value instanceof Character) {
            return DataType.KEYWORD;
        }
        throw new SqlIllegalArgumentException("No idea what's the DataType for {}", value.getClass());
    }

    //
    // Metadata methods, mainly for ODBC.
    // As these are fairly obscure and limited in use, there is no point to promote them as a full type methods
    // hence why they appear here as utility methods.
    //

    // https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-date-time/metadata-catalog
    // https://github.com/elastic/elasticsearch/issues/30386
    public static Integer metaSqlDataType(DataType t) {
        if (t == DataType.DATE) {
            // ODBC SQL_DATETME
            return Integer.valueOf(9);
        }
        // this is safe since the vendor SQL types are short despite the return value
        return t.jdbcType.getVendorTypeNumber();
    }

    // https://github.com/elastic/elasticsearch/issues/30386
    // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function?view=sql-server-2017
    public static Integer metaSqlDateTimeSub(DataType t) {
        if (t == DataType.DATE) {
            // ODBC SQL_CODE_TIMESTAMP
            return Integer.valueOf(3);
        }
        // ODBC null
        return 0;
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/decimal-digits?view=sql-server-2017
    public static Short metaSqlMinimumScale(DataType t) {
        // TODO: return info for HALF/SCALED_FLOATS (should be based on field not type)
        if (t == DataType.DATE) {
            return Short.valueOf((short) 3);
        }
        if (t.isInteger) {
            return Short.valueOf((short) 0);
        }
        // minimum scale?
        if (t.isRational) {
            return Short.valueOf((short) 0);
        }
        return null;
    }

    public static Short metaSqlMaximumScale(DataType t) {
        // TODO: return info for HALF/SCALED_FLOATS (should be based on field not type)
        if (t == DataType.DATE) {
            return Short.valueOf((short) 3);
        }
        if (t.isInteger) {
            return Short.valueOf((short) 0);
        }
        if (t.isRational) {
            return Short.valueOf((short) t.defaultPrecision);
        }
        return null;
    }

    // https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgettypeinfo-function?view=sql-server-2017
    public static Integer metaSqlRadix(DataType t) {
        // RADIX  - Determines how numbers returned by COLUMN_SIZE and DECIMAL_DIGITS should be interpreted.
        // 10 means they represent the number of decimal digits allowed for the column.
        // 2 means they represent the number of bits allowed for the column.
        // null means radix is not applicable for the given type.
        return t.isInteger ? Integer.valueOf(10) : (t.isRational ? Integer.valueOf(2) : null);
    }
}
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

abstract class JdbcUtils {

    static boolean isSigned(JDBCType type) {
        switch (type) {
            case BIGINT:
            case DECIMAL:
            case DOUBLE:
            case INTEGER:
            case SMALLINT:
            case FLOAT:
            case REAL:
            case NUMERIC:
            case TINYINT:
                return true;
            default:
                return false;
        }
    }

    static int scale(JDBCType type) {
        switch (type) {
            case REAL: return 7;
            case FLOAT:
            case DOUBLE: return 16;
            default: return 0;
        }
    }

    static int precision(JDBCType type) {
        switch (type) {
            case NULL: return 0;
            case BOOLEAN: return 1;
            case TINYINT: return 3;
            case SMALLINT: return 5;
            case INTEGER: return 10;
            case BIGINT: return 19;
            // 24 bits precision - 24*log10(2) =~ 7 (7.22)
            case REAL: return 7;
            // 53 bits precision ~ 16(15.95) decimal digits (53log10(2))
            case FLOAT:
            case DOUBLE: return 16;
            case VARBINARY: return Integer.MAX_VALUE;
            case TIME_WITH_TIMEZONE: return displaySize(type);
            default:
                return -1;
        }
    }

    static int displaySize(JDBCType type) {
        switch (type) {
            case NULL: return 0;
            case BOOLEAN: return 1;
            case TINYINT: return 3;
            case SMALLINT: return 6;
            case INTEGER: return 11;
            case BIGINT: return 20;
            case REAL: return 15;
            case FLOAT:
            case DOUBLE: return 25;
            case VARCHAR:
            case VARBINARY: return -1;
            case TIMESTAMP: return 20;
            default:
                return -1;
        }
    }

    static boolean isRational(JDBCType type) {
        switch (type) {
            case REAL:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case NUMERIC:
                return true;
            default:
                return false;
        }
    }

    static boolean isInteger(JDBCType type) {
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return true;
            default:
                return false;
        }
    }

    static int size(JDBCType type) {
        switch (type) {
            case NULL: return 0;
            case BOOLEAN: return 1;
            case TINYINT: return Byte.BYTES;
            case SMALLINT: return Short.BYTES;
            case INTEGER: return Integer.BYTES;
            case TIMESTAMP:
            case BIGINT: return Long.BYTES;
            case REAL: return Float.BYTES;
            case FLOAT:
            case DOUBLE: return Double.BYTES;
            case VARCHAR:
            case VARBINARY: return Integer.MAX_VALUE;
            default:
                return -1;
        }
    }
}
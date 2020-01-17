/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeConversion;
import org.elasticsearch.xpack.ql.type.DataTypeConversion.Conversion;

import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.DATE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.compatibleInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isInterval;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.isYearMonthInterval;

public class SqlTypeConverter {

    public static DataType commonType(DataType left, DataType right) {
        DataType common = DataTypeConversion.commonType(left, right);

        if (common != null) {
            return common;
        }

        // interval and dates
        if (left == DATE) {
            if (isYearMonthInterval(right)) {
                return left;
            }
            // promote
            return DATETIME;
        }
        if (right == DATE) {
            if (isYearMonthInterval(left)) {
                return right;
            }
            // promote
            return DATETIME;
        }
        if (left == TIME) {
            if (right == DATE) {
                return DATETIME;
            }
            if (isInterval(right)) {
                return left;
            }
        }
        if (right == TIME) {
            if (left == DATE) {
                return DATETIME;
            }
            if (isInterval(left)) {
                return right;
            }
        }
        if (left == DATETIME) {
            if (right == DATE || right == TIME) {
                return left;
            }
            if (isInterval(right)) {
                return left;
            }
        }
        if (right == DATETIME) {
            if (left == DATE || left == TIME) {
                return right;
            }
            if (isInterval(left)) {
                return right;
            }
        }
        // Interval * integer is a valid operation
        if (isInterval(left)) {
            if (right.isInteger()) {
                return left;
            }
        }
        if (isInterval(right)) {
            if (left.isInteger()) {
                return right;
            }
        }
        if (isInterval(left)) {
            // intervals widening
            if (isInterval(right)) {
                // null returned for incompatible intervals
                return compatibleInterval(left, right);
            }
        }

        return null;
    }

    public static Conversion conversionFor(DataType from, DataType to) {
        case DATE:
            return conversionToDate(from);
        case TIME:
            return conversionToTime(from);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeUnit;

/**
 * What a Parquet column means, for the types that the mapping cares about. A file may say it with the converted type of old writers,
 * with the logical type of newer ones, or with both, so this is the one place that reads them.
 */
public enum LogicalKind {
    /** No annotation: a plain integer, floating point number or byte array. */
    NONE,
    STRING,
    ENUM,
    JSON,
    DATE,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    TIMESTAMP_NANOS,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    DECIMAL,
    TIME,
    /** An annotation that the mapping does not know. */
    OTHER;

    static LogicalKind of(SchemaElement element) {
        if (element.isSetLogicalType()) {
            LogicalKind kind = fromLogicalType(element.getLogicalType());
            if (kind != null) {
                return kind;
            }
        }
        return element.isSetConverted_type() ? fromConvertedType(element.getConverted_type()) : NONE;
    }

    private static LogicalKind fromLogicalType(LogicalType type) {
        if (type.isSetSTRING()) {
            return STRING;
        } else if (type.isSetENUM()) {
            return ENUM;
        } else if (type.isSetJSON()) {
            return JSON;
        } else if (type.isSetDATE()) {
            return DATE;
        } else if (type.isSetDECIMAL()) {
            return DECIMAL;
        } else if (type.isSetTIME()) {
            return TIME;
        } else if (type.isSetTIMESTAMP()) {
            TimeUnit unit = type.getTIMESTAMP().getUnit();
            if (unit.isSetMILLIS()) {
                return TIMESTAMP_MILLIS;
            } else if (unit.isSetMICROS()) {
                return TIMESTAMP_MICROS;
            }
            return unit.isSetNANOS() ? TIMESTAMP_NANOS : OTHER;
        } else if (type.isSetINTEGER()) {
            int bits = type.getINTEGER().getBitWidth();
            boolean signed = type.getINTEGER().isIsSigned();
            return switch (bits) {
                case 8 -> signed ? INT_8 : UINT_8;
                case 16 -> signed ? INT_16 : UINT_16;
                case 32 -> signed ? INT_32 : UINT_32;
                case 64 -> signed ? INT_64 : UINT_64;
                default -> OTHER;
            };
        }
        // UNKNOWN (a column of nulls), UUID, geometry and the like are not annotations that the mapping knows
        return OTHER;
    }

    private static LogicalKind fromConvertedType(ConvertedType type) {
        return switch (type) {
            case UTF8 -> STRING;
            case ENUM -> ENUM;
            case JSON -> JSON;
            case DATE -> DATE;
            case TIMESTAMP_MILLIS -> TIMESTAMP_MILLIS;
            case TIMESTAMP_MICROS -> TIMESTAMP_MICROS;
            case INT_8 -> INT_8;
            case INT_16 -> INT_16;
            case INT_32 -> INT_32;
            case INT_64 -> INT_64;
            case UINT_8 -> UINT_8;
            case UINT_16 -> UINT_16;
            case UINT_32 -> UINT_32;
            case UINT_64 -> UINT_64;
            case DECIMAL -> DECIMAL;
            case TIME_MILLIS, TIME_MICROS -> TIME;
            default -> OTHER;
        };
    }
}

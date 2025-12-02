/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.common;

public enum TimestampComponentType {
    YEAR("Y"),
    MONTH("M"),
    DAY("D"),
    HOUR("h"),
    AM_PM("AP"),
    MINUTE("m"),
    SECOND("s"),
    MILLISECOND("ms"),
    MICROSECOND("us"),
    NANOSECOND("ns"),
    TIMEZONE_OFFSET_HOURS("TZh"),
    TIMEZONE_OFFSET_MINUTES("TZm"),
    TIMEZONE_OFFSET_HOURS_AND_MINUTES("TZhm"),
    NA("NA");

    // AM/PM indicator codes
    public static final int NO_AM_PM_CODE = 0;
    public static final int AM_CODE = 1;
    public static final int PM_CODE = 2;

    // enum instance codes (derived from ordinal)
    public static final int YEAR_CODE;
    public static final int MONTH_CODE;
    public static final int DAY_CODE;
    public static final int HOUR_CODE;
    public static final int AM_PM_CODE;
    public static final int MINUTE_CODE;
    public static final int SECOND_CODE;
    public static final int MILLISECOND_CODE;
    public static final int MICROSECOND_CODE;
    public static final int NANOSECOND_CODE;
    public static final int TIMEZONE_OFFSET_HOURS_CODE;
    public static final int TIMEZONE_OFFSET_MINUTES_CODE;
    public static final int TIMEZONE_OFFSET_HOURS_AND_MINUTES_CODE;
    public static final int NA_CODE;

    static {
        YEAR_CODE = YEAR.ordinal();
        MONTH_CODE = MONTH.ordinal();
        DAY_CODE = DAY.ordinal();
        HOUR_CODE = HOUR.ordinal();
        AM_PM_CODE = AM_PM.ordinal();
        MINUTE_CODE = MINUTE.ordinal();
        SECOND_CODE = SECOND.ordinal();
        MILLISECOND_CODE = MILLISECOND.ordinal();
        MICROSECOND_CODE = MICROSECOND.ordinal();
        NANOSECOND_CODE = NANOSECOND.ordinal();
        TIMEZONE_OFFSET_HOURS_CODE = TIMEZONE_OFFSET_HOURS.ordinal();
        TIMEZONE_OFFSET_MINUTES_CODE = TIMEZONE_OFFSET_MINUTES.ordinal();
        TIMEZONE_OFFSET_HOURS_AND_MINUTES_CODE = TIMEZONE_OFFSET_HOURS_AND_MINUTES.ordinal();
        NA_CODE = NA.ordinal();
    }

    private final int code;
    private final String symbol;

    TimestampComponentType(String symbol) {
        this.code = this.ordinal();
        this.symbol = symbol;
    }

    public int getCode() {
        return code;
    }

    public String getSymbol() {
        return symbol;
    }

    public static TimestampComponentType fromSymbol(String symbol) {
        for (TimestampComponentType tc : values()) {
            if (tc.symbol.equals(symbol)) {
                return tc;
            }
        }
        throw new IllegalArgumentException("Unknown timestamp component symbol: " + symbol);
    }
}

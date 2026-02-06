/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceAggregateNestedExpressionWithEval.inferTruncIntervalFromFormat;

public class DateFormatToTruncIntervalTests extends ESTestCase {

    public void testYear() {
        List<String> formats = List.of("y", "yy", "yyy", "yyyy", "yyyyyyyyyy", "u", "uu", "uuu", "uuuuuu");
        test(formats, "P1Y");
    }

    public void testQuarter() {
        List<String> formats = List.of("yyyy-Q", "yyyy-QQ", "yyyy-q", "yyyy-qq", "yyyy-qqq", "yyyy-qqqq");
        test(formats, "P3M");
    }

    public void testMonth() {
        List<String> formats = List.of(
            "yyyy-MM",
            "y-M",
            "u-L",
            "yyyy/MM",
            "y M",
            // Edge cases with different month patterns
            "yyyy-MMM",
            "yyyy-MMMM", // Text month names should still work
            "yyyy-LL",
            "yyyy-LLL",
            "yyyy-LLLL" // Standalone month names
        );
        test(formats, "P1M");
    }

    public void testDay() {
        List<String> formats = List.of(
            "yyyy-MM-dd",
            "y-M-d",
            "yyyy/MM/dd",
            "yyyy MM dd",
            // Edge cases with different day patterns
            "yyyy-MM-d",
            "yyyy-M-dd", // Mixed single/double digits
            // Different separators
            "yyyy_MM_dd",
            "yyyy:MM:dd",
            // Day of year (D) should map to day level but requires year and month context
            "yyyy-D",
            "y-DDD"
        );
        test(formats, "P1D");
    }

    public void testHour() {
        List<String> formats = List.of(
            "yyyy-MM-dd HH",
            "y-M-d H",
            "yyyy-MM-dd kk",
            "yyyy/MM/dd k",
            // Edge cases with different hour patterns
            "yyyy-MM-dd'T'HH", // ISO format with literal 'T'
            // Single digit hours
            "yyyy-MM-dd H",
            "yyyy-MM-dd k"
        );
        test(formats, "PT1H");
    }

    public void testMinute() {
        List<String> formats = List.of(
            "yyyy-MM-dd HH:mm",
            "y-M-d H:m",
            "yyyy-MM-dd kk:mm",
            // Edge cases with different minute patterns
            "yyyy-MM-dd'T'HH:mm", // ISO format with literal 'T'
            // Different separators
            "yyyy-MM-dd H.mm",
            "yyyy-MM-dd HH mm"
        );
        test(formats, "PT1M");
    }

    public void testSecond() {
        List<String> formats = List.of(
            "yyyy-MM-dd HH:mm:ss",
            "y-M-d H:m:s",
            "yyyy-MM-dd kk:mm:ss",
            // Complex valid patterns that map to second
            "yyyy-MM-dd'T'HH:mm:ss", // ISO format
            "yyyy/MM/dd HH:mm:ss",
            "yyyy.MM.dd.HH.mm.ss",
            // Edge cases with different second patterns
            "yyyy-MM-dd HH mm ss", // Space separators
            "yyyy-MM-dd'T'H:m:s", // ISO with single digits
            "yyyy-MM-dd HH.mm.ss" // Mixed separators
        );
        test(formats, "PT1S");
    }

    public void testMillisecond() {
        // Millisecond of day (A) should map to millisecond level
        List<String> formats = List.of("yyyy-MM-dd A");
        test(formats, "PT0.001S");
    }

    public void testUnsupportedPatterns() {
        // Test patterns that should return null due to unsupported fields
        List<String> unsupportedFormats = List.of(
            // ERA
            "G yyyy",
            "GGGG yyyy",
            // Quarter
            "yyyy-Q-h",
            "yyyy-QQ-d",
            // Week fields
            "yyyy-w",
            "yyyy-ww",
            "yyyy-W",
            // Day of week
            "yyyy-MM-dd E",
            "yyyy-MM-dd EEEE",
            "yyyy-MM-dd c",
            "yyyy-MM-dd e",
            // AM/PM
            "yyyy-MM-dd a",
            // 12-hour formats
            "yyyy-MM-dd h:mm",
            "yyyy-MM-dd K:mm",
            // Nanoseconds
            "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss.n",
            "yyyy-MM-dd N",
            // Timezone fields
            "yyyy-MM-dd HH:mm:ss z",
            "yyyy-MM-dd HH:mm:ss Z",
            "yyyy-MM-dd HH:mm:ss X",
            "yyyy-MM-dd HH:mm:ss VV",
            "yyyy-MM-dd HH:mm:ss O",
            // Day period
            "yyyy-MM-dd HH:mm B",
            // Modified Julian Day
            "g",
            // Non-continuous hierarchy patterns
            "yyyy-dd", // year + day (skips month)
            "yyyy HH", // year + hour (skips month and day)
            "MM-dd", // month + day (skips year)
            "HH:mm", // hour + minute (skips year, month, day)
            "yyyy mm", // year + minute (skips month, day, hour)
            "DD", // day of year only (missing year context)
            "mm MM DD", // invalid order/hierarchy
            // Invalid patterns
            "invalid",
            "xyz",
            "yyyy-MM-dd-invalid",
            "" // empty string
        );

        for (String format : unsupportedFormats) {
            assertNull("Format '" + format + "' should return null", inferTruncIntervalFromFormat(format, Source.EMPTY));
        }
    }

    private static void test(List<String> formats, String expected) {
        for (String format : formats) {
            Literal literal = inferTruncIntervalFromFormat(format, Source.EMPTY);
            assertEquals("Format '" + format + "' should return " + expected, expected, Objects.requireNonNull(literal).toString());
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.time;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.time.Instant;
import java.time.ZoneId;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.time.EpochTime.MILLIS_FORMATTER;
import static org.hamcrest.Matchers.is;

public class EpochTimeTests extends ESTestCase {

    public void testNegativeEpochMillis() {
        DateFormatter formatter = MILLIS_FORMATTER;

        // validate that negative epoch millis around rounded appropriately by the parser
        LongSupplier supplier = () -> 0L;
        {
            Instant instant = formatter.toDateMathParser().parse("0", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("0", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("1", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.001999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-1", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("1", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.001Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-1", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("0.999999", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.999999", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999000001Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("0.999999", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.999999", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999000001Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("6250000430768", supplier, true, ZoneId.of("UTC"));
            assertEquals("2168-01-20T23:13:50.768999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-6250000430768", supplier, true, ZoneId.of("UTC"));
            assertEquals("1771-12-12T00:46:09.232999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("6250000430768", supplier, false, ZoneId.of("UTC"));
            assertEquals("2168-01-20T23:13:50.768Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-6250000430768", supplier, false, ZoneId.of("UTC"));
            assertEquals("1771-12-12T00:46:09.232Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("0.123450", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000123450Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.123450", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999876550Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("0.123450", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000123450Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.123450", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999876550Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("0.123456", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000123456Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.123456", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999876544Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("0.123456", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000123456Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.123456", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999876544Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("86400000", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-02T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-86400000", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("86400000", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-02T00:00:00Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-86400000", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T00:00:00Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("86400000.999999", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-02T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-86400000.999999", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-30T23:59:59.999000001Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("86400000.999999", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-02T00:00:00.000999999Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-86400000.999999", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-30T23:59:59.999000001Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("200.89", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.200890Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-200.89", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.799110Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("200.89", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.200890Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-200.89", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.799110Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("200.", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.200Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-200.", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.800Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("200.", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.200Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-200.", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.800Z", instant.toString());
        }

        {
            Instant instant = formatter.toDateMathParser().parse("0.200", supplier, true, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000200Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.200", supplier, true, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999800Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("0.200", supplier, false, ZoneId.of("UTC"));
            assertEquals("1970-01-01T00:00:00.000200Z", instant.toString());
        }
        {
            Instant instant = formatter.toDateMathParser().parse("-0.200", supplier, false, ZoneId.of("UTC"));
            assertEquals("1969-12-31T23:59:59.999800Z", instant.toString());
        }

        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse(".200", supplier, true, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [.200] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("-.200", supplier, true, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [-.200] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse(".200", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [.200] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("-.200", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [-.200] with format [epoch_millis]"));
        }

        // tilda was included in the parsers at one point for delineating negative and positive infinity rounding and we want to
        // ensure it doesn't show up unexpectedly in the parser with its original "~" value
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~-0.200", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~-0.200] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~0.200", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~0.200] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~-1", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~-1] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~1", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~1] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~-1.", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~-1.] with format [epoch_millis]"));
        }
        {
            ElasticsearchParseException e = expectThrows(
                ElasticsearchParseException.class,
                () -> formatter.toDateMathParser().parse("~1.", supplier, false, ZoneId.of("UTC"))
            );
            assertThat(e.getMessage().split(":")[0], is("failed to parse date field [~1.] with format [epoch_millis]"));
        }
    }

}

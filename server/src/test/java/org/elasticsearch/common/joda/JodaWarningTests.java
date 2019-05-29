/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTime;

import java.util.Locale;

import static org.hamcrest.Matchers.is;

public class JodaWarningTests extends ESTestCase {

    @Override
    protected boolean enableJodaDeprecationWarningsCheck() {
        return true;
    }

    public void testDeprecatedFormatSpecifiers() {
        Joda.forPattern("CC");
        assertWarnings("'C' century of era is no longer supported. Prefix your date format with '8' to use the new specifier.");
        Joda.forPattern("YYYY");
        assertWarnings("'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year." +
            " Prefix your date format with '8' to use the new specifier.");
        Joda.forPattern("xxxx");
        assertWarnings("'x' weak-year should be replaced with 'Y'. Use 'x' for zone-offset. " +
            "Prefix your date format with '8' to use the new specifier.");
        // multiple deprecations - one field combines warnings
        Joda.forPattern("CC-YYYY");
        assertWarnings("'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.; " +
            "'C' century of era is no longer supported. " +
            "Prefix your date format with '8' to use the new specifier.");
    }

    public void testDeprecatedEpochScientificNotation() {
        assertValidDateFormatParsing("epoch_second", "1.234e5", "123400");
        assertWarnings("Use of scientific notation" +
            " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
        assertValidDateFormatParsing("epoch_millis", "1.234e5", "123400");
        assertWarnings("Use of scientific notation" +
            " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
    }

    public void testThatNegativeEpochsCanBeParsed() {
        // problem: negative epochs can be arbitrary in size...
        boolean parseMilliSeconds = randomBoolean();
        DateFormatter formatter = DateFormatter.forPattern(parseMilliSeconds ? "epoch_millis" : "epoch_second");
        DateTime dateTime = formatter.parseJoda("-10000");

        assertThat(dateTime.getYear(), is(1969));
        assertThat(dateTime.getMonthOfYear(), is(12));
        assertThat(dateTime.getDayOfMonth(), is(31));
        if (parseMilliSeconds) {
            assertThat(dateTime.getHourOfDay(), is(23)); // utc timezone, +2 offset due to CEST
            assertThat(dateTime.getMinuteOfHour(), is(59));
            assertThat(dateTime.getSecondOfMinute(), is(50));
        } else {
            assertThat(dateTime.getHourOfDay(), is(21)); // utc timezone, +2 offset due to CEST
            assertThat(dateTime.getMinuteOfHour(), is(13));
            assertThat(dateTime.getSecondOfMinute(), is(20));
        }

        // test floats get truncated
        String epochFloatValue = String.format(Locale.US, "%d.%d", dateTime.getMillis() / (parseMilliSeconds ? 1L : 1000L),
            randomNonNegativeLong());
        assertThat(formatter.parseJoda(epochFloatValue).getMillis(), is(dateTime.getMillis()));

        // every negative epoch must be parsed, no matter if exact the size or bigger
        if (parseMilliSeconds) {
            formatter.parseJoda("-100000000");
            formatter.parseJoda("-999999999999");
            formatter.parseJoda("-1234567890123");
            formatter.parseJoda("-1234567890123456789");

            formatter.parseJoda("-1234567890123.9999");
            formatter.parseJoda("-1234567890123456789.9999");
        } else {
            formatter.parseJoda("-100000000");
            formatter.parseJoda("-1234567890");
            formatter.parseJoda("-1234567890123456");

            formatter.parseJoda("-1234567890.9999");
            formatter.parseJoda("-1234567890123456.9999");
        }

        assertWarnings("Use of negative values" +
            " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
    }

    public void testDeprecatedEpochNegative() {
        assertValidDateFormatParsing("epoch_second", "-12345", "-12345");
        assertWarnings("Use of negative values" +
            " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
        assertValidDateFormatParsing("epoch_millis", "-12345", "-12345");
        assertWarnings("Use of negative values" +
            " in epoch time formats is deprecated and will not be supported in the next major version of Elasticsearch.");
    }

    private void assertValidDateFormatParsing(String pattern, String dateToParse, String expectedDate) {
        DateFormatter formatter = DateFormatter.forPattern(pattern);
        assertThat(formatter.formatMillis(formatter.parseMillis(dateToParse)), is(expectedDate));
    }
}

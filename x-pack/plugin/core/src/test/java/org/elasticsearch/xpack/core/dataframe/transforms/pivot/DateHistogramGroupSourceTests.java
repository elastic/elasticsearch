/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.stream.Stream;

public class DateHistogramGroupSourceTests extends AbstractSerializingTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource;
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.FixedInterval(
                    new DateHistogramInterval(randomPositiveTimeValue())));
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(field, new DateHistogramGroupSource.CalendarInterval(
                    new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w"))));
        }

        if (randomBoolean()) {
            dateHistogramGroupSource.setTimeZone(randomZone());
        }
        if (randomBoolean()) {
            dateHistogramGroupSource.setFormat(randomAlphaOfLength(10));
        }
        return dateHistogramGroupSource;
    }

    @Override
    protected DateHistogramGroupSource doParseInstance(XContentParser parser) throws IOException {
        return DateHistogramGroupSource.fromXContent(parser, false);
    }

    @Override
    protected DateHistogramGroupSource createTestInstance() {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected Reader<DateHistogramGroupSource> instanceReader() {
        return DateHistogramGroupSource::new;
    }

    public void testIsInvalidFormatQuarter() {
        String invalidFormat = "yyyy";
        String validFormat = "yyyy-MM";
        // Any quarterly interval greater than 1 is an invalid date_histogram interval
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(1).map(v -> v + "q"));
    }

    public void testIsInvalidFormatMonth() {
        String invalidFormat = "yyyy";
        String validFormat = "yyyy-MM";
        // Any monthly interval greater than 1 is an invalid date_histogram interval
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(1).map(v -> v + "M"));
    }

    public void testIsInvalidFormatDay() {
        String invalidFormat = "yyyy-MM";
        String validFormat = "yyyy-MM-dd";
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(10_000).map(v -> v + "d"));
    }

    public void testIsInvalidFormatHour() {
        String invalidFormat = "yyyy-MM-dd";
        String validFormat = "yyyy-MM-dd HH";
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(10_000).map(v -> v + "h"));
    }

    public void testIsInvalidFormatMinute() {
        String invalidFormat = "yyyy-MM-dd HH:00";
        String validFormat = "yyyy-MM-dd HH:mm";
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(10_000).map(v -> v + "m"));
    }

    public void testIsInvalidFormatSecond() {
        String invalidFormat = "yyyy-MM-dd HH:mm:00";
        String validFormat = "yyyy-MM-dd HH:mm:ss";
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(10_000).map(v -> v + "s"));
    }

    public void testIsInvalidFormatMilliSecond() {
        String invalidFormat = "yyyy-MM-dd HH:mm:ss";
        String validFormat = "yyyy-MM-dd HH:mm:ss.SSS";
        testFormatting(validFormat, invalidFormat, Stream.iterate(1, n -> n + 1).limit(10_000).map(v -> v + "ms"));
    }

    private void testFormatting(String validFormat, String invalidFormat, Stream<String> dateHistogramIntervalExpressionsToTest) {
        dateHistogramIntervalExpressionsToTest.forEach(expression -> {
            assertTrue("Invalid format [" + invalidFormat + "] considered valid for interval [" + expression + "]",
                DateHistogramGroupSource.isInvalidFormat(invalidFormat, new DateHistogramInterval(expression)));
            assertFalse("Valid format [" + validFormat + "] considered invalid for interval [" + expression + "]",
                DateHistogramGroupSource.isInvalidFormat(validFormat, new DateHistogramInterval(expression)));
        });
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;

import static org.hamcrest.Matchers.equalTo;

public class DateHistogramGroupSourceTests extends AbstractSerializingTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = randomBoolean() ? null : ScriptConfigTests.randomScriptConfig();
        DateHistogramGroupSource dateHistogramGroupSource;
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(randomTimeValue(1, 100, "d", "h", "ms", "s", "m"))),
                randomBoolean() ? randomZone() : null
            );
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                new DateHistogramGroupSource.CalendarInterval(
                    new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w", "M", "q", "y"))
                ),
                randomBoolean() ? randomZone() : null
            );
        }

        return dateHistogramGroupSource;
    }

    public void testBackwardsSerialization() throws IOException {
        DateHistogramGroupSource groupSource = randomDateHistogramGroupSource();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_2_0);
            groupSource.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_2_0);
                DateHistogramGroupSource streamedGroupSource = new DateHistogramGroupSource(in);
                assertEquals(groupSource, streamedGroupSource);
            }
        }
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

    public void testRoundingDateHistogramFixedInterval() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            field,
            null,
            new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
            null
        );

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertNotNull(dateHistogramGroupSource.getRounding());

        assertThat(
            dateHistogramGroupSource.getRounding().round(time("2020-03-26T23:59:59.000Z")),
            equalTo(time("2020-03-26T00:00:00.000Z"))
        );
        assertThat(
            dateHistogramGroupSource.getRounding().round(time("2020-03-26T00:00:01.000Z")),
            equalTo(time("2020-03-26T00:00:00.000Z"))
        );
    }

    public void testRoundingDateHistogramCalendarInterval() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            field,
            null,
            new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval("1w")),
            null
        );

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertNotNull(dateHistogramGroupSource.getRounding());

        assertThat(
            dateHistogramGroupSource.getRounding().round(time("2020-03-26T23:59:59.000Z")),
            equalTo(time("2020-03-23T00:00:00.000Z"))
        );
        assertThat(
            dateHistogramGroupSource.getRounding().round(time("2020-03-29T23:59:59.000Z")),
            equalTo(time("2020-03-23T00:00:00.000Z"))
        );
        assertThat(
            dateHistogramGroupSource.getRounding().round(time("2020-03-23T00:00:01.000Z")),
            equalTo(time("2020-03-23T00:00:00.000Z"))
        );
    }

    private static long time(String time) {
        TemporalAccessor accessor = DateFormatter.forPattern("date_optional_time").withZone(ZoneOffset.UTC).parse(time);
        return DateFormatters.from(accessor).toInstant().toEpochMilli();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DateHistogramGroupSourceTests extends AbstractXContentSerializingTestCase<DateHistogramGroupSource> {

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        return randomDateHistogramGroupSource(Version.CURRENT);
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSourceNoScript() {
        return randomDateHistogramGroupSource(Version.CURRENT, false);
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSourceNoScript(String fieldPrefix) {
        return randomDateHistogramGroupSource(Version.CURRENT, false, fieldPrefix);
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource(Version version) {
        return randomDateHistogramGroupSource(version, randomBoolean());
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource(Version version, boolean withScript) {
        return randomDateHistogramGroupSource(version, withScript, "");
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource(Version version, boolean withScript, String fieldPrefix) {
        ScriptConfig scriptConfig = null;
        String field;

        // either a field or a script must be specified, it's possible to have both, but disallowed to have none
        if (version.onOrAfter(Version.V_7_7_0) && withScript) {
            scriptConfig = ScriptConfigTests.randomScriptConfig();
            field = randomBoolean() ? null : fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        } else {
            field = fieldPrefix + randomAlphaOfLengthBetween(1, 20);
        }
        boolean missingBucket = version.onOrAfter(Version.V_7_10_0) ? randomBoolean() : false;
        Long offset = version.onOrAfter(Version.V_8_7_0) ? randomOffset() : null;

        DateHistogramGroupSource dateHistogramGroupSource;
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                missingBucket,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(randomTimeValue(1, 100, "d", "h", "ms", "s", "m"))),
                randomBoolean() ? randomZone() : null,
                randomBoolean() ? offset : null
            );
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                missingBucket,
                new DateHistogramGroupSource.CalendarInterval(
                    new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w", "M", "q", "y"))
                ),
                randomBoolean() ? randomZone() : null,
                randomBoolean() ? offset : null
            );
        }

        return dateHistogramGroupSource;
    }

    public void testBackwardsSerialization72() throws IOException {
        // version 7.7 introduced scripts, so test before that
        DateHistogramGroupSource groupSource = randomDateHistogramGroupSource(
            VersionUtils.randomVersionBetween(random(), Version.V_7_3_0, Version.V_7_6_2)
        );

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
    protected DateHistogramGroupSource mutateInstance(DateHistogramGroupSource instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<DateHistogramGroupSource> instanceReader() {
        return DateHistogramGroupSource::new;
    }

    public void testOffset() {
        {
            DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
                null,
                null,
                false,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
                null,
                null
            );
            assertThat(dateHistogramGroupSource.getOffset(), equalTo(0L));
        }
        {
            DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
                null,
                null,
                false,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
                null,
                0L
            );
            assertThat(dateHistogramGroupSource.getOffset(), equalTo(0L));
        }
        {
            DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
                null,
                null,
                false,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
                null,
                DateHistogramAggregationBuilder.parseStringOffset("-1h")
            );
            assertThat(dateHistogramGroupSource.getOffset(), equalTo(-3_600_000L));
        }
        {
            DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
                null,
                null,
                false,
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
                null,
                DateHistogramAggregationBuilder.parseStringOffset("+1h")
            );
            assertThat(dateHistogramGroupSource.getOffset(), equalTo(3_600_000L));
        }
    }

    public void testRoundingDateHistogramFixedInterval() {
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20),
            null,
            randomBoolean(),
            new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval("1d")),
            null,
            null
        );

        Rounding.Prepared rounding = dateHistogramGroupSource.getRounding();
        assertThat(rounding, notNullValue());

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertThat(rounding.round(time("2020-03-25T23:59:59.000Z")), equalTo(time("2020-03-25T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T00:00:00.000Z")), equalTo(time("2020-03-26T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T00:00:01.000Z")), equalTo(time("2020-03-26T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T23:59:59.000Z")), equalTo(time("2020-03-26T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-27T00:00:00.000Z")), equalTo(time("2020-03-27T00:00:00.000Z")));
    }

    public void testRoundingDateHistogramCalendarInterval() {
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20),
            null,
            randomBoolean(),
            new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval("1w")),
            null,
            null
        );

        Rounding.Prepared rounding = dateHistogramGroupSource.getRounding();
        assertThat(rounding, notNullValue());

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertThat(rounding.round(time("2020-03-21T23:59:59.000Z")), equalTo(time("2020-03-16T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T00:00:00.000Z")), equalTo(time("2020-03-16T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T23:59:59.000Z")), equalTo(time("2020-03-16T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:00.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:01.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-24T00:00:00.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T23:59:59.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-28T23:59:59.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T00:00:00.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T23:59:59.000Z")), equalTo(time("2020-03-23T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-30T00:00:00.000Z")), equalTo(time("2020-03-30T00:00:00.000Z")));
    }

    public void testRoundingDateHistogramCalendarIntervalWithNegativeOffset() {
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20),
            null,
            randomBoolean(),
            new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval("1w")),
            null,
            DateHistogramAggregationBuilder.parseStringOffset("-1d")
        );

        Rounding.Prepared rounding = dateHistogramGroupSource.getRounding();
        assertThat(rounding, notNullValue());

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertThat(rounding.round(time("2020-03-21T23:59:59.000Z")), equalTo(time("2020-03-15T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T00:00:00.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T23:59:59.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:00.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:01.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-24T00:00:00.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T23:59:59.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-28T23:59:59.000Z")), equalTo(time("2020-03-22T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T00:00:00.000Z")), equalTo(time("2020-03-29T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T23:59:59.000Z")), equalTo(time("2020-03-29T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-30T00:00:00.000Z")), equalTo(time("2020-03-29T00:00:00.000Z")));
    }

    public void testRoundingDateHistogramCalendarIntervalWithPositiveOffset() {
        DateHistogramGroupSource dateHistogramGroupSource = new DateHistogramGroupSource(
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20),
            null,
            randomBoolean(),
            new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval("1w")),
            null,
            DateHistogramAggregationBuilder.parseStringOffset("+1d")
        );

        Rounding.Prepared rounding = dateHistogramGroupSource.getRounding();
        assertThat(rounding, notNullValue());

        // not meant to be complete rounding tests, see {@link RoundingTests} for more
        assertThat(rounding.round(time("2020-03-21T23:59:59.000Z")), equalTo(time("2020-03-17T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T00:00:00.000Z")), equalTo(time("2020-03-17T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-22T23:59:59.000Z")), equalTo(time("2020-03-17T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:00.000Z")), equalTo(time("2020-03-17T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-23T00:00:01.000Z")), equalTo(time("2020-03-17T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-24T00:00:00.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-26T23:59:59.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-28T23:59:59.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T00:00:00.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-29T23:59:59.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
        assertThat(rounding.round(time("2020-03-30T00:00:00.000Z")), equalTo(time("2020-03-24T00:00:00.000Z")));
    }

    private static long time(String time) {
        TemporalAccessor accessor = DateFormatter.forPattern("date_optional_time").withZone(ZoneOffset.UTC).parse(time);
        return DateFormatters.from(accessor).toInstant().toEpochMilli();
    }

    private static long randomOffset() {
        return randomLongBetween(-1_000_000, 1_000_000);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup.job.config;

import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DateHistogramGroupConfigTests extends AbstractXContentTestCase<DateHistogramGroupConfig> {

    @Override
    protected DateHistogramGroupConfig createTestInstance() {
        return randomDateHistogramGroupConfig();
    }

    @Override
    protected DateHistogramGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return DateHistogramGroupConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testValidateNullField() {
        final DateHistogramGroupConfig config = new DateHistogramGroupConfig(null, DateHistogramInterval.DAY, null, null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Field name is required")));
    }

    public void testValidateEmptyField() {
        final DateHistogramGroupConfig config = new DateHistogramGroupConfig("", DateHistogramInterval.DAY, null, null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Field name is required")));
    }

    public void testValidateNullInterval() {
        final DateHistogramGroupConfig config = new DateHistogramGroupConfig("field", null, null, null);

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(true));
        ValidationException validationException = validation.get();
        assertThat(validationException.validationErrors().size(), is(1));
        assertThat(validationException.validationErrors(), contains(is("Interval is required")));
    }

    public void testValidate() {
        final DateHistogramGroupConfig config = randomDateHistogramGroupConfig();

        Optional<ValidationException> validation = config.validate();
        assertThat(validation, notNullValue());
        assertThat(validation.isPresent(), is(false));
    }

    static DateHistogramGroupConfig randomDateHistogramGroupConfig() {
        final String field = randomAlphaOfLength(randomIntBetween(3, 10));
        final DateHistogramInterval delay = randomBoolean() ? new DateHistogramInterval(randomPositiveTimeValue()) : null;
        final String timezone = randomBoolean() ? randomDateTimeZone().toString() : null;
        int i = randomIntBetween(0,2);
        final DateHistogramInterval interval;
        switch (i) {
            case 0:
                interval = new DateHistogramInterval(randomPositiveTimeValue());
                return new DateHistogramGroupConfig.FixedInterval(field, interval, delay, timezone);
            case 1:
                interval = new DateHistogramInterval(randomTimeValue(1,1, "m", "h", "d", "w"));
                return new DateHistogramGroupConfig.CalendarInterval(field, interval, delay, timezone);
            default:
                interval = new DateHistogramInterval(randomPositiveTimeValue());
                return new DateHistogramGroupConfig(field, interval, delay, timezone);
        }

    }
}

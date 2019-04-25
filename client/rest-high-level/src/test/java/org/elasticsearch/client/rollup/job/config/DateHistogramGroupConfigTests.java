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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomDateHistogramGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DateHistogramGroupConfigSerializingTests extends AbstractSerializingTestCase<DateHistogramGroupConfig> {

    private enum DateHistoType {
        FIXED,
        CALENDAR
    }

    private static DateHistoType type;

    @Override
    protected DateHistogramGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return DateHistogramGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DateHistogramGroupConfig> instanceReader() {
        if (type.equals(DateHistoType.FIXED)) {
            return DateHistogramGroupConfig.FixedInterval::new;
        } else if (type.equals(DateHistoType.CALENDAR)) {
            return DateHistogramGroupConfig.CalendarInterval::new;
        }
        throw new IllegalStateException("Illegal date histogram legacy interval");
    }

    @Override
    protected DateHistogramGroupConfig createTestInstance() {
        DateHistogramGroupConfig config = randomDateHistogramGroupConfig(random());
        if (config.getClass().equals(DateHistogramGroupConfig.FixedInterval.class)) {
            type = DateHistoType.FIXED;
        } else if (config.getClass().equals(DateHistogramGroupConfig.CalendarInterval.class)) {
            type = DateHistoType.CALENDAR;
        } else {
            throw new IllegalStateException("Illegal date histogram legacy interval");
        }
        return config;
    }

    public void testValidateNoMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "Could not find one of [date,date_nanos] fields with name [my_field] in " + "any of the indices matching the index pattern."
            )
        );
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "Could not find one of [date,date_nanos] fields with name [my_field] in " + "any of the indices matching the index pattern."
            )
        );
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "The field referenced by a date_histo group must be one of type "
                    + "[date,date_nanos] across all indices in the index pattern.  Found: [keyword] for field [my_field]"
            )
        );
    }

    public void testValidateFieldMixtureTypes() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        Map<String, FieldCapabilities> types = Maps.newMapWithExpectedSize(2);
        types.put("date", fieldCaps);
        types.put("keyword", fieldCaps);
        responseMap.put("my_field", types);

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "The field referenced by a date_histo group must be one of type "
                    + "[date,date_nanos] across all indices in the index pattern.  Found: [date, keyword] for field [my_field]"
            )
        );
    }

    public void testValidateFieldMatchingNotAggregatable() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1d"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }

    public void testValidateWeek() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        DateHistogramGroupConfig config = new DateHistogramGroupConfig.CalendarInterval(
            "my_field",
            new DateHistogramInterval("1w"),
            null,
            null
        );
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }

    /**
     * Tests that a DateHistogramGroupConfig can be serialized/deserialized correctly after
     * the timezone was changed from DateTimeZone to String.
     */
    public void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            final DateHistogramGroupConfig reference = ConfigTestHelpers.randomDateHistogramGroupConfig(random());

            final BytesStreamOutput out = new BytesStreamOutput();
            reference.writeTo(out);

            // previous way to deserialize a DateHistogramGroupConfig
            final StreamInput in = out.bytes().streamInput();
            DateHistogramInterval interval = new DateHistogramInterval(in);
            String field = in.readString();
            DateHistogramInterval delay = in.readOptionalWriteable(DateHistogramInterval::new);
            ZoneId timeZone = in.readZoneId();

            if (reference instanceof DateHistogramGroupConfig.FixedInterval) {
                assertEqualInstances(reference, new DateHistogramGroupConfig.FixedInterval(field, interval, delay, timeZone.getId()));
            } else if (reference instanceof DateHistogramGroupConfig.CalendarInterval) {
                assertEqualInstances(reference, new DateHistogramGroupConfig.CalendarInterval(field, interval, delay, timeZone.getId()));
            } else {
                fail("And you may ask yourself, how did I get here?");
            }
        }

        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            final String field = ConfigTestHelpers.randomField(random());
            final DateHistogramInterval interval = ConfigTestHelpers.randomInterval();
            final DateHistogramInterval delay = randomBoolean() ? ConfigTestHelpers.randomInterval() : null;
            final ZoneId timezone = randomZone();

            // previous way to serialize a DateHistogramGroupConfig
            final BytesStreamOutput out = new BytesStreamOutput();
            interval.writeTo(out);
            out.writeString(field);
            out.writeOptionalWriteable(delay);
            out.writeZoneId(timezone);

            final StreamInput in = out.bytes().streamInput();
            DateHistogramGroupConfig deserialized = new DateHistogramGroupConfig.FixedInterval(in);

            assertEqualInstances(new DateHistogramGroupConfig.FixedInterval(field, interval, delay, timezone.getId()), deserialized);
        }
    }

    /**
     * Tests that old DateHistogramGroupConfigs can be serialized/deserialized
     * into the specialized Fixed/Calendar versions
     */
    public void testLegacyConfigBWC() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            // Serialize the old format
            final DateHistogramGroupConfig reference = ConfigTestHelpers.randomDateHistogramGroupConfig(random());

            final BytesStreamOutput out = new BytesStreamOutput();
            reference.writeTo(out);
            final StreamInput in = out.bytes().streamInput();

            // Deserialize the new format
            DateHistogramGroupConfig test = DateHistogramGroupConfig.fromUnknownTimeUnit(in);

            assertThat(reference.getInterval(), equalTo(test.getInterval()));
            assertThat(reference.getField(), equalTo(test.getField()));
            assertThat(reference.getTimeZone(), equalTo(test.getTimeZone()));
            assertThat(reference.getDelay(), equalTo(test.getDelay()));
        }

        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            // Serialize the new format
            final DateHistogramGroupConfig reference = ConfigTestHelpers.randomDateHistogramGroupConfig(random());

            final BytesStreamOutput out = new BytesStreamOutput();
            reference.writeTo(out);
            final StreamInput in = out.bytes().streamInput();

            // Deserialize the old format
            DateHistogramGroupConfig test = new DateHistogramGroupConfig.FixedInterval(in);

            assertThat(reference.getInterval(), equalTo(test.getInterval()));
            assertThat(reference.getField(), equalTo(test.getField()));
            assertThat(reference.getTimeZone(), equalTo(test.getTimeZone()));
            assertThat(reference.getDelay(), equalTo(test.getDelay()));
        }
    }
}

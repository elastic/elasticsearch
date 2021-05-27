/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupActionDateHistogramGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupActionDateHistogramGroupConfigSerializingTests
        extends AbstractSerializingTestCase<RollupActionDateHistogramGroupConfig> {

    @Override
    protected RollupActionDateHistogramGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupActionDateHistogramGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupActionDateHistogramGroupConfig> instanceReader() {
        return RollupActionDateHistogramGroupConfig::readFrom;
    }

    @Override
    protected RollupActionDateHistogramGroupConfig createTestInstance() {
        return randomRollupActionDateHistogramGroupConfig(random());
    }

    public void testValidateNoMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find one of [date,date_nanos] fields with name [my_field]."));
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("date", fieldCaps));

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find one of [date,date_nanos] fields with name [my_field]."));
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a date_histo group must be one of type " +
            "[date,date_nanos]. Found: [keyword] for field [my_field]"));
    }

    public void testValidateFieldMixtureTypes() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        Map<String, FieldCapabilities> types = new HashMap<>(2);
        types.put("date", fieldCaps);
        types.put("keyword", fieldCaps);
        responseMap.put("my_field", types);

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a date_histo group must be one of type " +
            "[date,date_nanos]. Found: [date, keyword] for field [my_field]"));
    }

    public void testValidateFieldMatchingNotAggregatable() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        RollupActionDateHistogramGroupConfig config =new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable, but is not."));
    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1d"));
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

        RollupActionDateHistogramGroupConfig config = new RollupActionDateHistogramGroupConfig.CalendarInterval("my_field",
            new DateHistogramInterval("1w"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }
}

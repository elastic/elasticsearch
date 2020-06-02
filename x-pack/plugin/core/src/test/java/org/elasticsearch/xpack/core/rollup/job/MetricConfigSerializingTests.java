/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricConfigSerializingTests extends AbstractSerializingTestCase<MetricConfig> {

    @Override
    protected MetricConfig doParseInstance(final XContentParser parser) throws IOException {
        return MetricConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<MetricConfig> instanceReader() {
        return MetricConfig::new;
    }

    @Override
    protected MetricConfig createTestInstance() {
        return ConfigTestHelpers.randomMetricConfig(random());
    }

    public void testValidateNoMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [date] field with name [my_field] in any" +
            " of the indices matching the index pattern."));
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("date", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [date] field with name [my_field] in any" +
            " of the indices matching the index pattern."));
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat("The field referenced by a metric group must be a [numeric] or [date] type," +
            " but found [keyword] for field [my_field]", is(in(e.validationErrors())));
    }

    public void testValidateFieldMatchingNotAggregatable() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap("long", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateDateFieldUnsupportedMetric() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", Arrays.asList("avg", "max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Only the metrics " + RollupField.SUPPORTED_DATE_METRICS.toString() +
            " are supported for [date] types, but unsupported metrics [avg] supplied for field [my_field]"));
    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("long", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));


        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("double", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("float", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("short", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("byte", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("half_float", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("scaled_float", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("integer", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));

        fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap("date", fieldCaps));
        config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }

}

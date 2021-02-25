/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [date,date_nanos] field with name [my_field] " +
            "in any of the indices matching the index pattern."));
    }

    public void testValidateNoMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("date", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [date,date_nanos] field with name [my_field] " +
            "in any of the indices matching the index pattern."));
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        MetricConfig config = new MetricConfig("my_field", singletonList("max"));
        config.validateMappings(responseMap, e);
        assertThat("The field referenced by a metric group must be a [numeric] or [date,date_nanos] type," +
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

    public void testValidateDateFieldsUnsupportedMetric() {
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        for (String mappingType : RollupField.DATE_FIELD_MAPPER_TYPES) {
            // Have to mock fieldcaps because the ctor's aren't public...
            FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
            when(fieldCaps.isAggregatable()).thenReturn(true);
            responseMap.put("my_field", Collections.singletonMap(mappingType, fieldCaps));

            Set<String> unsupportedMetrics = new HashSet<>(RollupField.SUPPORTED_METRICS);
            unsupportedMetrics.removeAll(RollupField.SUPPORTED_DATE_METRICS);
            for (String unsupportedMetric : unsupportedMetrics) {
                MetricConfig config = new MetricConfig("my_field", Collections.singletonList(unsupportedMetric));
                ActionRequestValidationException e = new ActionRequestValidationException();
                config.validateMappings(responseMap, e);
                assertThat(e.validationErrors().get(0), equalTo("Only the metrics " + RollupField.SUPPORTED_DATE_METRICS.toString() +
                    " are supported for [" + mappingType + "] types, but unsupported metrics [" + unsupportedMetric +
                    "] supplied for field [my_field]"));
            }
        }

    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        for (String numericType : RollupField.NUMERIC_FIELD_MAPPER_TYPES) {
            // Have to mock fieldcaps because the ctor's aren't public...
            FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
            when(fieldCaps.isAggregatable()).thenReturn(true);
            responseMap.put("my_field", Collections.singletonMap(numericType, fieldCaps));
            MetricConfig config = ConfigTestHelpers
                .randomMetricConfigWithFieldAndMetrics(random(), "my_field", RollupField.SUPPORTED_NUMERIC_METRICS);
            config.validateMappings(responseMap, e);
            assertThat(e.validationErrors().size(), equalTo(0));
        }

        for (String dateType : RollupField.DATE_FIELD_MAPPER_TYPES) {
            // Have to mock fieldcaps because the ctor's aren't public...
            FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
            when(fieldCaps.isAggregatable()).thenReturn(true);
            responseMap.put("my_field", Collections.singletonMap(dateType, fieldCaps));
            MetricConfig config = ConfigTestHelpers
                .randomMetricConfigWithFieldAndMetrics(random(), "my_field", RollupField.SUPPORTED_DATE_METRICS);
            config.validateMappings(responseMap, e);
            assertThat(e.validationErrors().size(), equalTo(0));
        }
    }

}

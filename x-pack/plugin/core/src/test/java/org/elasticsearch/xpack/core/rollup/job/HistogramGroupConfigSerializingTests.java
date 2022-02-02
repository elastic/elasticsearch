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
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomHistogramGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HistogramGroupConfigSerializingTests extends AbstractSerializingTestCase<HistogramGroupConfig> {

    @Override
    protected HistogramGroupConfig doParseInstance(XContentParser parser) throws IOException {
        return HistogramGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<HistogramGroupConfig> instanceReader() {
        return HistogramGroupConfig::new;
    }

    @Override
    protected HistogramGroupConfig createTestInstance() {
        return randomHistogramGroupConfig(random());
    }

    public void testValidateNoMapping() throws IOException {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        HistogramGroupConfig config = new HistogramGroupConfig(132, "my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo("Could not find a [numeric] field with name [my_field] in any of the " + "indices matching the index pattern.")
        );
    }

    public void testValidateNomatchingField() throws IOException {

        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("long", fieldCaps));

        HistogramGroupConfig config = new HistogramGroupConfig(132, "my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo("Could not find a [numeric] field with name [my_field] in any of the " + "indices matching the index pattern.")
        );
    }

    public void testValidateFieldWrongType() throws IOException {

        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("keyword", fieldCaps));

        HistogramGroupConfig config = new HistogramGroupConfig(132, "my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo("The field referenced by a histo group must be a [numeric] type, but " + "found [keyword] for field [my_field]")
        );
    }

    public void testValidateFieldMatchingNotAggregatable() throws IOException {

        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap("long", fieldCaps));

        HistogramGroupConfig config = new HistogramGroupConfig(132, "my_field");
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateMatchingField() throws IOException {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        String mappingType = randomFrom(RollupField.NUMERIC_FIELD_MAPPER_TYPES);
        responseMap.put("my_field", Collections.singletonMap(mappingType, fieldCaps));

        HistogramGroupConfig config = new HistogramGroupConfig(132, "my_field");
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().size(), equalTo(0));
    }
}

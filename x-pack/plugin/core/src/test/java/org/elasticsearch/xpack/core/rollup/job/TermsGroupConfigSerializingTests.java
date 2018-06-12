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
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsGroupConfigSerializingTests extends AbstractSerializingTestCase<TermsGroupConfig> {

    private static final List<String> FLOAT_TYPES = Arrays.asList("half_float", "float", "double", "scaled_float");
    private static final List<String> NATURAL_TYPES = Arrays.asList("byte", "short", "integer", "long");

    @Override
    protected TermsGroupConfig doParseInstance(XContentParser parser) throws IOException {
        return TermsGroupConfig.PARSER.apply(parser, null).build();
    }

    @Override
    protected Writeable.Reader<TermsGroupConfig> instanceReader() {
        return TermsGroupConfig::new;
    }

    @Override
    protected TermsGroupConfig createTestInstance() {
        return ConfigTestHelpers.getTerms().build();
    }

    public void testValidateNoMapping() throws IOException {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        TermsGroupConfig config = new TermsGroupConfig.Builder()
                .setFields(Collections.singletonList("my_field"))
                .build();
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [keyword/text] field with name " +
                "[my_field] in any of the indices matching the index pattern."));
    }

    public void testValidateNomatchingField() throws IOException {

        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("keyword", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig.Builder()
                .setFields(Collections.singletonList("my_field"))
                .build();
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [keyword/text] field with name " +
                "[my_field] in any of the indices matching the index pattern."));
    }

    public void testValidateFieldWrongType() throws IOException {

        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("geo_point", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig.Builder()
                .setFields(Collections.singletonList("my_field"))
                .build();
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a terms group must be a [numeric] or " +
                "[keyword/text] type, but found [geo_point] for field [my_field]"));
    }

    public void testValidateFieldMatchingNotAggregatable() throws IOException {


        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap(getRandomType(), fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig.Builder()
                .setFields(Collections.singletonList("my_field"))
                .build();
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateMatchingField() throws IOException {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        String type = getRandomType();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap(type, fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig.Builder()
                .setFields(Collections.singletonList("my_field"))
                .build();
        config.validateMappings(responseMap, e);
        if (e.validationErrors().size() != 0) {
            fail(e.getMessage());
        }

        List<CompositeValuesSourceBuilder<?>> builders = config.toBuilders();
        assertThat(builders.size(), equalTo(1));
    }

    private String getRandomType() {
        int n = randomIntBetween(0,8);
        if (n == 0) {
            return "keyword";
        } else if (n == 1) {
            return "text";
        } else if (n == 2) {
            return "long";
        } else if (n == 3) {
            return "integer";
        } else if (n == 4) {
            return "short";
        } else if (n == 5) {
            return "float";
        } else if (n == 6) {
            return "double";
        } else if (n == 7) {
            return "scaled_float";
        } else if (n == 8) {
            return "half_float";
        }
        return "long";
    }
}

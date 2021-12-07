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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomTermsGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsGroupConfigSerializingTests extends AbstractSerializingTestCase<TermsGroupConfig> {

    @Override
    protected TermsGroupConfig doParseInstance(XContentParser parser) throws IOException {
        return TermsGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TermsGroupConfig> instanceReader() {
        return TermsGroupConfig::new;
    }

    @Override
    protected TermsGroupConfig createTestInstance() {
        return randomTermsGroupConfig(random());
    }

    public void testValidateNoMapping() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "Could not find a [numeric] or [keyword/text] field with name "
                    + "[my_field] in any of the indices matching the index pattern."
            )
        );
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("keyword", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "Could not find a [numeric] or [keyword/text] field with name "
                    + "[my_field] in any of the indices matching the index pattern."
            )
        );
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("geo_point", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(
            e.validationErrors().get(0),
            equalTo(
                "The field referenced by a terms group must be a [numeric] or "
                    + "[keyword/text] type, but found [geo_point] for field [my_field]"
            )
        );
    }

    public void testValidateFieldMatchingNotAggregatable() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(false);
        responseMap.put("my_field", Collections.singletonMap(getRandomType(), fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field [my_field] must be aggregatable across all indices, but is not."));
    }

    public void testValidateMatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
        String type = getRandomType();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        when(fieldCaps.isAggregatable()).thenReturn(true);
        responseMap.put("my_field", Collections.singletonMap(type, fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        if (e.validationErrors().size() != 0) {
            fail(e.getMessage());
        }
    }

    private String getRandomType() {
        int n = randomIntBetween(0, 8);
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

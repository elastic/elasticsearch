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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomTermsGroupConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

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
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [keyword/text] field with name " +
                "[my_field] in any of the indices matching the index pattern."));
    }

    public void testValidateNomatchingField() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("some_other_field", Collections.singletonMap("keyword", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("Could not find a [numeric] or [keyword/text] field with name " +
                "[my_field] in any of the indices matching the index pattern."));
    }

    public void testValidateFieldWrongType() {
        ActionRequestValidationException e = new ActionRequestValidationException();
        Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();

        // Have to mock fieldcaps because the ctor's aren't public...
        FieldCapabilities fieldCaps = mock(FieldCapabilities.class);
        responseMap.put("my_field", Collections.singletonMap("geo_point", fieldCaps));

        TermsGroupConfig config = new TermsGroupConfig("my_field");
        config.validateMappings(responseMap, e);
        assertThat(e.validationErrors().get(0), equalTo("The field referenced by a terms group must be a [numeric] or " +
                "[keyword/text] type, but found [geo_point] for field [my_field]"));
    }
}

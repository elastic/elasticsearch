/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action.job;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.job.RollupIndexer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupIndexTests extends ESTestCase {

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

        List<CompositeValuesSourceBuilder<?>> builders = RollupIndexer.createValueSourceBuilders(config);
        assertThat(builders.size(), equalTo(1));
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

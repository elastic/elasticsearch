/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RuntimeMappingsValidatorTests extends ESTestCase {

    public void testValidate_GivenFieldWhoseValueIsNotMap() {
        Map<String, Object> runtimeMappings = Collections.singletonMap("mapless", "not a map");

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> RuntimeMappingsValidator.validate(runtimeMappings));

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Expected map for runtime field [mapless] definition but got a String"));
    }

    public void testValidate_GivenFieldWithoutType() {
        Map<String, Object> fieldMapping = Collections.singletonMap("not a type", "42");
        Map<String, Object> runtimeMappings = Collections.singletonMap("typeless", fieldMapping);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> RuntimeMappingsValidator.validate(runtimeMappings));

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("No type specified for runtime field [typeless]"));
    }

    public void testValidate_GivenValid() {
        Map<String, Object> fieldMapping = Collections.singletonMap("type", "keyword");
        Map<String, Object> runtimeMappings = Collections.singletonMap("valid_field", fieldMapping);

        RuntimeMappingsValidator.validate(runtimeMappings);
    }
}

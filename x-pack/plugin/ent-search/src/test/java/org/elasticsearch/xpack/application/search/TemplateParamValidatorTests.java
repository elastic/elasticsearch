/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class TemplateParamValidatorTests extends ESTestCase {

    public void testValidateThrowsWhenSchemaDontMatch() {
        Map<String, Object> failingParams = new HashMap<>();
        failingParams.put("fail", null);
        TemplateParamValidator validator = new TemplateParamValidator(
            "{\"properties\":{\"title_boost\":{\"type\":\"number\"},\"additionalProperties\":false},\"required\":[\"query_string\"]}"
        );

        assertThrows(ValidationException.class, () -> validator.validate(failingParams));

    }

    public void testValidateWithFloats() {
        Map<String, Object> passingParams = new HashMap<>();
        passingParams.put("title_boost", 1.0f);
        passingParams.put("query_string", "query_string");
        TemplateParamValidator validator = new TemplateParamValidator(
            "{\"properties\":{\"title_boost\":{\"type\":\"number\"},\"additionalProperties\":false},\"required\":[\"query_string\"]}"
        );
        // This shouldn't throw
        // We don't have nice assertions for this in JUnit 4
        validator.validate(passingParams);
    }

}

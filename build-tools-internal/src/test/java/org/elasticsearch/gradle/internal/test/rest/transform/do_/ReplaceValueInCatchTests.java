/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.do_;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReplaceValueInCatchTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testReplaceValueInCatch() throws Exception {

        String test_original = "/rest/transform/do/replace_value_in_catch_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/do/replace_value_in_catch_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new ReplaceValueInCatch(MAPPER.convertValue("/parse_exception/", JsonNode.class), null))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }
}

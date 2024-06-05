/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.length;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReplaceValueInLengthTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testReplaceMatch() throws Exception {
        String test_original = "/rest/transform/length/length_replace_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/length/length_replace_transformed_value.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        NumericNode replacementNode = MAPPER.convertValue(99, NumericNode.class);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new ReplaceValueInLength("key.in_length_to_replace", replacementNode, null))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }
}

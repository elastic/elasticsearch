/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest.transform.close_to;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class ReplaceValueInCloseToTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testReplaceValue() throws Exception {
        String test_original = "/rest/transform/close_to/close_to_replace_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/close_to/close_to_replace_transformed_value.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        NumericNode replacementNode = MAPPER.convertValue(99.99, NumericNode.class);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new ReplaceValueInCloseTo("aggregations.tsids.buckets.0.voltage.value", replacementNode, null))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }
}

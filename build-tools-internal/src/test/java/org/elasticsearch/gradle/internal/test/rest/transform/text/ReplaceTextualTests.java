/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.text;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.List;

public class ReplaceTextualTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testReplaceAll() throws Exception {
        String test_original = "/rest/transform/text/text_replace_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/text/text_replace_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            List.of(
                new ReplaceTextual("key_to_replace", "value_to_replace", MAPPER.convertValue("_replaced_value", TextNode.class), null),
                new ReplaceIsTrue("is_true_to_replace", MAPPER.convertValue("is_true_replaced", TextNode.class)),
                new ReplaceIsFalse("is_false_to_replace", MAPPER.convertValue("is_false_replaced", TextNode.class))
            )
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

}

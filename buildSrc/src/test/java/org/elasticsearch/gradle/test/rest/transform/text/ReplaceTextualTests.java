/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.text;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsearch.gradle.test.rest.transform.TransformTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplaceTextualTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testReplaceAll() throws Exception {
        String testName = "/rest/transform/text/text_replace.yml";
        List<ObjectNode> tests = getTests(testName);
        TextNode replacementNode = MAPPER.convertValue("_replaced_value", TextNode.class);
        validateTest(tests, true, true);
        List<ObjectNode> transformedTests = transformTests(
            new LinkedList<>(tests),
            Collections.singletonList(new ReplaceTextual("key_to_replace", "value_to_replace", replacementNode, null))
        );
        printTest(testName, transformedTests);
        validateTest(tests, false, true);
    }

    private void validateTest(List<ObjectNode> tests, boolean beforeTransformation, boolean allTests) {
        validateSetupAndTearDownForMatchTests(tests);
        // first test
        JsonNode firstTestChild = tests.get(2).get("First test");
        assertThat(firstTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode firstTestParentArray = (ArrayNode) firstTestChild;

        AtomicBoolean firstTestHasKeyToReplace = new AtomicBoolean(false);
        AtomicBoolean firstTestHasValueToReplace = new AtomicBoolean(false);

        firstTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("key_to_replace");
            if (matchObject != null) {
                firstTestHasKeyToReplace.set(true);
                if (beforeTransformation && matchObject.asText().equals("value_to_replace")) {
                    firstTestHasValueToReplace.set(true);
                }
                if (beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("_replaced_value"));
                    firstTestHasValueToReplace.set(true);
                }
            }
        });
        assertTrue(firstTestHasKeyToReplace.get());
        assertTrue(firstTestHasValueToReplace.get());

        // last test
        JsonNode lastTestChild = tests.get(tests.size() - 1).get("Last test");
        assertThat(lastTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode lastTestParentArray = (ArrayNode) lastTestChild;

        AtomicBoolean lastTestHasKeyToReplace = new AtomicBoolean(false);
        AtomicBoolean lastTestHasValueToReplace = new AtomicBoolean(false);
        lastTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("key_to_replace");
            if (matchObject != null) {
                lastTestHasKeyToReplace.set(true);
                if (beforeTransformation && matchObject.asText().equals("value_to_replace")) {
                    lastTestHasValueToReplace.set(true);
                }
                if (beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("_replaced_value"));
                    lastTestHasValueToReplace.set(true);
                }
            }
        });
        assertTrue(lastTestHasKeyToReplace.get());
        assertTrue(lastTestHasValueToReplace.get());

        // validate test3 - with key_to_replace but without a value
        {
            ObjectNode otherTest = tests.get(4);
            JsonNode otherTestChild = otherTest.get(otherTest.fields().next().getKey());
            assertThat(otherTestChild, CoreMatchers.instanceOf(ArrayNode.class));
            ArrayNode otherTestParentArray = (ArrayNode) otherTestChild;
            otherTestParentArray.elements().forEachRemaining(node -> {
                assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
                ObjectNode childObject = (ObjectNode) node;
                JsonNode value = childObject.get("key_to_replace");
                if (value != null && beforeTransformation && allTests) {
                    assertThat(value.asText(), CoreMatchers.is("value_NOT_to_replace"));
                }
                if (value != null && beforeTransformation == false && allTests) {
                    assertThat(value.asText(), CoreMatchers.is("value_NOT_to_replace"));
                }
            });
        }

        // exclude setup, teardown, first test, second to last test (does not have value to replace), and last test
        for (int i = 3; i <= tests.size() - 3; i++) {
            ObjectNode otherTest = tests.get(i);
            JsonNode otherTestChild = otherTest.get(otherTest.fields().next().getKey());
            assertThat(otherTestChild, CoreMatchers.instanceOf(ArrayNode.class));
            ArrayNode otherTestParentArray = (ArrayNode) otherTestChild;
            otherTestParentArray.elements().forEachRemaining(node -> {
                assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
                ObjectNode childObject = (ObjectNode) node;
                JsonNode value = childObject.get("key_to_replace");
                if (value != null && beforeTransformation == false && allTests) {
                    assertThat(value.asText(), CoreMatchers.is("_replaced_value"));
                }
            });
        }
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

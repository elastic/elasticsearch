/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.match;

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
        String testName = "/rest/transform/match/text_replace.yml";
        List<ObjectNode> tests = getTests(testName);
        TextNode replacementNode = MAPPER.convertValue("_replaced_value", TextNode.class);
        validateTest(tests, true, true);
        List<ObjectNode> transformedTests = transformTests(
            new LinkedList<>(tests),
            Collections.singletonList(new ReplaceTextual("key_to_replace", "key_to_replace", replacementNode, null))
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

        AtomicBoolean firstTestOccurrenceFound = new AtomicBoolean(false);

        firstTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("key_to_replace");
            if (matchObject != null) {
                firstTestOccurrenceFound.set(true);
                if (beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("_replaced_value"));
                }
            }
        });
        assertTrue(firstTestOccurrenceFound.get());

        // last test
        JsonNode lastTestChild = tests.get(tests.size() - 1).get("Last test");
        assertThat(lastTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode lastTestParentArray = (ArrayNode) lastTestChild;

        AtomicBoolean lastTestOccurrenceFound = new AtomicBoolean(false);
        lastTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("key_to_replace");
            if (matchObject != null) {
                lastTestOccurrenceFound.set(true);
                if (beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("_replaced_value"));
                }
            }
        });
        assertTrue(lastTestOccurrenceFound.get());

        // exclude setup, teardown, first test, and last test
        for (int i = 3; i <= tests.size() - 2; i++) {
            ObjectNode otherTest = tests.get(i);
            JsonNode otherTestChild = otherTest.get(otherTest.fields().next().getKey());
            assertThat(otherTestChild, CoreMatchers.instanceOf(ArrayNode.class));
            ArrayNode otherTestParentArray = (ArrayNode) otherTestChild;
            otherTestParentArray.elements().forEachRemaining(node -> {
                assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
                ObjectNode childObject = (ObjectNode) node;
                JsonNode matchObject = childObject.get("key_to_replace");
                if (matchObject != null) {
                    if (beforeTransformation == false && allTests) {
                        assertThat(matchObject.get("key_to_replace").asText(), CoreMatchers.is("_replaced_value"));
                    }
                }
            });
        }
    }

    @Override
    protected boolean getHumanDebug() {
        return true;
    }
}

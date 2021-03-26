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
        String testName = "/rest/transform/match/is_true.yml";
        List<ObjectNode> tests = getTests(testName);
        JsonNode replacementNode = MAPPER.convertValue("test_index.mappings._doc", JsonNode.class);
        validateTest(tests, true, true);
        List<ObjectNode> transformedTests = transformTests(
            new LinkedList<>(tests),
            Collections.singletonList(new ReplaceTextual("test_index.mappings.type_1", replacementNode, null))
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

        AtomicBoolean firstTestHasMatchObject = new AtomicBoolean(false);
        AtomicBoolean firstTestHasTypeMatch = new AtomicBoolean(false);

        firstTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("is_true");
            if (matchObject != null) {
                firstTestHasMatchObject.set(true);
                if (firstTestHasTypeMatch.get() == false ) {
                    firstTestHasTypeMatch.set(true);
                }/*&& matchObject.asText().equals("test_index.mappings.type_1")*/
                if (/*matchObject.get("_type") != null && */beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("test_index.mappings._doc"));
                }
            }
        });
        assertTrue(firstTestHasMatchObject.get());
        assertTrue(firstTestHasTypeMatch.get());

        // last test
        JsonNode lastTestChild = tests.get(tests.size() - 1).get("Last test");
        assertThat(lastTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode lastTestParentArray = (ArrayNode) lastTestChild;

        AtomicBoolean lastTestHasMatchObject = new AtomicBoolean(false);
        AtomicBoolean lastTestHasTypeMatch = new AtomicBoolean(false);
        lastTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("is_true");
            if (matchObject != null) {
                lastTestHasMatchObject.set(true);
                if (lastTestHasTypeMatch.get() == false ) {
                    lastTestHasTypeMatch.set(true);
                }/*&& matchObject.asText().equals("test_index.mappings.type_1")*/
                if (/*matchObject.get("_type") != null && */beforeTransformation == false && allTests) {
                    assertThat(matchObject.asText(), CoreMatchers.is("test_index.mappings._doc"));
                }
            }
        });
        assertTrue(lastTestHasMatchObject.get());
        assertTrue(lastTestHasTypeMatch.get());

//        // exclude setup, teardown, first test, and last test
//        for (int i = 3; i <= tests.size() - 2; i++) {
//            ObjectNode otherTest = tests.get(i);
//            JsonNode otherTestChild = otherTest.get(otherTest.fields().next().getKey());
//            assertThat(otherTestChild, CoreMatchers.instanceOf(ArrayNode.class));
//            ArrayNode otherTestParentArray = (ArrayNode) otherTestChild;
//            otherTestParentArray.elements().forEachRemaining(node -> {
//                assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
//                ObjectNode childObject = (ObjectNode) node;
//                JsonNode matchObject = childObject.get("match");
//                if (matchObject != null) {
//                    if (matchObject.get("_type") != null) {
//                        if (matchObject.get("_type") != null && beforeTransformation == false && allTests) {
//                            assertThat(matchObject.get("_type").asText(), CoreMatchers.is("_replaced_type"));
//                        }
//                    }
//                }
//            });
//        }
    }

    @Override
    protected boolean getHumanDebug() {
        return true;
    }
}

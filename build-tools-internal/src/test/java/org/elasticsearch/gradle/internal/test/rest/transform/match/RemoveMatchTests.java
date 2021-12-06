/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.match;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoveMatchTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);

    @Test
    public void testRemoveAll() throws Exception {
        String testName = "/rest/transform/match/match_original.yml";
        List<ObjectNode> tests = getTests(testName);
        validateTest(tests, true, true);
        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new RemoveMatch("_type")));
        printTest(testName, transformedTests);
        validateTest(tests, false, true);
    }

    @Test
    public void testRemoveByTest() throws Exception {
        String testName = "/rest/transform/match/match_original.yml";
        List<ObjectNode> tests = getTests(testName);
        validateTest(tests, true, false);
        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new RemoveMatch("_type", "Last test")));
        printTest(testName, transformedTests);
        validateTest(tests, false, false);

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
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                firstTestHasMatchObject.set(true);
                if (firstTestHasTypeMatch.get() == false && matchObject.get("_type") != null) {
                    firstTestHasTypeMatch.set(true);
                }
            }
        });
        assertTrue(firstTestHasMatchObject.get());
        if (beforeTransformation) {
            assertTrue(firstTestHasTypeMatch.get());
        } else {
            if (allTests) {
                assertFalse(firstTestHasTypeMatch.get());
            } else {
                assertTrue(firstTestHasTypeMatch.get());
            }
        }

        // last test
        JsonNode lastTestChild = tests.get(tests.size() - 1).get("Last test");
        assertThat(lastTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode lastTestParentArray = (ArrayNode) lastTestChild;

        AtomicBoolean lastTestHasMatchObject = new AtomicBoolean(false);
        AtomicBoolean lastTestHasTypeMatch = new AtomicBoolean(false);
        lastTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                lastTestHasMatchObject.set(true);
                if (lastTestHasTypeMatch.get() == false && matchObject.get("_type") != null) {
                    lastTestHasTypeMatch.set(true);
                }
            }
        });
        assertTrue(lastTestHasMatchObject.get());
        if (beforeTransformation) {
            assertTrue(lastTestHasTypeMatch.get());
        } else {
            assertFalse(lastTestHasTypeMatch.get());
        }

        // exclude setup, teardown, first test, and last test
        for (int i = 3; i <= tests.size() - 2; i++) {
            ObjectNode otherTest = tests.get(i);

            JsonNode otherTestChild = otherTest.get(otherTest.fields().next().getKey());
            assertThat(otherTestChild, CoreMatchers.instanceOf(ArrayNode.class));
            ArrayNode otherTestParentArray = (ArrayNode) otherTestChild;
            AtomicBoolean otherTestHasTypeMatch = new AtomicBoolean(false);
            otherTestParentArray.elements().forEachRemaining(node -> {
                assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
                ObjectNode childObject = (ObjectNode) node;
                JsonNode matchObject = childObject.get("match");
                if (matchObject != null) {
                    if (beforeTransformation == false) {
                        if (otherTestHasTypeMatch.get() == false && matchObject.get("_type") != null) {
                            otherTestHasTypeMatch.set(true);
                        }
                    }
                }

                if (allTests) {
                    assertFalse(otherTestHasTypeMatch.get());
                }
            });
        }
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

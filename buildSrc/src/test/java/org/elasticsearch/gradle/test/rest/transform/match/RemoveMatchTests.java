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
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformer;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RemoveMatchTests extends GradleUnitTestCase {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);

    private static final boolean humanDebug = false; // useful for humans trying to debug these tests

    @Test
    public void testRemoveAll() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        validateTest(tests, true, true);
        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new RemoveMatch("_type"))
        );
        printTest(testName, transformedTests);
        validateTest(tests, false, true);
    }

    @Test
    public void testRemoveByTest() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        validateTest(tests, true, false);
        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new RemoveMatch("_type", "Basic"))
        );
        printTest(testName, transformedTests);
        validateTest(tests, false, false);

    }

    private void validateTest(List<ObjectNode> tests, boolean beforeTransformation, boolean allTests) {
        ObjectNode setUp = tests.get(0);
        assertThat(setUp.get("setup"), CoreMatchers.notNullValue());
        ObjectNode tearDown = tests.get(1);
        assertThat(tearDown.get("teardown"), CoreMatchers.notNullValue());
        ObjectNode firstTest = tests.get(2);
        assertThat(firstTest.get("Test that queries on _index match against the correct indices."), CoreMatchers.notNullValue());
        ObjectNode lastTest = tests.get(tests.size() - 1);
        assertThat(lastTest.get("Basic"), CoreMatchers.notNullValue());

        // setup
        JsonNode setup = setUp.get("setup");
        assertThat(setup, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode setupParentArray = (ArrayNode) setup;

        AtomicBoolean setUpHasMatchObject = new AtomicBoolean(false);
        setupParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                setUpHasMatchObject.set(true);
            }
        });
        assertFalse(setUpHasMatchObject.get());

        // teardown
        JsonNode teardown = tearDown.get("teardown");
        assertThat(teardown, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode teardownParentArray = (ArrayNode) teardown;

        AtomicBoolean teardownHasMatchObject = new AtomicBoolean(false);
        teardownParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                teardownHasMatchObject.set(true);
            }
        });
        assertFalse(teardownHasMatchObject.get());

        // first test
        JsonNode firstTestChild = firstTest.get("Test that queries on _index match against the correct indices.");
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
        JsonNode lastTestChild = lastTest.get("Basic");
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

    // only to help manually debug
    private void printTest(String testName, List<ObjectNode> tests) {
        if (humanDebug) {
            System.out.println("\n************* " + testName + " *************");
            try (SequenceWriter sequenceWriter = MAPPER.writer().writeValues(System.out)) {
                for (ObjectNode transformedTest : tests) {
                    sequenceWriter.write(transformedTest);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

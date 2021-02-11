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

public class AddMatchTests extends GradleUnitTestCase {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);

    private static final boolean humanDebug = false; // useful for humans trying to debug these tests

    @Test
    public void testAddAllNotSupported() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        JsonNode addNode = MAPPER.convertValue("_doc", JsonNode.class);
        assertEquals(
            "adding matches is only supported for named tests",
            expectThrows(
                NullPointerException.class,
                () -> transformer.transformRestTests(
                    new LinkedList<>(tests),
                    Collections.singletonList(new AddMatch("_type", addNode, null))
                )
            ).getMessage()
        );

    }

    @Test
    public void testAddByTest() throws Exception {
        String testName = "/rest/transform/match/match.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        JsonNode addNode = MAPPER.convertValue(123456789, JsonNode.class);
        validateTest(tests, true);
        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new AddMatch("my_number", addNode, "Basic"))
        );
        printTest(testName, transformedTests);
        validateTest(tests, false);
    }

    private void validateTest(List<ObjectNode> tests, boolean beforeTransformation) {
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
        firstTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                firstTestHasMatchObject.set(true);
            }
        });
        assertTrue(firstTestHasMatchObject.get());

        // last test
        JsonNode lastTestChild = lastTest.get("Basic");
        assertThat(lastTestChild, CoreMatchers.instanceOf(ArrayNode.class));
        ArrayNode lastTestParentArray = (ArrayNode) lastTestChild;

        AtomicBoolean lastTestHasMatchObject = new AtomicBoolean(false);
        AtomicBoolean lastTestHasAddedObject = new AtomicBoolean(false);
        lastTestParentArray.elements().forEachRemaining(node -> {
            assertThat(node, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode childObject = (ObjectNode) node;
            JsonNode matchObject = childObject.get("match");
            if (matchObject != null) {
                lastTestHasMatchObject.set(true);
                if (lastTestHasAddedObject.get() == false && matchObject.get("my_number") != null) {
                    lastTestHasAddedObject.set(true);
                }

            }
        });
        assertTrue(lastTestHasMatchObject.get());
        if (beforeTransformation) {
            assertFalse(lastTestHasAddedObject.get());
        } else {
            assertTrue(lastTestHasAddedObject.get());
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

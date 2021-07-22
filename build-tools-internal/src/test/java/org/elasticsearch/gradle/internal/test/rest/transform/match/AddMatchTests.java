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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class AddMatchTests extends TransformTests {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);

    @Test
    public void testAddAllNotSupported() throws Exception {
        String testName = "/rest/transform/match/match_original.yml";
        List<ObjectNode> tests = getTests(testName);
        JsonNode addNode = MAPPER.convertValue("_doc", JsonNode.class);
        assertEquals(
            "adding matches is only supported for named tests",
            expectThrows(
                NullPointerException.class,
                () -> transformTests(tests, Collections.singletonList(new AddMatch("_type", addNode, null)))
            ).getMessage()
        );
    }

    @Test
    public void testAddByTest() throws Exception {
        String testName = "/rest/transform/match/match_original.yml";
        List<ObjectNode> tests = getTests(testName);
        JsonNode addNode = MAPPER.convertValue(123456789, JsonNode.class);
        validateTest(tests, true);
        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new AddMatch("my_number", addNode, "Last test"))
        );
        printTest(testName, transformedTests);
        validateTest(tests, false);
    }

    private void validateTest(List<ObjectNode> tests, boolean beforeTransformation) {
        validateSetupAndTearDownForMatchTests(tests);
        // first test
        JsonNode firstTestChild = tests.get(2).get("First test");
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
        JsonNode lastTestChild = tests.get(tests.size() - 1).get("Last test");
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

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

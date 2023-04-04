/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Perform the transformations against the set of RestTests from a given file.
 */
public class RestTestTransformer {
    /**
     * Transforms a REST test based on the requested {@link RestTestTransform}'s
     *
     * @param tests           The REST tests from the same file. Uses linked list so we can easily add to the beginning of the list.
     * @param transformations The set of transformations to perform against the test
     * @return the transformed tests
     */
    public List<ObjectNode> transformRestTests(LinkedList<ObjectNode> tests, List<RestTestTransform<?>> transformations) {
        ObjectNode setupSection = null;
        ObjectNode teardownSection = null;

        // Collect any global setup transformations
        List<RestTestTransformGlobalSetup> setupTransforms = transformations.stream()
            .filter(transform -> transform instanceof RestTestTransformGlobalSetup)
            .map(transform -> (RestTestTransformGlobalSetup) transform)
            .collect(Collectors.toList());

        // Collect any global teardown transformations
        List<RestTestTransformGlobalTeardown> teardownTransforms = transformations.stream()
            .filter(transform -> transform instanceof RestTestTransformGlobalTeardown)
            .map(transform -> (RestTestTransformGlobalTeardown) transform)
            .collect(Collectors.toList());

        // Collect any transformations that are identified by an object key.
        Map<String, List<RestTestTransformByParentObject>> objectKeyFinders = transformations.stream()
            .filter(transform -> transform instanceof RestTestTransformByParentObject)
            .map(transform -> (RestTestTransformByParentObject) transform)
            .collect(Collectors.groupingBy(RestTestTransformByParentObject::getKeyToFind));

        // Collect any transformations that are identified by an object key where the value is an array
        Map<String, List<RestTestTransformByParentArray>> arrayByObjectKeyFinders = transformations.stream()
            .filter(transform -> transform instanceof RestTestTransformByParentArray)
            .map(transform -> (RestTestTransformByParentArray) transform)
            .collect(Collectors.groupingBy(RestTestTransformByParentArray::getKeyOfArrayToFind));

        // transform the tests and include the global setup and teardown as part of the transform
        for (ObjectNode test : tests) {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                String testName = testObject.getKey();
                if ("setup".equals(testName)) {
                    setupSection = test;
                }
                if ("teardown".equals(testName)) {
                    teardownSection = test;
                }
                traverseTest(new RestTestContext(testName), test, null, objectKeyFinders, arrayByObjectKeyFinders);
            }
        }

        // transform the global setup
        if (setupTransforms.isEmpty() == false) {
            int i = 0;
            for (RestTestTransformGlobalSetup setupTransform : setupTransforms) {
                ObjectNode result = setupTransform.transformSetup(setupSection);
                // add the setup section if it does not currently exist
                if (i++ == 0 && result != null && setupSection == null) {
                    tests.addFirst(result);
                    setupSection = result;
                }
            }
        }

        // transform the global teardown
        if (setupTransforms.isEmpty() == false) {
            int i = 0;
            for (RestTestTransformGlobalTeardown teardownTransform : teardownTransforms) {
                ObjectNode result = teardownTransform.transformTeardown(teardownSection);
                // add the teardown section if it does not currently exist
                if (i++ == 0 && result != null && teardownSection == null) {
                    tests.addLast(result);
                    teardownSection = result;
                }
            }
        }

        return tests;
    }

    /**
     * Recursive method to traverse the test.
     *
     * @param testContext             A pojo to hold information about the current state of the test that is being traversed.
     * @param currentNode             The current node that is being evaluated.
     * @param parentKeyName           The name of the parent key object for the current node. null if none.
     * @param objectKeyFinders        A Map of object keys to find and their associated transformation by parent Object
     * @param arrayByObjectKeyFinders A Map of object keys to find and their associated transformation by parent Array
     */
    private void traverseTest(
        RestTestContext testContext,
        JsonNode currentNode,
        String parentKeyName,
        Map<String, List<RestTestTransformByParentObject>> objectKeyFinders,
        Map<String, List<RestTestTransformByParentArray>> arrayByObjectKeyFinders
    ) {
        if (currentNode.isArray()) {
            if (parentKeyName != null) {
                List<RestTestTransformByParentArray> transforms = arrayByObjectKeyFinders.get(parentKeyName);
                if (transforms != null) {
                    for (RestTestTransformByParentArray transform : transforms) {
                        if (transform.shouldApply(testContext)) {
                            transform.transformTest((ArrayNode) currentNode);
                        }
                    }
                }
            }
            currentNode.elements().forEachRemaining(node -> {
                traverseTest(testContext, node, parentKeyName, objectKeyFinders, arrayByObjectKeyFinders);
            });
        } else if (currentNode.isObject()) {
            currentNode.fields().forEachRemaining(entry -> {
                List<RestTestTransformByParentObject> transforms = objectKeyFinders.get(entry.getKey());
                if (transforms == null) {
                    traverseTest(testContext, entry.getValue(), entry.getKey(), objectKeyFinders, arrayByObjectKeyFinders);
                } else {
                    for (RestTestTransformByParentObject transform : transforms) {
                        if (transform.shouldApply(testContext)) {
                            if (transform.requiredChildKey() == null) {
                                transform.transformTest((ObjectNode) currentNode);
                            } else {
                                if (entry.getValue().isObject()) {
                                    ObjectNode child = (ObjectNode) entry.getValue();
                                    if (transform.matches(child)) {
                                        transform.transformTest((ObjectNode) currentNode);
                                    }
                                } else if (entry.getValue().isTextual()) {
                                    TextNode value = (TextNode) entry.getValue();
                                    if (transform.matches(value)) {
                                        transform.transformTest((ObjectNode) currentNode);
                                    }

                                }
                            }
                        }
                        traverseTest(testContext, entry.getValue(), entry.getKey(), objectKeyFinders, arrayByObjectKeyFinders);
                    }
                }
            });
        }
    }
}

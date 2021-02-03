/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
        Map<String, List<RestTestTransformByObjectKey>> objectKeyFinders = transformations.stream()
            .filter(transform -> transform instanceof RestTestTransformByObjectKey)
            .map(transform -> (RestTestTransformByObjectKey) transform)
            .collect(Collectors.groupingBy(RestTestTransformByObjectKey::getKeyToFind));

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
                traverseTest(testName, test, objectKeyFinders);
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
     * @param testName         The name of the test that is being traversed.
     * @param currentNode      The current node that is being evaluated.
     * @param objectKeyFinders A Map of object keys to find and their associated transformation
     */
    private void traverseTest(String testName, JsonNode currentNode, Map<String, List<RestTestTransformByObjectKey>> objectKeyFinders) {
        if (currentNode.isArray()) {
            currentNode.elements().forEachRemaining(node -> {
                traverseTest(testName, node, objectKeyFinders);
            });
        } else if (currentNode.isObject()) {
            currentNode.fields().forEachRemaining(entry -> {
                List<RestTestTransformByObjectKey> transforms = objectKeyFinders.get(entry.getKey());
                if (transforms == null) {
                    traverseTest(testName, entry.getValue(), objectKeyFinders);
                } else {
                    for (RestTestTransformByObjectKey transform : transforms) {
                        if (transform.getTestName() == null || testName.equals(transform.getTestName())) {
                            if (transform.withChildKey() == null) {
                                transform.transformTest((ObjectNode) currentNode);
                            } else {
                                if (entry.getValue().isObject()) {
                                    ObjectNode child = (ObjectNode) entry.getValue();
                                    if (child.has(transform.withChildKey())) {
                                        transform.transformTest((ObjectNode) currentNode);
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }
    }
}

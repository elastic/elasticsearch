/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;


import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Perform the transformations against the set of RestTests from a given file.
 */
public class RestTestTransformer {


    private static final YAMLFactory yaml = new YAMLFactory();
    private static final ObjectMapper mapper = new ObjectMapper(yaml);
    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);


    /**
     * @param tests           The REST tests from the same file.
     * @param transformations The set of transformations to perform against the test
     * @return the transformed tests
     */
    public List<ObjectNode> transformRestTests(LinkedList<ObjectNode> tests, List<RestTestTransform<?>> transformations) {
        if (transformations == null || transformations.isEmpty()) {
            return tests;
        }

        ObjectNode setupSection = null;
        for (ObjectNode test : tests) {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();

            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                String testName = testObject.getKey();
                if("setup".equals(testName)){
                    setupSection = test;
                }
                Map<String, ObjectKeyFinder> objectKeyFinders = transformations.stream()
                    .filter(a -> a instanceof ObjectKeyFinder)
                    .map(b -> (ObjectKeyFinder) b)
                    .collect(Collectors.toMap(ObjectKeyFinder::getKeyToFind, d -> d));

                transformByObjectKeyName(test, objectKeyFinders);
            }
        }

        List<RestTestSetupTransform> setupTransforms = transformations.stream()
            .filter(a -> a instanceof RestTestSetupTransform)
            .map(b -> (RestTestSetupTransform) b)
            .collect(Collectors.toList());

        assert setupTransforms.isEmpty() || setupTransforms.size() == 1;

        if (setupTransforms.isEmpty() == false) {
            RestTestSetupTransform setupTransform = setupTransforms.iterator().next();
            ObjectNode result = setupTransform.transformSetup(setupSection);
            if(setupSection == null){
                tests.addFirst(result);
            }
        }
        tests.forEach(t -> {
            System.out.println(t.toPrettyString());
        });
        return tests;
    }

    private void transformByObjectKeyName(JsonNode currentNode, Map<String, ObjectKeyFinder> objectKeyFinders) {
        if (currentNode.isArray()) {
            currentNode.elements().forEachRemaining(node -> {
                transformByObjectKeyName(node, objectKeyFinders);
            });
        } else if (currentNode.isObject()) {
            currentNode.fields().forEachRemaining(entry -> {
                ObjectKeyFinder transform = objectKeyFinders.get(entry.getKey());
                if (transform == null) {
                    transformByObjectKeyName(entry.getValue(), objectKeyFinders);
                } else {
                    transform.transformTest((ObjectNode) currentNode);
                }
            });
        }
    }
}

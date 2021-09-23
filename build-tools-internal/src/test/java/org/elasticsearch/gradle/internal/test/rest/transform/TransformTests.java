/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.elasticsearch.gradle.internal.test.rest.transform.headers.InjectHeaders;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public abstract class TransformTests extends GradleUnitTestCase {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);

    private static final Map<String, String> headers1 = Map.of("foo", "bar");
    private static final Map<String, String> headers2 = Map.of("abc", "xyz");

    RestTestTransformer transformer;

    @Before
    public void setup() {
        transformer = new RestTestTransformer();
    }

    protected void validateSetupAndTearDown(List<ObjectNode> transformedTests) {
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
    }

    protected ObjectNode validateSkipNodesExist(List<ObjectNode> tests) {
        List<ObjectNode> skipNodes = tests.stream()
            .filter(node -> node.get("setup") != null)
            .filter(node -> getSkipNode((ArrayNode) node.get("setup")) != null)
            .map(node -> getSkipNode((ArrayNode) node.get("setup")))
            .collect(Collectors.toList());
        assertThat(skipNodes.size(), CoreMatchers.equalTo(1));
        return skipNodes.get(0);
    }

    protected void validateSkipNodesDoesNotExist(List<ObjectNode> tests) {
        List<ObjectNode> skipNodes = tests.stream()
            .filter(node -> node.get("setup") != null)
            .filter(node -> getSkipNode((ArrayNode) node.get("setup")) != null)
            .map(node -> getSkipNode((ArrayNode) node.get("setup")))
            .collect(Collectors.toList());
        assertThat(skipNodes.size(), CoreMatchers.equalTo(0));
    }

    protected void validatePreExistingFeatureExist(List<ObjectNode> tests) {
        assertThat(validateSkipNodesExist(tests).get("features"), CoreMatchers.notNullValue());
    }

    protected void validateSetupDoesNotExist(List<ObjectNode> tests) {
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(0L));
    }

    protected void validateFeatureNameExists(List<ObjectNode> tests, String featureName) {
        ObjectNode skipNode = validateSkipNodesExist(tests);
        JsonNode featureValues = skipNode.get("features");
        assertNotNull(featureValues);

        List<String> features = new ArrayList<>(1);
        if (featureValues.isArray()) {
            Iterator<JsonNode> featuresIt = featureValues.elements();
            while (featuresIt.hasNext()) {
                JsonNode feature = featuresIt.next();
                features.add(feature.asText());
            }
        } else if (featureValues.isTextual()) {
            features.add(featureValues.asText());
        }

        assertThat(features, IsCollectionContaining.hasItem(featureName));
    }

    protected void validateSetupExist(List<ObjectNode> tests) {
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
    }

    protected List<ObjectNode> transformTests(List<ObjectNode> tests) {
        return transformTests(tests, getTransformations());
    }

    protected List<ObjectNode> transformTests(List<ObjectNode> tests, List<RestTestTransform<?>> transforms) {
        List<ObjectNode> t = transformer.transformRestTests(new LinkedList<>(tests), transforms);
        getKnownFeatures().forEach(name -> { validateFeatureNameExists(t, name); });

        return t;
    }

    protected List<String> getKnownFeatures() {
        return Collections.emptyList();
    }

    protected List<RestTestTransform<?>> getTransformations() {
        List<RestTestTransform<?>> transformations = new ArrayList<>();
        transformations.add(new InjectHeaders(headers1, Collections.emptySet()));
        transformations.add(new InjectHeaders(headers2, Collections.emptySet()));
        return transformations;
    }

    protected List<ObjectNode> getTests(String relativePath) throws Exception {
        File testFile = new File(getClass().getResource(relativePath).toURI());
        YAMLParser yamlParser = YAML_FACTORY.createParser(testFile);
        return READER.<ObjectNode>readValues(yamlParser).readAll();
    }

    protected void assertTeardown(ObjectNode teardownNode) {
        assertThat(teardownNode.get("teardown"), CoreMatchers.instanceOf(ArrayNode.class));
        ObjectNode skipNode = getSkipNode((ArrayNode) teardownNode.get("teardown"));
        assertSkipNode(skipNode);
    }

    protected void assertSetup(ObjectNode setupNode) {
        assertThat(setupNode.get("setup"), CoreMatchers.instanceOf(ArrayNode.class));
        ObjectNode skipNode = getSkipNode((ArrayNode) setupNode.get("setup"));
        assertSkipNode(skipNode);
    }

    protected void assertSkipNode(ObjectNode skipNode) {
        assertThat(skipNode, CoreMatchers.notNullValue());
        List<String> featureValues = new ArrayList<>();
        if (skipNode.get("features").isArray()) {
            assertThat(skipNode.get("features"), CoreMatchers.instanceOf(ArrayNode.class));
            ArrayNode features = (ArrayNode) skipNode.get("features");
            features.forEach(x -> {
                if (x.isTextual()) {
                    featureValues.add(x.asText());
                }
            });
        } else {
            featureValues.add(skipNode.get("features").asText());
        }
        assertEquals(featureValues.stream().distinct().count(), featureValues.size());
    }

    protected ObjectNode getSkipNode(ArrayNode setupNodeValue) {
        Iterator<JsonNode> setupIt = setupNodeValue.elements();
        while (setupIt.hasNext()) {
            JsonNode arrayEntry = setupIt.next();
            if (arrayEntry.isObject()) {
                ObjectNode skipCandidate = (ObjectNode) arrayEntry;
                if (skipCandidate.get("skip") != null) {
                    ObjectNode skipNode = (ObjectNode) skipCandidate.get("skip");
                    return skipNode;
                }
            }
        }
        return null;
    }

    protected void validateBodyHasWarnings(String featureName, List<ObjectNode> tests, Collection<String> expectedWarnings) {
        validateBodyHasWarnings(featureName, null, tests, expectedWarnings);
    }

    protected void validateBodyHasWarnings(String featureName, String testName, List<ObjectNode> tests, Collection<String> expectedWarnings) {
        AtomicBoolean actuallyDidSomething = new AtomicBoolean(false);
        tests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                if (testName == null || testName.equals(testObject.getKey())) {
                    ArrayNode testBody = (ArrayNode) testObject.getValue();
                    testBody.forEach(arrayObject -> {
                        assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
                        ObjectNode testSection = (ObjectNode) arrayObject;
                        if (testSection.get("do") != null) {
                            ObjectNode doSection = (ObjectNode) testSection.get("do");
                            assertThat(doSection.get(featureName), CoreMatchers.notNullValue());
                            ArrayNode warningsNode = (ArrayNode) doSection.get(featureName);
                            List<String> actual  = new ArrayList<>();
                            warningsNode.forEach(node -> actual.add(node.asText()));
                            String[] expected = expectedWarnings.toArray(new String[]{});
                            assertThat(actual, Matchers.containsInAnyOrder(expected));
                            actuallyDidSomething.set(true);
                        }
                    });
                }
            }
        });
        assertTrue(actuallyDidSomething.get());
    }

    protected void validateBodyHasNoWarnings(String featureName, List<ObjectNode> tests) {
        validateBodyHasNoWarnings(featureName, null, tests);
    }

    protected void validateBodyHasNoWarnings(String featureName, String testName, List<ObjectNode> tests) {
        AtomicBoolean actuallyDidSomething = new AtomicBoolean(false);
        tests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                if (testName == null || testName.equals(testObject.getKey())) {
                    assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                    ArrayNode testBody = (ArrayNode) testObject.getValue();
                    testBody.forEach(arrayObject -> {
                        assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
                        ObjectNode testSection = (ObjectNode) arrayObject;
                        if (testSection.get("do") != null) {
                            ObjectNode doSection = (ObjectNode) testSection.get("do");
                            assertThat(doSection.get(featureName), CoreMatchers.nullValue());
                            actuallyDidSomething.set(true);
                        }
                    });
                }
            }
        });
        assertTrue(actuallyDidSomething.get());
    }

    protected void validateBodyHasEmptyNoWarnings(String featureName, List<ObjectNode> tests) {
        tests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                testBody.forEach(arrayObject -> {
                    assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
                    ObjectNode testSection = (ObjectNode) arrayObject;
                    if (testSection.get("do") != null) {
                        ObjectNode doSection = (ObjectNode) testSection.get("do");
                        assertThat(doSection.get(featureName), CoreMatchers.notNullValue());
                        ArrayNode warningsNode = (ArrayNode) doSection.get(featureName);
                        assertTrue(warningsNode.isEmpty());
                    }
                });
            }
        });
    }

    protected void validateBodyHasHeaders(List<ObjectNode> tests, Map<String, String> expectedHeaders) {
        tests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                testBody.forEach(arrayObject -> {
                    assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
                    ObjectNode testSection = (ObjectNode) arrayObject;
                    if (testSection.get("do") != null) {
                        ObjectNode doSection = (ObjectNode) testSection.get("do");
                        assertThat(doSection.get("headers"), CoreMatchers.notNullValue());
                        ObjectNode headersNode = (ObjectNode) doSection.get("headers");
                        LongAdder assertions = new LongAdder();
                        expectedHeaders.forEach((k, v) -> {
                            assertThat(headersNode.get(k), CoreMatchers.notNullValue());
                            TextNode textNode = (TextNode) headersNode.get(k);
                            assertThat(textNode.asText(), CoreMatchers.equalTo(v));
                            assertions.increment();
                        });
                        assertThat(assertions.intValue(), CoreMatchers.equalTo(expectedHeaders.size()));
                    }
                });
            }
        });
    }

    protected void validateSetupAndTearDownForMatchTests(List<ObjectNode> tests) {
        ObjectNode setUp = tests.get(0);
        assertThat(setUp.get("setup"), CoreMatchers.notNullValue());
        ObjectNode tearDown = tests.get(1);
        assertThat(tearDown.get("teardown"), CoreMatchers.notNullValue());
        ObjectNode firstTest = tests.get(2);
        assertThat(firstTest.get("First test"), CoreMatchers.notNullValue());
        ObjectNode lastTest = tests.get(tests.size() - 1);
        assertThat(lastTest.get("Last test"), CoreMatchers.notNullValue());

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
    }

    protected boolean getHumanDebug() {
        return false;
    }

    // only to help manually debug
    protected void printTest(String testName, List<ObjectNode> tests) {
        if (getHumanDebug()) {
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

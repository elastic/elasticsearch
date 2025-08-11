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
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class InjectHeaderTests {

    private static final YAMLFactory yaml = new YAMLFactory();
    private static final ObjectMapper mapper = new ObjectMapper(yaml);
    private static final Map<String, String> headers = Map.of(
        "Content-Type",
        "application/vnd.elasticsearch+json;compatible-with=7",
        "Accept",
        "application/vnd.elasticsearch+json;compatible-with=7"
    );
    private static final boolean humanDebug = false; // useful for humans trying to debug these tests

    /**
     * test file does not have setup: block
     */
    @Test
    public void testInjectHeadersWithoutSetupBlock() throws Exception {
        String testName = "/rest/header_inject/no_setup.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();
        // validate no setup
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(0L));

        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers))
        );
        printTest(testName, transformedTests);
        // ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
        // ensure do body is correct
        transformedTests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                assertTestBodyForHeaders(testBody, headers);
            }
        });
    }

    /**
     * test file has a setup: block, but no relevant children
     */
    @Test
    public void testInjectHeadersWithSetupBlock() throws Exception {
        String testName = "/rest/header_inject/with_setup.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        // validate setup exists
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));

        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers))
        );
        printTest(testName, transformedTests);
        // ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
        // ensure do body is correct
        transformedTests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                assertTestBodyForHeaders(testBody, headers);
            }
        });
    }

    /**
     * test file has a setup: then skip: but does not have the features: block
     */
    @Test
    public void testInjectHeadersWithSkipBlock() throws Exception {
        String testName = "/rest/header_inject/with_skip.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        // validate setup exists
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));

        List<ObjectNode> skipNodes = tests.stream()
            .filter(node -> node.get("setup") != null)
            .filter(node -> getSkipNode((ArrayNode) node.get("setup")) != null)
            .map(node -> getSkipNode((ArrayNode) node.get("setup")))
            .collect(Collectors.toList());

        // validate skip node exists
        assertThat(skipNodes.size(), CoreMatchers.equalTo(1));
        // validate features does not exists
        assertNull(skipNodes.get(0).get("features"));

        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers))
        );
        printTest(testName, transformedTests);
        // ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);

        // ensure do body is correct
        transformedTests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                assertTestBodyForHeaders(testBody, headers);
            }
        });
    }

    /**
     * test file has a setup: then skip:, then features: block , but does not have the headers feature defined
     */
    @Test
    public void testInjectHeadersWithFeaturesBlock() throws Exception {
        String testName = "/rest/header_inject/with_features.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        // validate setup exists
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));

        List<ObjectNode> skipNodes = tests.stream()
            .filter(node -> node.get("setup") != null)
            .filter(node -> getSkipNode((ArrayNode) node.get("setup")) != null)
            .map(node -> getSkipNode((ArrayNode) node.get("setup")))
            .collect(Collectors.toList());

        // validate skip node exists
        assertThat(skipNodes.size(), CoreMatchers.equalTo(1));
        // validate features exists
        assertThat(skipNodes.get(0).get("features"), CoreMatchers.notNullValue());

        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers))
        );
        printTest(testName, transformedTests);
        // ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
        // ensure do body is correct
        transformedTests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                assertTestBodyForHeaders(testBody, headers);
            }
        });
    }

    /**
     * test file has a setup: then skip:, then features: block , and already has the headers feature defined
     */
    @Test
    public void testInjectHeadersWithHeadersBlock() throws Exception {
        String testName = "/rest/header_inject/with_headers.yml";
        File testFile = new File(getClass().getResource(testName).toURI());
        YAMLParser yamlParser = yaml.createParser(testFile);
        List<ObjectNode> tests = mapper.readValues(yamlParser, ObjectNode.class).readAll();
        RestTestTransformer transformer = new RestTestTransformer();

        // validate setup exists
        assertThat(tests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));

        List<ObjectNode> skipNodes = tests.stream()
            .filter(node -> node.get("setup") != null)
            .filter(node -> getSkipNode((ArrayNode) node.get("setup")) != null)
            .map(node -> getSkipNode((ArrayNode) node.get("setup")))
            .collect(Collectors.toList());

        // validate skip node exists
        assertThat(skipNodes.size(), CoreMatchers.equalTo(1));
        // validate features exists
        assertThat(skipNodes.get(0).get("features"), CoreMatchers.notNullValue());

        JsonNode featureValues = skipNodes.get(0).get("features");
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
        // validate that features block has a headers value
        assertThat(features, IsCollectionContaining.hasItem("headers"));

        List<ObjectNode> transformedTests = transformer.transformRestTests(
            new LinkedList<>(tests),
            Collections.singletonList(new InjectHeaders(headers))
        );
        printTest(testName, transformedTests);
        // ensure setup is correct
        assertThat(transformedTests.stream().filter(node -> node.get("setup") != null).count(), CoreMatchers.equalTo(1L));
        transformedTests.stream().filter(node -> node.get("setup") != null).forEach(this::assertSetup);
        transformedTests.stream().filter(node -> node.get("teardown") != null).forEach(this::assertTeardown);
        // ensure do body is correct
        transformedTests.forEach(test -> {
            Iterator<Map.Entry<String, JsonNode>> testsIterator = test.fields();
            while (testsIterator.hasNext()) {
                Map.Entry<String, JsonNode> testObject = testsIterator.next();
                assertThat(testObject.getValue(), CoreMatchers.instanceOf(ArrayNode.class));
                ArrayNode testBody = (ArrayNode) testObject.getValue();
                assertTestBodyForHeaders(testBody, headers);
            }
        });
    }

    private void assertTestBodyForHeaders(ArrayNode testBody, Map<String, String> headers) {
        testBody.forEach(arrayObject -> {
            assertThat(arrayObject, CoreMatchers.instanceOf(ObjectNode.class));
            ObjectNode testSection = (ObjectNode) arrayObject;
            if (testSection.get("do") != null) {
                ObjectNode doSection = (ObjectNode) testSection.get("do");
                assertThat(doSection.get("headers"), CoreMatchers.notNullValue());
                ObjectNode headersNode = (ObjectNode) doSection.get("headers");
                LongAdder assertions = new LongAdder();
                headers.forEach((k, v) -> {
                    assertThat(headersNode.get(k), CoreMatchers.notNullValue());
                    TextNode textNode = (TextNode) headersNode.get(k);
                    assertThat(textNode.asText(), CoreMatchers.equalTo(v));
                    assertions.increment();
                });
                assertThat(assertions.intValue(), CoreMatchers.equalTo(headers.size()));
            }
        });
    }

    private void assertTeardown(ObjectNode teardownNode) {
        assertThat(teardownNode.get("teardown"), CoreMatchers.instanceOf(ArrayNode.class));
        ObjectNode skipNode = getSkipNode((ArrayNode) teardownNode.get("teardown"));
        assertSkipNode(skipNode);
    }

    private void assertSetup(ObjectNode setupNode) {
        assertThat(setupNode.get("setup"), CoreMatchers.instanceOf(ArrayNode.class));
        ObjectNode skipNode = getSkipNode((ArrayNode) setupNode.get("setup"));
        assertSkipNode(skipNode);
    }

    private void assertSkipNode(ObjectNode skipNode) {
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
        assertThat(featureValues, IsCollectionContaining.hasItem("headers"));
        assertEquals(featureValues.stream().distinct().count(), featureValues.size());
    }

    private ObjectNode getSkipNode(ArrayNode setupNodeValue) {
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

    // only to help manually debug
    private void printTest(String testName, List<ObjectNode> tests) {
        if (humanDebug) {
            System.out.println("\n************* " + testName + " *************");
            try (SequenceWriter sequenceWriter = mapper.writer().writeValues(System.out)) {
                for (ObjectNode transformedTest : tests) {
                    sequenceWriter.write(transformedTest);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

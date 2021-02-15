/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test.rest.transform.feature;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformer;
import org.elasticsearch.gradle.test.rest.transform.headers.InjectHeaders;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InjectFeatureTests extends GradleUnitTestCase {

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

    /**
     * test file does not have a setup
     */
    @Test
    public void testInjectFeatureWithoutSetup() throws Exception {
        String testName = "/rest/transform/feature/without_setup.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    }

    /**
     * test file has a single feature
     */
    @Test
    public void testInjectFeatureWithSinglePreexistingFeature() throws Exception {
        String testName = "/rest/transform/feature/with_single_feature.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validatePreExistingFeatureExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    }

    /**
     * test file has multiple feature
     */
    @Test
    public void testInjectFeatureWithMultiplePreexistingFeature() throws Exception {
        String testName = "/rest/transform/feature/with_multiple_feature.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validatePreExistingFeatureExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    }

    /**
     * test file has a setup, but no skip (and by inference no feature)
     */
    @Test
    public void testInjectFeatureWithSetupNoSkip() throws Exception {
        String testName = "/rest/transform/feature/with_setup_no_skip.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateSkipNodesDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    }

    /**
     * test file has a setup, a skip section, but no features
     */
    @Test
    public void testInjectFeatureWithSetupWithSkipNoFeature() throws Exception {
        String testName = "/rest/transform/feature/with_setup_no_feature.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateSkipNodesExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
    }

    /**
     * test file has a single feature
     */
    @Test
    public void testInjectFeatureWithFeatureAlreadyDefined() throws Exception {
        String testName = "/rest/transform/feature/with_feature_predefined.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validatePreExistingFeatureExist(tests);
        validateFeatureNameExists(tests, "headers");
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
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
        List<ObjectNode> t = transformer.transformRestTests(new LinkedList<>(tests), getTransformations());
        getKnownFeatures().forEach(name -> { validateFeatureNameExists(t, name); });
        return t;
    }

    protected List<String> getKnownFeatures() {
        return Collections.singletonList("headers");
    }

    protected List<RestTestTransform<?>> getTransformations() {
        List<RestTestTransform<?>> transformations = new ArrayList<>();
        transformations.add(new InjectHeaders(headers1));
        transformations.add(new InjectHeaders(headers2));
        return transformations;
    }

    protected List<ObjectNode> getTests(String relativePath) throws Exception {
        String testName = relativePath;
        File testFile = new File(getClass().getResource(testName).toURI());
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

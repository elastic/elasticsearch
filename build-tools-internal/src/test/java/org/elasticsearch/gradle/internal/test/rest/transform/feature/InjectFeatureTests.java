/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.feature;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.List;

public class InjectFeatureTests extends TransformTests {

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
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.skip;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.elasticsearch.gradle.internal.test.rest.transform.headers.InjectHeaders;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class SkipTests extends TransformTests {


    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new Skip("my reason"));
    }

    @Override
    protected boolean getHumanDebug() {
        return true;
    }

    /**
     * test file does not have a setup
     */
    @Test
    public void testInjectSkipWithoutSetup() throws Exception {
        String testName = "/rest/transform/skip/without_setup.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSkipVersion(transformedTests, "all");
        validateSkipReason(transformedTests, "my reason");
    }

    /**
     * test file has features existing feature
     */
    @Test
    public void testInjectSkipWithFeatures() throws Exception {
        String testName = "/rest/transform/skip/with_features.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateSkipNodesExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSkipVersion(transformedTests, "all");
        validateSkipReason(transformedTests, "my reason");
    }

    /**
     * test file has a setup, but no skip
     */
    @Test
    public void testInjectSkipWithExistingSkip() throws Exception {
        String testName = "/rest/transform/skip/with_skip.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateSkipNodesExist(tests);
        validateSkipVersionExist(tests);
        validateSkipReasonExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSkipVersionExist(tests);
        validateSkipReasonExist(tests);
        validateSkipVersion(transformedTests, "all");
        validateSkipReason(transformedTests, "my reason");
    }

    /**
     * test file has a setup but no skip
     */
    @Test
    public void testInjectSkipWithSetup() throws Exception {
        String testName = "/rest/transform/feature/with_setup_no_skip.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateSkipNodesDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSkipVersion(transformedTests, "all");
        validateSkipReason(transformedTests, "my reason");
    }

}

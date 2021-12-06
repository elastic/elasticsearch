/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.warnings;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RemoveWarningsTests extends TransformTests {

    private static final String WARNINGS = "warnings";

    /**
     * test file does not any warnings defined
     */
    @Test
    public void testRemoveWarningsNoPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/without_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupDoesNotExist(transformedTests);
        validateBodyHasNoWarnings(WARNINGS, transformedTests);
    }

    /**
     * test file has preexisting multiple warnings
     */
    @Test
    public void testRemoveWarningWithPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(WARNINGS, tests, Set.of("a", "b"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(WARNINGS, tests, Set.of("b"));
    }

    @Test
    public void testRemoveWarningWithPreExistingFromSingleTest() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(WARNINGS, tests, Set.of("a", "b"));
        List<ObjectNode> transformedTests = transformTests(tests, getTransformationsForTest("Test warnings"));
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(WARNINGS, "Test warnings", tests, Set.of("b"));
        validateBodyHasWarnings(WARNINGS, "Not the test to change", tests, Set.of("a", "b"));
    }

    private List<RestTestTransform<?>> getTransformationsForTest(String testName) {
        return Collections.singletonList(new RemoveWarnings(Set.of("a"), testName));
    }

    /**
     * test file has preexisting single warning
     */
    @Test
    public void testRemoveWarningWithSinglePreExisting() throws Exception {
        // For simplicity, when removing the last item, it does not remove the headers/teardown and leaves an empty array
        String testName = "/rest/transform/warnings/with_existing_single_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(WARNINGS, tests, Set.of("a"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasEmptyNoWarnings(WARNINGS, tests);
    }

    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new RemoveWarnings(Set.of("a")));
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

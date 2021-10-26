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
import org.elasticsearch.gradle.internal.test.rest.transform.feature.InjectFeatureTests;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class InjectAllowedWarningsTests extends InjectFeatureTests {

    Set<String> addWarnings = Set.of("added warning");
    private static final String ALLOWED_WARNINGS = "allowed_warnings";

    /**
     * test file does not any allowed warnings defined
     */
    @Test
    public void testInjectAllowedWarningsNoPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/without_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(ALLOWED_WARNINGS, transformedTests, addWarnings);
    }

    /**
     * test file has preexisting allowed warnings
     */
    @Test
    public void testInjectAllowedWarningsWithPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_allowed_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(ALLOWED_WARNINGS, tests, Set.of("a", "b"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(ALLOWED_WARNINGS, transformedTests, List.of("a", "b", "added warning"));
    }

    @Test
    public void testInjectAllowedWarningsWithPreExistingForSingleTest() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_allowed_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(ALLOWED_WARNINGS, tests, Set.of("a", "b"));
        List<ObjectNode> transformedTests = transformTests(tests, getTransformationsForTest("Test with existing allowed warnings"));
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(
            ALLOWED_WARNINGS,
            "Test with existing allowed warnings",
            transformedTests,
            Set.of("a", "b", "added warning")
        );
        validateBodyHasWarnings(ALLOWED_WARNINGS, "Test with existing allowed warnings not to change", transformedTests, Set.of("a", "b"));
    }

    private List<RestTestTransform<?>> getTransformationsForTest(String testName) {
        return Collections.singletonList(new InjectAllowedWarnings(false, new ArrayList<>(addWarnings), testName));
    }

    @Override
    protected List<String> getKnownFeatures() {
        return Collections.singletonList(ALLOWED_WARNINGS);
    }

    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new InjectAllowedWarnings(new ArrayList<>(addWarnings)));
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

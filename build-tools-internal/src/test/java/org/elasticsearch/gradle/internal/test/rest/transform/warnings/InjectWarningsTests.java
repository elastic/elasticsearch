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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InjectWarningsTests extends InjectFeatureTests {
    Set<String> addWarnings = Set.of("added warning");
    private static final String WARNINGS = "warnings";

    /**
     * inject warning requires a test name to insert
     */
    @Test
    public void testInjectWarningsRequiresTestName() throws Exception {
        String testName = "/rest/transform/warnings/without_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        assertEquals(
            "inject warnings is only supported for named tests",
            assertThrows(
                NullPointerException.class,
                () -> transformTests(tests, Collections.singletonList(new InjectWarnings(new ArrayList<>(addWarnings), null)))
            ).getMessage()
        );
    }

    /**
     * test file does not any warnings defined
     */
    @Test
    public void testInjectWarningsNoPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/without_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupDoesNotExist(tests);
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(WARNINGS, "Test warnings", transformedTests, addWarnings);
        validateBodyHasNoWarnings(WARNINGS, "Test another", transformedTests);
    }

    /**
     * test file has preexisting warnings
     */
    @Test
    public void testInjectWarningsWithPreExisting() throws Exception {
        String testName = "/rest/transform/warnings/with_existing_warnings.yml";
        List<ObjectNode> tests = getTests(testName);
        validateSetupExist(tests);
        validateBodyHasWarnings(WARNINGS, tests, Set.of("a", "b"));
        List<ObjectNode> transformedTests = transformTests(tests);
        printTest(testName, transformedTests);
        validateSetupAndTearDown(transformedTests);
        validateBodyHasWarnings(WARNINGS, "Not the test to change", tests, Set.of("a", "b"));
        validateBodyHasWarnings(WARNINGS, "Test warnings", tests, Set.of("a", "b", "added warning"));
    }

    @Override
    protected List<String> getKnownFeatures() {
        return Collections.singletonList(WARNINGS);
    }

    @Override
    protected List<RestTestTransform<?>> getTransformations() {
        return Collections.singletonList(new InjectWarnings(new ArrayList<>(addWarnings), "Test warnings"));
    }

    @Override
    protected boolean getHumanDebug() {
        return false;
    }
}

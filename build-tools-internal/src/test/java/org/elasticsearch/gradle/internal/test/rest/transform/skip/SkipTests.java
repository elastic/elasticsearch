/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rest.transform.skip;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.elasticsearch.gradle.internal.test.rest.transform.AssertObjectNodes;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.elasticsearch.gradle.internal.test.rest.transform.match.ReplaceKeyInMatch;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class SkipTests extends TransformTests {

//
//    @Override
//    protected List<RestTestTransform<?>> getTransformations() {
//        return Collections.singletonList(new Skip("my reason"));
//    }

    @Override
    protected boolean getHumanDebug() {
        return true;
    }



    @Test
    public void testAddGlobalSetup() throws Exception {
        String test_original = "/rest/transform/skip/without_setup_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/without_setup_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new Skip("my reason"))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithSkip() throws Exception {
        String test_original = "/rest/transform/skip/without_setup_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/without_setup_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new Skip("my reason"))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithoutSkip() throws Exception {
        String test_original = "/rest/transform/skip/with_setup_no_skip_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/with_setup_no_skip_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new Skip("my reason"))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithFeatures() throws Exception {
        String test_original = "/rest/transform/skip/with_features_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/with_features_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            Collections.singletonList(new Skip("my reason"))
        );

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyPerTestSetup() throws Exception {
        String test_original = "/rest/transform/skip/per_test_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/per_test_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(
            tests,
            List.of(new Skip("Two Test", "my reason"), new Skip("Three Test", "another reason"))
        );

printTest("x", transformedTests);
        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }


//    /**
//     * test file does not have a setup
//     */
//    @Test
//    public void testInjectSkipWithoutSetup() throws Exception {
//        String testName = "/rest/transform/skip/without_setup_original.yml";
//        List<ObjectNode> tests = getTests(testName);
//        validateSetupDoesNotExist(tests);
//        List<ObjectNode> transformedTests = transformTests(tests);
//        printTest(testName, transformedTests);
//        validateGlobalSkipVersion(transformedTests, "all");
//        validateGlobalSkipReason(transformedTests, "my reason");
//    }
//
//    /**
//     * test file has features existing feature
//     */
//    @Test
//    public void testInjectSkipWithFeatures() throws Exception {
//        String testName = "/rest/transform/skip/with_features_original.yml";
//        List<ObjectNode> tests = getTests(testName);
//        validateSetupExist(tests);
//        validateGlobalSkipNodesExist(tests);
//        List<ObjectNode> transformedTests = transformTests(tests);
//        printTest(testName, transformedTests);
//        validateGlobalSkipVersion(transformedTests, "all");
//        validateGlobalSkipReason(transformedTests, "my reason");
//    }
//
//    /**
//     * test file has a setup, but no skip
//     */
//    @Test
//    public void testInjectSkipWithExistingSkip() throws Exception {
//        String testName = "/rest/transform/skip/with_skip_original.yml";
//        List<ObjectNode> tests = getTests(testName);
//        validateSetupExist(tests);
//        validateGlobalSkipNodesExist(tests);
//        validateGlobalSkipVersionExist(tests);
//        validateGlobalSkipReasonExist(tests);
//        List<ObjectNode> transformedTests = transformTests(tests);
//        printTest(testName, transformedTests);
//        validateGlobalSkipVersionExist(tests);
//        validateGlobalSkipReasonExist(tests);
//        validateGlobalSkipVersion(transformedTests, "all");
//        validateGlobalSkipReason(transformedTests, "my reason");
//    }
//
//    /**
//     * test file has a setup but no skip
//     */
//    @Test
//    public void testInjectSkipWithSetup() throws Exception {
//        String testName = "/rest/transform/feature/with_setup_no_skip_original.yml";
//        List<ObjectNode> tests = getTests(testName);
//        validateSetupExist(tests);
//        validateGlobalSkipNodesDoesNotExist(tests);
//        List<ObjectNode> transformedTests = transformTests(tests);
//        printTest(testName, transformedTests);
//        validateGlobalSkipVersion(transformedTests, "all");
//        validateGlobalSkipReason(transformedTests, "my reason");
//    }

}

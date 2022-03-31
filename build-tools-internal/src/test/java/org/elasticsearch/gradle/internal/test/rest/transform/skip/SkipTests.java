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
import org.elasticsearch.gradle.internal.test.rest.transform.TransformTests;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class SkipTests extends TransformTests {

    @Test
    public void testAddGlobalSetup() throws Exception {
        String test_original = "/rest/transform/skip/without_setup_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/without_setup_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new Skip("my reason")));

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithSkip() throws Exception {
        String test_original = "/rest/transform/skip/without_setup_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/without_setup_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new Skip("my reason")));

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithoutSkip() throws Exception {
        String test_original = "/rest/transform/skip/with_setup_no_skip_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/with_setup_no_skip_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new Skip("my reason")));

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

    @Test
    public void testModifyGlobalSetupWithFeatures() throws Exception {
        String test_original = "/rest/transform/skip/with_features_original.yml";
        List<ObjectNode> tests = getTests(test_original);

        String test_transformed = "/rest/transform/skip/with_features_transformed.yml";
        List<ObjectNode> expectedTransformation = getTests(test_transformed);

        List<ObjectNode> transformedTests = transformTests(tests, Collections.singletonList(new Skip("my reason")));

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

        AssertObjectNodes.areEqual(transformedTests, expectedTransformation);
    }

}

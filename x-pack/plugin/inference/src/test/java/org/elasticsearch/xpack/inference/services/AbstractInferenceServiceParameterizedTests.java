/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.junit.Ignore;

/**
 * Base class for testing inference services using parameterized tests.
 */
public abstract class AbstractInferenceServiceParameterizedTests extends AbstractInferenceServiceBaseTests {

    public AbstractInferenceServiceParameterizedTests(AbstractInferenceServiceBaseTests.TestConfiguration testConfiguration) {
        super(testConfiguration);
    }

    @Ignore("Test is ignored to avoid multiple executions in parameterized tests")
    public void testRerankersImplementRerankInterface() {
        fail("""
            Parameterized tests are creating separate test instances per test case, so to avoid the test being run multiple times, \
            this test is disabled here. Reranking services should be tested in their own test class extending InferenceServiceTestCase.""");
    }

    @Ignore("Test is ignored to avoid multiple executions in parameterized tests")
    public void testRerankersHaveWindowSize() {
        fail("""
            Parameterized tests are creating separate test instances per test case, so to avoid the test being run multiple times, \
            this test is disabled here. Reranking services should be tested in their own test class extending InferenceServiceTestCase.""");
    }
}

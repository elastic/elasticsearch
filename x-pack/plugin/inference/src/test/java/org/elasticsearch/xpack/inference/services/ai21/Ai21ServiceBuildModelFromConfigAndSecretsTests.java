/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.inference.services.AbstractBuildModelFromConfigAndSecretsTests;

public class Ai21ServiceBuildModelFromConfigAndSecretsTests extends AbstractBuildModelFromConfigAndSecretsTests {
    public Ai21ServiceBuildModelFromConfigAndSecretsTests(TestCase testCase) {
        super(Ai21ServiceParameterizedTestConfiguration.createTestConfiguration(), testCase);
    }

    @ParametersFactory
    public static Iterable<TestCase[]> testParameters() {
        return parameters();
    }
}

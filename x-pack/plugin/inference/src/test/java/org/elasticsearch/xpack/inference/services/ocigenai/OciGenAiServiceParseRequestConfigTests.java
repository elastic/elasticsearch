/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.inference.services.AbstractParseRequestConfigTests;

public class OciGenAiServiceParseRequestConfigTests extends AbstractParseRequestConfigTests {
    public OciGenAiServiceParseRequestConfigTests(TestCase testCase) {
        super(OciGenAiServiceParameterizedTestConfiguration.createTestConfiguration(), testCase);
    }

    @ParametersFactory
    public static Iterable<TestCase[]> testParameters() {
        return parameters();
    }
}

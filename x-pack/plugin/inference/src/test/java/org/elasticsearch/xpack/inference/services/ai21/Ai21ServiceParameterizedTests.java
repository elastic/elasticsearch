/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;

import static org.elasticsearch.xpack.inference.services.ai21.Ai21ServiceTests.createTestConfiguration;

public class Ai21ServiceParameterizedTests extends AbstractInferenceServiceParameterizedTests {
    public Ai21ServiceParameterizedTests(AbstractInferenceServiceParameterizedTests.TestCase testCase) {
        super(createTestConfiguration(), testCase);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedModelCreationTests;

public class Ai21ServiceParameterizedModelCreationTests extends AbstractInferenceServiceParameterizedModelCreationTests {
    public Ai21ServiceParameterizedModelCreationTests(TestCase testCase) {
        super(Ai21ServiceTests.createTestConfiguration(), testCase);
    }
}

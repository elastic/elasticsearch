/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.xpack.inference.services.AbstractInferenceServiceParameterizedTests;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceTests.createTestConfiguration;

public class OpenAiServiceParameterizedTests extends AbstractInferenceServiceParameterizedTests {
    public OpenAiServiceParameterizedTests(AbstractInferenceServiceParameterizedTests.TestCase testCase) {
        super(createTestConfiguration(), testCase);
    }
}

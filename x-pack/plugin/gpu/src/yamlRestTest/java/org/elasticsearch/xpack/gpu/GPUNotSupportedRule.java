/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Inverse of {@link GPUSupportedRule}: skips tests when GPU hardware IS available.
 * Used for tests that validate behaviour on machines without GPU support.
 */
public class GPUNotSupportedRule implements TestRule {
    private static final Logger log = LogManager.getLogger(GPUNotSupportedRule.class);

    @Override
    public Statement apply(Statement base, Description description) {
        if (CuVSGPUSupport.instance().isSupported()) {
            return new Statement() {
                @Override
                public void evaluate() {
                    log.info("GPU supported, skipping {}", description.getDisplayName());
                }
            };
        }
        return base;
    }
}

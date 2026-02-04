/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class GPUSupportedRule implements TestRule {
    private static final Logger log = LogManager.getLogger(GPUSupportedRule.class);

    @Override
    public Statement apply(Statement base, Description description) {
        if (GPUSupport.isSupported() == false) {
            return new Statement() {
                @Override
                public void evaluate() {
                    log.info("GPU not supported, skipping {}", description.getDisplayName());
                }
            };
        }
        return base;
    }
}

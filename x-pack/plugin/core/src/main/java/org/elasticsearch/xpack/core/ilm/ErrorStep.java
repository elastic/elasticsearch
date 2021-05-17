/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

/**
 * Signals that an error was encountered during the execution of a policy on an index.
 */
public class ErrorStep extends Step {
    public static final String NAME = "ERROR";

    public ErrorStep(StepKey key) {
        super(key, key);
        if (NAME.equals(key.getName()) == false) {
            throw new IllegalArgumentException("An error step must have a step key whose step name is " + NAME);
        }
    }

    @Override
    public boolean isRetryable() {
        // this is marker step so it doesn't make sense to be retryable
        return false;
    }
}

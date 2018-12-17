/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import java.util.Objects;

import org.elasticsearch.bootstrap.FIPSContext;
import org.elasticsearch.env.Environment;

/**
 * Encapsulates a FIPS check.
 */
public interface FIPSInterface {

    /**
     * Encapsulate the result of a FIPS check.
     */
    final class FIPSCheckResult {

        private final String message;

        private static final FIPSCheckResult SUCCESS = new FIPSCheckResult(null);

        public static FIPSCheckResult success() {
            return SUCCESS;
        }

        public static FIPSCheckResult failure(final String message) {
            Objects.requireNonNull(message);
            return new FIPSCheckResult(message);
        }

        private FIPSCheckResult(final String message) {
            this.message = message;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public boolean isFailure() {
            return !isSuccess();
        }

        public String getMessage() {
            assert isFailure();
            assert message != null;
            return message;
        }

    }

    /**
     * Test if the node fails the check
     * @param context the FIPS context
     * @return the result of the FIPS check
     */
    FIPSCheckResult check(FIPSContext context, Environment env);
}

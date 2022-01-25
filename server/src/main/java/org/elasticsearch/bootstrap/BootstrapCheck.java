/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import java.util.Objects;

/**
 * Encapsulates a bootstrap check.
 */
public interface BootstrapCheck {

    /**
     * Encapsulate the result of a bootstrap check.
     */
    record BootstrapCheckResult(String message) {

        private static final BootstrapCheckResult SUCCESS = new BootstrapCheckResult(null);

        public static BootstrapCheckResult success() {
            return SUCCESS;
        }

        public static BootstrapCheckResult failure(final String message) {
            Objects.requireNonNull(message);
            return new BootstrapCheckResult(message);
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public boolean isFailure() {
            return isSuccess() == false;
        }

        public String getMessage() {
            assert isFailure();
            assert message != null;
            return message;
        }

    }

    /**
     * Test if the node fails the check.
     *
     * @param context the bootstrap context
     * @return the result of the bootstrap check
     */
    BootstrapCheckResult check(BootstrapContext context);

    default boolean alwaysEnforce() {
        return false;
    }

}

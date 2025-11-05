/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.ReferenceDocs;

import java.util.Objects;

/**
 * Encapsulates a bootstrap check performed during Elasticsearch node startup.
 * <p>
 * Bootstrap checks are validation tests run when Elasticsearch starts to ensure the node
 * is configured properly for production use. These checks verify system settings, resource
 * limits, and other prerequisites required for reliable operation.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * public class CustomBootstrapCheck implements BootstrapCheck {
 *     &#64;Override
 *     public BootstrapCheckResult check(BootstrapContext context) {
 *         if (isConfigurationValid(context)) {
 *             return BootstrapCheckResult.success();
 *         }
 *         return BootstrapCheckResult.failure("Configuration is invalid");
 *     }
 *
 *     &#64;Override
 *     public ReferenceDocs referenceDocs() {
 *         return ReferenceDocs.BOOTSTRAP_CHECKS;
 *     }
 * }
 * }</pre>
 */
public interface BootstrapCheck {

    /**
     * Encapsulates the result of a bootstrap check.
     * <p>
     * A result can either be a success (with no message) or a failure (with a descriptive
     * error message explaining why the check failed).
     */
    record BootstrapCheckResult(String message) {

        private static final BootstrapCheckResult SUCCESS = new BootstrapCheckResult(null);

        /**
         * Creates a successful bootstrap check result.
         *
         * @return a success result
         */
        public static BootstrapCheckResult success() {
            return SUCCESS;
        }

        /**
         * Creates a failed bootstrap check result with an error message.
         *
         * @param message the failure message explaining why the check failed
         * @return a failure result with the provided message
         * @throws NullPointerException if message is null
         */
        public static BootstrapCheckResult failure(final String message) {
            Objects.requireNonNull(message);
            return new BootstrapCheckResult(message);
        }

        /**
         * Checks if this result represents a successful bootstrap check.
         *
         * @return true if the check succeeded, false otherwise
         */
        public boolean isSuccess() {
            return this == SUCCESS;
        }

        /**
         * Checks if this result represents a failed bootstrap check.
         *
         * @return true if the check failed, false otherwise
         */
        public boolean isFailure() {
            return isSuccess() == false;
        }

        /**
         * Returns the failure message for this result.
         * <p>
         * This method should only be called on failure results.
         *
         * @return the failure message
         */
        public String getMessage() {
            assert isFailure();
            assert message != null;
            return message;
        }

    }

    /**
     * Tests if the node passes this bootstrap check.
     * <p>
     * This method performs the actual validation logic and returns a result indicating
     * whether the check passed or failed. If failed, the result should include a
     * descriptive message explaining the problem.
     *
     * @param context the bootstrap context containing environment and metadata
     * @return the result of the bootstrap check
     */
    BootstrapCheckResult check(BootstrapContext context);

    /**
     * Indicates whether this check should always be enforced, even in development mode.
     * <p>
     * By default, most bootstrap checks are only enforced in production mode. Checks that
     * return true from this method will be enforced regardless of the node's mode.
     *
     * @return true if this check should always be enforced, false for production-only enforcement
     */
    default boolean alwaysEnforce() {
        return false;
    }

    /**
     * Returns the reference documentation for this bootstrap check.
     * <p>
     * This provides users with a link to documentation explaining why the check failed
     * and how to resolve the issue.
     *
     * @return the reference documentation for this check
     */
    ReferenceDocs referenceDocs();

}

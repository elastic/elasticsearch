/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import org.elasticsearch.entitlement.bridge.NotEntitledException;

import java.util.function.Function;

/**
 * Defines strategies for handling failed entitlement checks during method instrumentation.
 * <p>
 * When an entitlement check fails for an instrumented method, the strategy determines
 * what action to take. This sealed hierarchy provides several built-in strategies:
 * <ul>
 *   <li>{@link NotEntitledDeniedEntitlementStrategy} - throws a {@link NotEntitledException}</li>
 *   <li>{@link ExceptionDeniedEntitlementStrategy} - throws a custom exception</li>
 *   <li>{@link DefaultValueDeniedEntitlementStrategy} - returns a predefined default value</li>
 *   <li>{@link MethodArgumentValueDeniedEntitlementStrategy} - returns one of the method's arguments</li>
 *   <li>{@link ReturnEarlyDeniedEntitlementStrategy} - returns early, making the method a no-op</li>
 * </ul>
 */
public abstract sealed class DeniedEntitlementStrategy permits DeniedEntitlementStrategy.DefaultValueDeniedEntitlementStrategy,
    DeniedEntitlementStrategy.ExceptionDeniedEntitlementStrategy, DeniedEntitlementStrategy.MethodArgumentValueDeniedEntitlementStrategy,
    DeniedEntitlementStrategy.ReturnEarlyDeniedEntitlementStrategy, DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy {

    /**
     * Strategy that throws a {@link NotEntitledException} when an entitlement check fails.
     * <p>
     * This is the default strategy and provides explicit notification that an operation
     * was denied due to missing entitlements.
     */
    public static final class NotEntitledDeniedEntitlementStrategy extends DeniedEntitlementStrategy {}

    /**
     * Strategy that returns a predefined default value when an entitlement check fails.
     * <p>
     * This strategy is useful for methods that return values, allowing them to gracefully
     * degrade functionality rather than throwing exceptions.
     *
     * @param <T> the type of the default value
     */
    public static final class DefaultValueDeniedEntitlementStrategy<T> extends DeniedEntitlementStrategy {
        private final T defaultValue;

        /**
         * Creates a strategy that returns the specified default value on denial.
         *
         * @param defaultValue the value to return when the entitlement check fails
         */
        public DefaultValueDeniedEntitlementStrategy(T defaultValue) {
            this.defaultValue = defaultValue;
        }

        /**
         * Gets the default value to return on denial.
         *
         * @return the default value
         */
        public T getDefaultValue() {
            return defaultValue;
        }
    }

    /**
     * Strategy that returns the value of a specified method argument when an entitlement check fails.
     * <p>
     * This strategy is useful for identity-like methods where returning the input argument
     * provides graceful degradation (e.g., a method that normalizes a path might just return
     * the original path when denied).
     */
    public static final class MethodArgumentValueDeniedEntitlementStrategy extends DeniedEntitlementStrategy {
        private final int index;

        /**
         * Creates a strategy that returns the argument at the specified index on denial.
         *
         * @param index the zero-based index of the method argument to return
         */
        public MethodArgumentValueDeniedEntitlementStrategy(int index) {
            this.index = index;
        }

        /**
         * Gets the index of the method argument to return on denial.
         *
         * @return the argument index
         */
        public int getIndex() {
            return index;
        }
    }

    /**
     * Strategy that throws a custom exception when an entitlement check fails.
     * <p>
     * This strategy allows converting the {@link NotEntitledException} into a more
     * specific exception type appropriate for the API being instrumented.
     */
    public static final class ExceptionDeniedEntitlementStrategy extends DeniedEntitlementStrategy {
        private final Function<NotEntitledException, ? extends Exception> exceptionSupplier;

        /**
         * Creates a strategy that throws a custom exception on denial.
         *
         * @param exceptionSupplier a function that creates the exception to throw,
         *                          receiving the original {@link NotEntitledException} as input
         */
        public ExceptionDeniedEntitlementStrategy(Function<NotEntitledException, ? extends Exception> exceptionSupplier) {
            this.exceptionSupplier = exceptionSupplier;
        }

        /**
         * Gets the function that creates the exception to throw on denial.
         *
         * @return the exception supplier function
         */
        public Function<NotEntitledException, ? extends Exception> getExceptionSupplier() {
            return exceptionSupplier;
        }
    }

    /**
     * Strategy that returns early from the method when an entitlement check fails.
     * <p>
     * This strategy is applicable only to void methods. It causes the method to return
     * immediately without executing its body, effectively turning the method into a no-op
     * when the entitlement check fails.
     */
    public static final class ReturnEarlyDeniedEntitlementStrategy extends DeniedEntitlementStrategy {}
}

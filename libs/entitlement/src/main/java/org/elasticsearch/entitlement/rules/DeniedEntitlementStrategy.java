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

public sealed class DeniedEntitlementStrategy permits DeniedEntitlementStrategy.DefaultValueDeniedEntitlementStrategy,
    DeniedEntitlementStrategy.ExceptionDeniedEntitlementStrategy, DeniedEntitlementStrategy.NoopDeniedEntitlementStrategy,
    DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy {

    /**
     * An {@link DeniedEntitlementStrategy} instructing that a failed entitlement check should result in a {@link NotEntitledException}.
     */
    public static final class NotEntitledDeniedEntitlementStrategy extends DeniedEntitlementStrategy {}

    /**
     * An {@link DeniedEntitlementStrategy} that returns a default value when an entitlement check fails.
     * @param <T> the type of the default value
     */
    public static final class DefaultValueDeniedEntitlementStrategy<T> extends DeniedEntitlementStrategy {
        private final T defaultValue;

        public DefaultValueDeniedEntitlementStrategy(T defaultValue) {
            this.defaultValue = defaultValue;
        }

        public T getDefaultValue() {
            return defaultValue;
        }
    }

    /**
     * An {@link DeniedEntitlementStrategy} that throws a specified exception when an entitlement check fails.
     */
    public static final class ExceptionDeniedEntitlementStrategy extends DeniedEntitlementStrategy {
        private final Function<NotEntitledException, ? extends Exception> exceptionSupplier;

        public ExceptionDeniedEntitlementStrategy(Function<NotEntitledException, ? extends Exception> exceptionSupplier) {
            this.exceptionSupplier = exceptionSupplier;
        }

        public Function<NotEntitledException, ? extends Exception> getExceptionSupplier() {
            return exceptionSupplier;
        }
    }

    /**
     * An {@link DeniedEntitlementStrategy} that returns early when an entitlement check fails. This effectively turns the instrumented
     * method into a no-op in the case of a failed entitlement check.
     */
    public static final class NoopDeniedEntitlementStrategy extends DeniedEntitlementStrategy {}
}

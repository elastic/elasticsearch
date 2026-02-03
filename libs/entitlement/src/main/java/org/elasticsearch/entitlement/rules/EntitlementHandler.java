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

public sealed class EntitlementHandler permits EntitlementHandler.DefaultValueEntitlementHandler,
    EntitlementHandler.ExceptionEntitlementHandler, EntitlementHandler.NotEntitledEntitlementHandler {

    /**
     * An {@link EntitlementHandler} instructing that a failed entitlement check should result in a {@link NotEntitledException}.
     */
    public static final class NotEntitledEntitlementHandler extends EntitlementHandler {}

    /**
     * An {@link EntitlementHandler} that returns a default value when an entitlement check fails.
     * @param <T> the type of the default value
     */
    public static final class DefaultValueEntitlementHandler<T> extends EntitlementHandler {
        private final T defaultValue;

        public DefaultValueEntitlementHandler(T defaultValue) {
            this.defaultValue = defaultValue;
        }

        public T getDefaultValue() {
            return defaultValue;
        }
    }

    /**
     * An {@link EntitlementHandler} that throws a specified exception when an entitlement check fails.
     */
    public static final class ExceptionEntitlementHandler extends EntitlementHandler {
        private final Function<NotEntitledException, ? extends Exception> exceptionSupplier;

        public ExceptionEntitlementHandler(Function<NotEntitledException, ? extends Exception> exceptionSupplier) {
            this.exceptionSupplier = exceptionSupplier;
        }

        public Function<NotEntitledException, ? extends Exception> getExceptionSupplier() {
            return exceptionSupplier;
        }
    }
}

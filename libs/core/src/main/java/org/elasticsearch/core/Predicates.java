/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * Utilities around predicates.
 */
public enum Predicates {
    ;

    @SuppressWarnings("rawtypes")
    private static final Predicate NEVER = new Predicate() {
        @Override
        public boolean test(Object o) {
            return false;
        }

        @Override
        public Predicate and(Predicate other) {
            return this;
        }

        @Override
        public Predicate negate() {
            return ALWAYS;
        }

        @Override
        public Predicate or(Predicate other) {
            return other;
        }

        @Override
        public String toString() {
            return "Predicate[NEVER]";
        }
    };

    @SuppressWarnings("rawtypes")
    private static final Predicate ALWAYS = new Predicate() {
        @Override
        public boolean test(Object o) {
            return true;
        }

        @Override
        public Predicate and(Predicate other) {
            return other;
        }

        @Override
        public Predicate negate() {
            return NEVER;
        }

        @Override
        public Predicate or(Predicate other) {
            return this;
        }

        @Override
        public String toString() {
            return "Predicate[ALWAYS]";
        }
    };

    /**
     * @return a predicate that accepts all input values
     * @param <T> type of the predicate
     */
    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> always() {
        return (Predicate<T>) ALWAYS;
    }

    /**
     * @return a predicate that rejects all input values
     * @param <T> type of the predicate
     */
    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> never() {
        return (Predicate<T>) NEVER;
    }

    private static class OnceTrue extends AtomicBoolean implements BooleanSupplier {
        OnceTrue() {
            super(true);
        }

        @Override
        public boolean getAsBoolean() {
            return getAndSet(false);
        }
    }

    /**
     * @return a {@link BooleanSupplier} which supplies {@code true} the first time it is called, and {@code false} subsequently.
     */
    public static BooleanSupplier once() {
        return new OnceTrue();
    }
}

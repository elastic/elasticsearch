/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.CheckedSupplier;

import java.util.Optional;

/**
 * A wrapper around either
 * <ul>
 * <li>a successful result of parameterized type {@code V}</li>
 * <li>a failure with exception type {@code E}</li>
 * </ul>
 */
public abstract class Result<V, E extends Exception> implements CheckedSupplier<V, E> {

    public static <V, E extends Exception> Result<V, E> of(V value) {
        return new Success<>(value);
    }

    public static <V, E extends Exception> Result<V, E> failure(E exception) {
        return new Failure<>(exception);
    }

    private Result() {}

    public abstract V get() throws E;

    public abstract Optional<E> failure();

    public abstract boolean isSuccessful();

    public boolean isFailure() {
        return isSuccessful() == false;
    };

    public abstract Optional<V> asOptional();

    private static class Success<V, E extends Exception> extends Result<V, E> {
        private final V value;

        Success(V value) {
            this.value = value;
        }

        @Override
        public V get() throws E {
            return value;
        }

        @Override
        public Optional<E> failure() {
            return Optional.empty();
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }

        @Override
        public Optional<V> asOptional() {
            return Optional.of(value);
        }
    }

    private static class Failure<V, E extends Exception> extends Result<V, E> {
        private final E exception;

        Failure(E exception) {
            this.exception = exception;
        }

        @Override
        public V get() throws E {
            throw exception;
        }

        @Override
        public Optional<E> failure() {
            return Optional.of(exception);
        }

        @Override
        public boolean isSuccessful() {
            return false;
        }

        @Override
        public Optional<V> asOptional() {
            return Optional.empty();
        }
    }
}

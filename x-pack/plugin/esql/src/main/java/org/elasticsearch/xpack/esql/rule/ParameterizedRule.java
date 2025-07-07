/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.tree.Node;

public interface ParameterizedRule<E extends T, T extends Node<T>, P> extends Rule<E, T> {

    void apply(T t, P p, ActionListener<T> listener);

    /**
     * Abstract base class for asynchronous parameterized rules that use ActionListener callbacks.
     * This follows the same pattern as Rule.Async but adds parameter support.
     */
    abstract class Async<E extends T, T extends Node<T>, P> extends Rule.Async<E, T> implements ParameterizedRule<E, T, P> {

        protected Async() {
            super();
        }

        protected Async(String name) {
            super(name);
        }

        public abstract void apply(T t, P p, ActionListener<T> listener);

        @Override
        public final void apply(T t, ActionListener<T> listener) {
            listener.onFailure(new RuleExecutionException("Cannot call parameterized rule without parameter"));
        }
    }

    /**
     * Abstract base class for synchronous parameterized rules that return results directly.
     * The RuleExecutor will wrap these in async callbacks when executing.
     */
    abstract class Sync<E extends T, T extends Node<T>, P> extends Async<E, T, P> {

        protected Sync() {
            this(null);
        }

        protected Sync(String name) {
            super(name);
        }

        @Override
        public final void apply(T t, P p, ActionListener<T> listener) {
            try {
                T result = apply(t, p);
                listener.onResponse(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        /**
         * Synchronous apply method to be implemented by subclasses.
         */
        public abstract T apply(T t, P p);
    }
}

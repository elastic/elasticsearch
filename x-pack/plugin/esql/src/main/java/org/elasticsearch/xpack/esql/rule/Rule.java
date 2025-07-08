/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.rule;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.util.ReflectionUtils;

/**
 * Rules that apply transformation to a tree. In addition, performs
 * type filtering so that a rule that the rule implementation doesn't
 * have to manually filter.
 * <p>
 * Rules <strong>could</strong> could be built as lambdas but most
 * rules are much larger, so we keep them as full-blown subclasses.
 */
public interface Rule<E extends T, T extends Node<T>> {

    Class<E> typeToken();

    String name();

    void apply(T t, ActionListener<T> listener);

    /**
     * Abstract base class for asynchronous rules that use ActionListener callbacks.
     * This is the current implementation pattern for rules.
     */
    abstract class Async<E extends T, T extends Node<T>> implements Rule<E, T> {

        protected Logger log = LogManager.getLogger(getClass());

        private final String name;
        private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

        protected Async() {
            this(null);
        }

        protected Async(String name) {
            this.name = (name == null ? ReflectionUtils.ruleLikeNaming(getClass()) : name);
        }

        @Override
        public Class<E> typeToken() {
            return typeToken;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String toString() {
            return name();
        }

        @Override
        public abstract void apply(T t, ActionListener<T> listener);
    }

    /**
     * Abstract base class for synchronous rules that return results directly.
     * The RuleExecutor will wrap these in async callbacks when executing.
     */
    abstract class Sync<E extends T, T extends Node<T>> extends Async<E, T> {

        protected Sync() {
            this(null);
        }

        protected Sync(String name) {
            super(name);
        }

        @Override
        public final void apply(T t, ActionListener<T> listener) {
            try {
                T result = apply(t);
                listener.onResponse(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        /**
         * Synchronous apply method to be implemented by subclasses.
         */
        public abstract T apply(T t);
    }
}

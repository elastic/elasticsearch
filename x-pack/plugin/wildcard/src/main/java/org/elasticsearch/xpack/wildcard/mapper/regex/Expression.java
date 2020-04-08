/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

import java.util.Set;

/**
 * Immutable representation of some logic. Expressions can be simplified and
 * transformed. Simplifying expressions eliminates extraneous terms and factors
 * out common terms.  Transformation allows client code to convert the expression
 * to some other (maybe evaluable) form.
 *
 * @param <T> type stored in leaves
 */
public interface Expression<T> {
    /**
     * Returns a simplified copy of this expression. If the simplification
     * didn't change anything then returns this.
     */
    Expression<T> simplify();

    /**
     * Is this node in the expression always true? Note that this only
     * represents _this_ node of the expression. To know if the entire
     * expression is always true of false first {@link Expression#simplify()}
     * it.
     */
    boolean alwaysTrue();

    /**
     * Is this node in the expression always false? Note that this only
     * represents _this_ node of the expression. To know if the entire
     * expression is always true of false first {@link Expression#simplify()}
     * it.
     */
    boolean alwaysFalse();

    /**
     * Is this expression made of many subexpressions?
     */
    boolean isComposite();

    /**
     * Transform this expression into another form.
     *
     * @param <J> result of the transformation.
     */
    <J> J transform(Transformer<T, J> transformer);

    /**
     * Transformer for expression components.
     *
     * @param <T> type stored in leaves
     * @param <J> result of the transformation.
     */
    interface Transformer<T, J> {
        /**
         * Transform an expression that is always true.
         */
        J alwaysTrue();

        /**
         * Transform an expression that is always false.
         */
        J alwaysFalse();

        /**
         * Transform a leaf expression.
         *
         * @param t data stored in the leaf
         * @return result of the transform
         */
        J leaf(T t);

        /**
         * Transform an and expression.
         *
         * @param js transformed sub-expressions
         * @return result of the transform
         */
        J and(Set<J> js);

        /**
         * Transform an or expression.
         *
         * @param js transformed sub-expressions
         * @return result of the transform
         */
        J or(Set<J> js);
    }
}

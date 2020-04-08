/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper.regex;

/**
 * Type that can be expressed as an expression.
 *
 * @param <T> type stored in leaves
 */
public interface ExpressionSource<T> {
    /**
     * This expressed as an expression. The result might not be simplified so
     * call simplify on it if you need it simplified.
     */
    Expression<T> expression();
}

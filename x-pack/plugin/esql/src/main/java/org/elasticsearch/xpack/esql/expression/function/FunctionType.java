/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

/**
 * The position the function can appear in the language.
 */
public enum FunctionType {
    /**
     * Functions that can appear anywhere. For example, {@code LENGTH} in
     * {@code | STATS MAX(LENGTH(string))} and {@code | EVAL l = LENGTH(string)}.
     */
    SCALAR,
    /**
     * Functions that can only appear in the "aggregate" position of a {@code STATS}.
     * For example, {@code MAX} in {@code | STATS MAX(LENGTH(string))}.
     */
    AGGREGATE,
    /**
     * Functions that can only appear in the "grouping" position of a {@code STATS}.
     * For example, {@code CATEGORIZE} in {@code | STATS MAX(a) BY CATEGORIZE(message)}.
     */
    GROUPING,
}

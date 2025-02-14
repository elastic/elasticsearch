/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Strategy indicating the type of resolution to apply for resolving the actual function definition in a pluggable way.
 */
public interface FunctionResolutionStrategy {

    /**
     * Default behavior of standard function calls like {@code ABS(col)}.
     */
    FunctionResolutionStrategy DEFAULT = new FunctionResolutionStrategy() {
    };

    /**
     * Build the real function from this one and resolution metadata.
     */
    default Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
        return def.builder().build(uf, cfg);
    }

    /**
     * The kind of strategy being applied. Used when
     * building the error message sent back to the user when
     * they specify a function that doesn't exist.
     */
    default String kind() {
        return "function";
    }

    /**
     * Is {@code def} a valid alternative for function invocations
     * of this kind. Used to filter the list of "did you mean"
     * options sent back to the user when they specify a missing
     * function.
     */
    default boolean isValidAlternative(FunctionDefinition def) {
        return true;
    }
}

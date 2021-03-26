/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function;

import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.session.Configuration;

public enum EqlFunctionResolution implements FunctionResolutionStrategy {

    /**
     * Indicate the function should be insensitive {@code startwith~()},
     */
    CASE_INSENSITIVE {
        @Override
        public Function buildResolved(UnresolvedFunction uf, Configuration cfg, FunctionDefinition def) {
            if (def instanceof EqlFunctionDefinition) {
                return ((EqlFunctionDefinition) def).builder().build(uf, cfg, true);
            }
            throw new ParsingException(uf.source(), "Function [{}] does not support case-(in)sensitivity {}", name(), def);
        }
    }
}

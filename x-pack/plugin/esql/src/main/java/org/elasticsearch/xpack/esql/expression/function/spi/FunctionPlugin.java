/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.spi;

import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

/**
 * Extension point for {@link FunctionDefinition functions}.
 */
public interface FunctionPlugin {
    /**
     * Esql functions.
     */
    FunctionDefinition[] functions();

    /**
     * PromQL functions contributed by this plugin. Defaults to none.
     */
    default PromqlFunctionDefinition[] promqlFunctions() {
        return new PromqlFunctionDefinition[0];
    }
}

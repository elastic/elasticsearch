/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.function.math;

import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.spi.FunctionPlugin;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;

public class MathFunctions implements FunctionPlugin {
    @Override
    public FunctionDefinition[] functions() {
        return new FunctionDefinition[] { Abs.DEFINITION, Atan2.DEFINITION };
    }

    @Override
    public PromqlFunctionDefinition[] promqlFunctions() {
        return new PromqlFunctionDefinition[] { Abs.PROMQL_DEFINITION };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;

import java.util.Locale;

public class EsqlFunctionRegistry extends FunctionRegistry {

    public EsqlFunctionRegistry() {
        register(functions());
    }

    EsqlFunctionRegistry(FunctionDefinition... functions) {
        register(functions);
    }

    private FunctionDefinition[][] functions() {
        return new FunctionDefinition[][] { new FunctionDefinition[] { def(Avg.class, Avg::new, "avg") } };
    }

    @Override
    protected String normalize(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}

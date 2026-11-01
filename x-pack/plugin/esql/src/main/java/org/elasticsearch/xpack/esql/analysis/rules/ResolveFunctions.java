/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules.ParameterizedAnalyzerRule;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Resolves ESQL functions.
 */
public class ResolveFunctions extends ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
        // Allow resolving snapshot-only functions, but do not include them in the documentation
        final EsqlFunctionRegistry snapshotRegistry = context.functionRegistry().snapshotRegistry();
        return plan.transformExpressionsOnly(
            UnresolvedFunction.class,
            uf -> resolveFunction(uf, context.configuration(), snapshotRegistry)
        );
    }

    public static Function resolveFunction(UnresolvedFunction uf, Configuration configuration, EsqlFunctionRegistry functionRegistry) {
        Function f = null;
        if (uf.analyzed()) {
            f = uf;
        } else {
            String functionName = functionRegistry.resolveAlias(uf.name());
            if (functionRegistry.functionExists(functionName) == false) {
                f = uf.missing(functionName, functionRegistry.listFunctions());
            } else {
                FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                f = uf.buildResolved(configuration, def);
            }
        }
        return f;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Listing-time fold of all-literal date function calls. Resolves the name through the
 * session {@link EsqlFunctionRegistry} and dispatches to a static fold on the registered
 * class — never {@link org.elasticsearch.xpack.esql.expression.function.FunctionDefinition#build},
 * which would instantiate the function and run arity validation that throws. Comparison
 * inversion is a follow-up.
 */
public final class DateFunctionComparisonRewriter {

    private static final Logger logger = LogManager.getLogger(DateFunctionComparisonRewriter.class);

    private DateFunctionComparisonRewriter() {}

    /**
     * Fold {@code call} when every argument is a {@link Literal} and the resolved class has a
     * listing fold. Returns a {@link Literal} that keeps {@code call}'s {@link org.elasticsearch.xpack.esql.core.tree.Source},
     * or {@code call} itself when the call cannot be folded.
     */
    public static Expression tryFoldCall(UnresolvedFunction call, Configuration config, EsqlFunctionRegistry functionRegistry) {
        try {
            String canonical = functionRegistry.resolveAlias(call.name());
            if (functionRegistry.functionExists(canonical) == false) {
                return call;
            }
            Class<? extends Function> clazz = functionRegistry.resolveFunction(canonical).clazz();
            Literal folded = null;
            if (clazz == DateExtract.class) {
                folded = DateExtract.tryFoldLiterals(call.source(), call.children(), config);
            } else if (clazz == DateTrunc.class) {
                folded = DateTrunc.tryFoldLiterals(call.source(), call.children(), config);
            }
            return folded != null ? folded : call;
        } catch (UnresolvedException e) {
            // Tripwire for the fail-closed catch below. Current paths only call dataType() after
            // instanceof Literal, so this should not fire; swallowing it would hide a regression
            // that skipped partition hints instead of failing analysis.
            throw e;
        } catch (Exception e) {
            logger.debug("listing-time fold failed for [{}]", call.name(), e);
            return call;
        }
    }
}

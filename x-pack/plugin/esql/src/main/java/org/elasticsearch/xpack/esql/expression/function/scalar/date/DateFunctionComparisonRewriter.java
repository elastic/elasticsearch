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
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;

/**
 * Listing-time fold of all-literal date function calls, and post-analysis inversion of
 * {@code DATE_TRUNC} / monotonic {@code DATE_EXTRACT} comparisons into timestamp ranges.
 * <p>
 * Fold resolves the name through the session {@link EsqlFunctionRegistry} and dispatches to a
 * static method on the registered class — never
 * {@link org.elasticsearch.xpack.esql.expression.function.FunctionDefinition#build}, which would
 * instantiate the function and run arity validation that throws. Invert dispatches on the
 * resolved {@link DateTrunc} / {@link DateExtract} node the same way.
 */
public final class DateFunctionComparisonRewriter {

    private static final Logger logger = LogManager.getLogger(DateFunctionComparisonRewriter.class);

    private DateFunctionComparisonRewriter() {}

    /**
     * Fold {@code call} when every argument is a {@link Literal} and the resolved class has a
     * listing fold. Returns a {@link Literal} that keeps {@code call}'s {@link Source},
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

    /**
     * Invert {@code fn(datetime field) op literal} after analysis. Literal is on the right
     * after {@code LiteralsOnTheRight}. Time zone comes from the resolved function node.
     * Returns {@code null} to leave the comparison alone.
     */
    public static Expression tryRewriteComparison(EsqlBinaryComparison cmp, FoldContext ctx) {
        try {
            return switch (cmp.left()) {
                case DateTrunc trunc -> DateTrunc.tryRewriteComparison(trunc, cmp, ctx);
                case DateExtract extract -> DateExtract.tryRewriteComparison(extract, cmp, ctx);
                default -> null;
            };
        } catch (Exception e) {
            logger.debug("date-function comparison invert failed", e);
            return null;
        }
    }

    /**
     * Map {@code trunc/extract op literal} through the half-open bucket {@code [start, next)}.
     * Non-aligned equality is the empty range {@code field >= start AND field < start}, which
     * is false for every non-null field value and null when the field is null. Non-aligned
     * {@code !=} is left alone. TODO: rewrite that case to {@code field IS NOT NULL} — it is
     * vacuously true for every non-null timestamp and stays null when the field is null.
     */
    static Expression rewriteComparisonBounds(EsqlBinaryComparison cmp, Expression field, Literal start, Literal next, boolean aligned) {
        Source source = cmp.source();
        ZoneId zoneId = cmp.zoneId();
        return switch (cmp) {
            case Equals ignored -> aligned
                ? new And(source, new GreaterThanOrEqual(source, field, start, zoneId), new LessThan(source, field, next, zoneId))
                : new And(source, new GreaterThanOrEqual(source, field, start, zoneId), new LessThan(source, field, start, zoneId));
            case NotEquals ignored -> aligned
                ? new Or(source, new LessThan(source, field, start, zoneId), new GreaterThanOrEqual(source, field, next, zoneId))
                // TODO: non-aligned != → field IS NOT NULL (vacuous, 3VL-safe)
                : null;
            case GreaterThan ignored -> new GreaterThanOrEqual(source, field, next, zoneId);
            case GreaterThanOrEqual ignored -> new GreaterThanOrEqual(source, field, aligned ? start : next, zoneId);
            case LessThan ignored -> new LessThan(source, field, aligned ? start : next, zoneId);
            case LessThanOrEqual ignored -> new LessThan(source, field, next, zoneId);
            default -> throw new IllegalArgumentException("unexpected comparison [" + cmp.getClass().getSimpleName() + "]");
        };
    }

    static Literal boundLiteral(EsqlBinaryComparison cmp, long epoch, DataType fieldType) {
        return new Literal(cmp.source(), epoch, fieldType);
    }
}

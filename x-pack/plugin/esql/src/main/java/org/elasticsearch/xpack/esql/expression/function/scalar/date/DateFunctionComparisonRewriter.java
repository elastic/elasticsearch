/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.time.DateUtils;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDateNanos;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;
import java.time.temporal.TemporalAmount;
import java.util.List;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.DEFAULT_DATE_NANOS_FORMATTER;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateNanosToLong;

/**
 * Listing-time fold of all-literal date function calls, and post-analysis inversion of
 * {@code DATE_TRUNC} / monotonic {@code DATE_EXTRACT} comparisons into timestamp ranges.
 * <p>
 * {@link #foldConstants} is the entry point of the fold, for the listing of external sources and for the time range
 * that index resolution reads from a query.
 * <p>
 * Fold resolves the name through the session {@link EsqlFunctionRegistry} and dispatches on the
 * registered class — never
 * {@link org.elasticsearch.xpack.esql.expression.function.FunctionDefinition#build}, which would
 * instantiate the function and run arity validation that throws. Most arms call a static fold on
 * the function class itself; {@code TO_DATETIME} is folded here instead, because it lives in the
 * {@code convert} package and the literal helpers this fold needs are package-private to this one.
 * It is also the one conversion with two spellings - the named call and the {@code ::datetime} inline
 * cast - so it has two entry points: {@link #tryFoldCall} for the unresolved call and
 * {@link #tryFoldCast} for the node the parser builds.
 * Unary {@code YEAR}/{@code MONTH}/{@code DAY}/{@code HOUR} fold by injecting their chrono and
 * delegating to {@link DateExtract}. Invert dispatches on the resolved {@link DateTrunc} /
 * {@link DateExtract} node after surrogate substitution.
 */
public final class DateFunctionComparisonRewriter {

    private static final Logger logger = LogManager.getLogger(DateFunctionComparisonRewriter.class);

    private DateFunctionComparisonRewriter() {}

    /**
     * Folds the constant dates of an expression of the parsed plan to {@link Literal}s, and returns {@code expression}
     * itself when there is nothing to fold. The expression is walked bottom-up, so that a constant built from others,
     * like {@code DATE_TRUNC(1 day, NOW() - 7 days)}, folds as a whole.
     * <p>
     * This is the one place that evaluates constant dates before the analysis: external sources are listed with it and
     * the time range of a query is read with it, see {@code QueryTimeRangeExtractor}. Both skip data that the query
     * would otherwise read, so a fold must give exactly what the evaluation of the query gives, or decline.
     */
    public static Expression foldConstants(Expression expression, Configuration config, EsqlFunctionRegistry functionRegistry) {
        return expression.transformUp(expr -> {
            if (expr instanceof UnresolvedFunction call) {
                return tryFoldCall(call, config, functionRegistry);
            }
            if (expr instanceof ToDatetime cast) {
                return tryFoldCast(cast, config);
            }
            if (expr instanceof ToDateNanos cast) {
                return tryFoldCast(cast, config);
            }
            if (expr instanceof Add add) {
                return tryFoldArithmetic(add, add.left(), add.right(), true, config);
            }
            if (expr instanceof Sub sub) {
                return tryFoldArithmetic(sub, sub.left(), sub.right(), false, config);
            }
            return expr;
        });
    }

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
            } else if (clazz == ToDatetime.class) {
                folded = tryFoldToDatetime(call.source(), call.children(), config);
            } else if (clazz == ToDateNanos.class) {
                folded = tryFoldToDateNanos(call.source(), call.children(), config);
            } else if (clazz == DateParse.class) {
                folded = DateParse.tryFoldLiterals(call.source(), call.children(), config);
            } else if (clazz == Now.class) {
                // The time that the query runs with, the one that the function itself returns.
                folded = call.children().isEmpty() ? new Literal(call.source(), config.now().toEpochMilli(), DataType.DATETIME) : null;
            } else {
                String chrono = DatePartFunction.chronoFor(clazz);
                if (chrono != null) {
                    folded = DatePartFunction.tryFoldLiterals(chrono, call.source(), call.children(), config);
                }
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
     * The same fold as {@link #tryFoldCall}, for a {@code TO_DATETIME} that arrives already constructed.
     * <p>
     * An inline cast is not a named call: the parser's {@code castToType} builds the conversion function itself, so
     * {@code "2026-07-13T00:00:00.000Z"::datetime} reaches the listing fold as a {@link ToDatetime} node and never as
     * an {@link UnresolvedFunction}. Dispatching on the name alone therefore folded only one of the two spellings of
     * one conversion, and a filter written with the cast emitted no partition hint at all - no error, just the whole
     * glob listed. The node carries a {@code Configuration} of its own, which the parser fills with a marker, so the
     * session's is passed in here exactly as on the named path.
     */
    public static Expression tryFoldCast(ToDatetime cast, Configuration config) {
        try {
            Literal folded = tryFoldToDatetime(cast.source(), cast.children(), config);
            return folded != null ? folded : cast;
        } catch (UnresolvedException e) {
            // Same tripwire as tryFoldCall: swallowing this would hide a regression that skipped partition
            // hints instead of failing analysis.
            throw e;
        } catch (Exception e) {
            logger.debug("listing-time fold failed for an inline datetime cast", e);
            return cast;
        }
    }

    /** The same fold as {@link #tryFoldCast(ToDatetime, Configuration)}, for the {@code ::date_nanos} inline cast. */
    public static Expression tryFoldCast(ToDateNanos cast, Configuration config) {
        try {
            Literal folded = tryFoldToDateNanos(cast.source(), cast.children(), config);
            return folded != null ? folded : cast;
        } catch (UnresolvedException e) {
            throw e;
        } catch (Exception e) {
            logger.debug("listing-time fold failed for an inline date_nanos cast", e);
            return cast;
        }
    }

    /**
     * Fold a time span added to or subtracted from a date literal, the way {@link Add} and {@link Sub} evaluate it.
     */
    private static Expression tryFoldArithmetic(
        Expression operation,
        Expression left,
        Expression right,
        boolean add,
        Configuration config
    ) {
        try {
            Literal date;
            TemporalAmount amount;
            if (temporalAmount(right) instanceof TemporalAmount onRight && left instanceof Literal literal) {
                date = literal;
                amount = onRight;
            } else if (add && temporalAmount(left) instanceof TemporalAmount onLeft && right instanceof Literal literal) {
                date = literal;
                amount = onLeft;
            } else {
                return operation;
            }
            ZoneId zone = DateFunctionLiterals.zoneId(config);
            // A string next to a time span is a date: the analyzer casts it to one, with the same formatter.
            DateFunctionLiterals.ParsedDate parsed = DateFunctionLiterals.parseDateLiteral(date, zone);
            if (parsed == null) {
                return operation;
            }
            long epoch = parsed.epoch();
            if (parsed.nanos()) {
                long result = add ? Add.processDateNanos(epoch, amount, zone) : Sub.processDateNanos(epoch, amount, zone);
                return new Literal(operation.source(), result, DataType.DATE_NANOS);
            }
            long result = add ? Add.processDatetimes(epoch, amount, zone) : Sub.processDatetimes(epoch, amount, zone);
            return new Literal(operation.source(), result, DataType.DATETIME);
        } catch (Exception e) {
            logger.debug("listing-time fold failed for date arithmetic", e);
            return operation;
        }
    }

    private static TemporalAmount temporalAmount(Expression expression) {
        return expression instanceof Literal literal && literal.value() instanceof TemporalAmount amount ? amount : null;
    }

    /**
     * Fold {@code TO_DATETIME(<literal>)} to a {@code DATETIME} literal of epoch millis.
     * <p>
     * Needed because a date function is usually written over a converted string rather than over a bare one -
     * {@code DATE_EXTRACT("YEAR", TO_DATETIME("2026-07-13T00:00:00.000Z"))} is the shape a dashboard sends - and
     * {@link DateFunctionLiterals#literalArgs} refuses any argument that is not already a {@link Literal}, so the
     * outer call cannot fold while the inner one stands. The filter condition is walked bottom-up, so folding this
     * call hands the outer one two literals and its existing fold then applies.
     */
    private static Literal tryFoldToDatetime(Source source, List<Expression> args, Configuration config) {
        Literal[] literals = DateFunctionLiterals.literalArgs(args, 1);
        if (literals == null || literals[0].value() == null) {
            return null;
        }
        Long number = integralNumber(literals[0]);
        if (number != null) {
            // A number counts the milliseconds since the epoch.
            return new Literal(source, number, DataType.DATETIME);
        }
        DateFunctionLiterals.ParsedDate parsed = DateFunctionLiterals.parseDateLiteral(literals[0], DateFunctionLiterals.zoneId(config));
        if (parsed == null) {
            return null;
        }
        long millis = parsed.nanos() ? DateUtils.toMilliSeconds(parsed.epoch()) : parsed.epoch();
        return new Literal(source, millis, DataType.DATETIME);
    }

    /**
     * Fold {@code TO_DATE_NANOS(<literal>)} to a {@code DATE_NANOS} literal of epoch nanos. A string is parsed with the
     * nanosecond formatter, a number counts the nanoseconds since the epoch, and a date is converted. Dates before the
     * epoch have no nanosecond value: the conversion throws and the call is left as it is.
     */
    private static Literal tryFoldToDateNanos(Source source, List<Expression> args, Configuration config) {
        Literal[] literals = DateFunctionLiterals.literalArgs(args, 1);
        if (literals == null || literals[0].value() == null) {
            return null;
        }
        Literal literal = literals[0];
        Long nanos = null;
        Long number = integralNumber(literal);
        if (number != null) {
            nanos = number < 0 ? null : number;
        } else if (literal.dataType() == DataType.DATE_NANOS && literal.value() instanceof Long value) {
            nanos = value;
        } else if (literal.dataType() == DataType.DATETIME && literal.value() instanceof Long value) {
            nanos = DateUtils.toNanoSeconds(value);
        } else if (DataType.isString(literal.dataType())) {
            nanos = dateNanosToLong(
                BytesRefs.toString(literal.value()),
                DEFAULT_DATE_NANOS_FORMATTER.withZone(DateFunctionLiterals.zoneId(config))
            );
        }
        return nanos == null ? null : new Literal(source, nanos, DataType.DATE_NANOS);
    }

    private static Long integralNumber(Literal literal) {
        return (literal.dataType() == DataType.LONG || literal.dataType() == DataType.INTEGER) && literal.value() instanceof Number number
            ? Long.valueOf(number.longValue())
            : null;
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

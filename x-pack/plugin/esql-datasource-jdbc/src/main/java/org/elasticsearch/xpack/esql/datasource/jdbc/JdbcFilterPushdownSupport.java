/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Contains;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Translates ESQL filter expressions into a {@link JdbcPushedQuery} (an opaque tree of {@link SqlPredicate} nodes)
 * for the JDBC connector.
 * <p>
 * <b>Type support.</b> Numeric, boolean, datetime, and keyword columns are supported. Anything else (text, ip,
 * geo, ...) is rejected at the per-conjunct convertibility check so the rule never produces a SQL fragment that
 * the driver would refuse.
 * <p>
 * <b>RECHECK semantics for keyword.</b> Under {@link GenericDialect} we cannot guarantee that the database's
 * collation matches ESQL's byte-exact semantics (case sensitivity, accent folding, NFC/NFD normalization vary
 * across vendors). RECHECK only protects against false positives -- rows the database returns that ESQL would
 * reject -- because we keep the conjunct in {@code FilterExec}. It cannot recover false negatives: rows the
 * database filtered out that ESQL would have kept. So under {@link GenericDialect} we ONLY push KEYWORD
 * predicates whose SQL behavior is a guaranteed superset of the byte-exact semantics:
 * <ul>
 *   <li><b>Allowed</b> (RECHECK): {@code =}, {@code IN (...)}, {@code LIKE / StartsWith / EndsWith / Contains}.
 *       A case-insensitive collation can return EXTRA rows; the engine-side filter rejects them.</li>
 *   <li><b>Allowed</b> (no recheck needed): {@code IS NULL}, {@code IS NOT NULL} -- collation-independent.</li>
 *   <li><b>Refused</b>: {@code <>}, {@code <}, {@code <=}, {@code >}, {@code >=}, {@code BETWEEN}, and any
 *       {@code NOT(inner)} where {@code inner} is itself a RECHECK predicate. These are SUBSETS of the byte-exact
 *       result under at least one collation, and a missed row cannot be recovered from the engine side.</li>
 * </ul>
 * Vendor dialects that expose byte-exact (binary / case-sensitive / NFC-normalized) collation can override the
 * recheck logic and lift these refusals.
 * <p>
 * <b>Conjunct strategy.</b> {@link FilterPushdownSupport#pushFilters(List)} receives AND-separated conjuncts.
 * Each conjunct is processed independently: a translatable conjunct contributes a {@link SqlPredicate} and (for
 * RECHECK conjuncts) also stays in the remainder list. The final {@link SqlPredicate} is the AND of every
 * translated conjunct -- never a single OR, since cross-conjunct OR would over-pushdown.
 * <p>
 * <b>FoldContext.</b> {@link FilterPushdownSupport#pushFilters} does not receive a coordinator-side
 * {@link FoldContext}. We construct {@link FoldContext#small()} per push attempt: literals reach this stage
 * already folded by earlier optimizer passes; the budget is a defense-in-depth measure for the rare expression
 * that arrives foldable-but-not-folded.
 */
public final class JdbcFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(JdbcFilterPushdownSupport.class);

    /** ESQL data types whose values we are willing to bind into JDBC parameters. */
    private static final Set<DataType> SUPPORTED_TYPES = Set.of(
        DataType.BOOLEAN,
        DataType.BYTE,
        DataType.SHORT,
        DataType.INTEGER,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.DATETIME
    );

    private static final Predicate<DataType> TYPE_SUPPORTED = SUPPORTED_TYPES::contains;

    private final JdbcDialect dialect;

    public JdbcFilterPushdownSupport(JdbcDialect dialect) {
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        this.dialect = dialect;
    }

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        if (filters == null || filters.isEmpty()) {
            return PushdownResult.none(List.of());
        }
        FoldContext foldCtx = FoldContext.small();
        List<SqlPredicate> pushed = new ArrayList<>(filters.size());
        List<Expression> pushedExpressions = new ArrayList<>(filters.size());
        List<Expression> remainder = new ArrayList<>(filters.size());
        for (Expression filter : filters) {
            Translation t = translate(filter, foldCtx);
            if (t == null) {
                remainder.add(filter);
                continue;
            }
            pushed.add(t.predicate);
            pushedExpressions.add(filter);
            if (t.recheck) {
                // RECHECK semantics: keep the conjunct in the engine-side filter, but also push for skipping.
                remainder.add(filter);
            }
        }
        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }
        SqlPredicate combined = pushed.size() == 1 ? pushed.get(0) : new SqlPredicate.And(List.copyOf(pushed));
        logger.debug("JDBC pushdown: {} of {} expressions translated", pushed.size(), filters.size());
        return new PushdownResult(new JdbcPushedQuery(combined), List.copyOf(pushedExpressions), List.copyOf(remainder));
    }

    @Override
    public Pushability canPush(Expression expr) {
        Translation t = translate(expr, FoldContext.small());
        if (t == null) {
            return Pushability.NO;
        }
        return t.recheck ? Pushability.RECHECK : Pushability.YES;
    }

    /** Result of translating one ESQL expression. {@code null} means "not pushable". */
    private record Translation(SqlPredicate predicate, boolean recheck) {}

    /**
     * Translates a single ESQL expression into a {@link SqlPredicate}, returning {@code null} when the expression
     * is not safe to push (structural mismatch, unsupported type, foldable-but-throws, ...). Compound expressions
     * (And/Or/Not) recurse; under OR all children must be pushable (a single non-pushable child would discard the
     * disjunction entirely if we dropped it).
     */
    private Translation translate(Expression expr, FoldContext foldCtx) {
        try {
            if (expr instanceof And and) {
                return translateAnd(and, foldCtx);
            }
            if (expr instanceof Or or) {
                return translateOr(or, foldCtx);
            }
            if (expr instanceof Not not) {
                Translation inner = translate(not.field(), foldCtx);
                if (inner == null) {
                    return null;
                }
                // NOT around a RECHECK predicate flips superset semantics into subset semantics: the SQL side may
                // EXCLUDE rows ESQL would keep, and the engine-side filter cannot recover them. Refuse pushdown of
                // NOT(recheck) under GenericDialect; the engine evaluates the conjunct directly.
                if (inner.recheck) {
                    return null;
                }
                return new Translation(new SqlPredicate.Not(inner.predicate), false);
            }
            if (expr instanceof IsNull isNull) {
                return translateIsNull(isNull);
            }
            if (expr instanceof IsNotNull isNotNull) {
                return translateIsNotNull(isNotNull);
            }
            if (expr instanceof EsqlBinaryComparison bc) {
                return translateComparison(bc, foldCtx);
            }
            if (expr instanceof In inExpr) {
                return translateIn(inExpr, foldCtx);
            }
            if (expr instanceof Range range) {
                return translateRange(range, foldCtx);
            }
            if (expr instanceof StartsWith sw) {
                return translateStartsWith(sw, foldCtx);
            }
            if (expr instanceof EndsWith ew) {
                return translateEndsWith(ew, foldCtx);
            }
            if (expr instanceof Contains c) {
                return translateContains(c, foldCtx);
            }
            if (expr instanceof WildcardLike wl) {
                return translateWildcardLike(wl);
            }
        } catch (org.elasticsearch.xpack.esql.core.QlIllegalArgumentException e) {
            // ESQL's own "cannot fold / not applicable here" signal. Expected during translation; debug-only.
            logger.debug(() -> "JDBC pushdown: ESQL refused to translate [" + expr + "]", e);
            return null;
        } catch (RuntimeException e) {
            // Genuinely unexpected -- a NullPointerException from a missing accessor or an arithmetic overflow during
            // fold(). Treat as "not pushable" so the engine still evaluates the conjunct, but surface at WARN so a real
            // bug doesn't get buried in DEBUG noise.
            logger.warn(() -> "JDBC pushdown: unexpected failure translating [" + expr + "], skipping pushdown", e);
            return null;
        }
        return null;
    }

    private Translation translateAnd(And and, FoldContext foldCtx) {
        // AND: all-or-nothing.
        //
        // Why not partial pushdown? translateAnd is never called for top-level conjuncts -- the optimizer's
        // splitAnd() in PushFiltersToSource hands us a pre-split list. translateAnd only fires recursively from
        // translateOr (or from an inner And under an Or). In that nested position, dropping one side of the AND
        // widens the OR's left leg, which produces extra rows the engine cannot reject unless the entire enclosing
        // conjunct is RECHECK -- and we can't guarantee that here. Refuse instead.
        Translation left = translate(and.left(), foldCtx);
        if (left == null) {
            return null;
        }
        Translation right = translate(and.right(), foldCtx);
        if (right == null) {
            return null;
        }
        // Flatten nested ANDs so the rendered SQL is one tidy (a AND b AND c) group.
        List<SqlPredicate> parts = new ArrayList<>(4);
        addAndParts(parts, left.predicate);
        addAndParts(parts, right.predicate);
        return new Translation(new SqlPredicate.And(parts), left.recheck || right.recheck);
    }

    private static void addAndParts(List<SqlPredicate> sink, SqlPredicate p) {
        if (p instanceof SqlPredicate.And and) {
            sink.addAll(and.parts());
        } else {
            sink.add(p);
        }
    }

    private Translation translateOr(Or or, FoldContext foldCtx) {
        // OR: partial pushdown is NOT safe -- dropping one disjunct widens the result set. Both sides must push.
        Translation left = translate(or.left(), foldCtx);
        Translation right = translate(or.right(), foldCtx);
        if (left == null || right == null) {
            return null;
        }
        List<SqlPredicate> parts = new ArrayList<>(4);
        addOrParts(parts, left.predicate);
        addOrParts(parts, right.predicate);
        return new Translation(new SqlPredicate.Or(parts), left.recheck || right.recheck);
    }

    private static void addOrParts(List<SqlPredicate> sink, SqlPredicate p) {
        if (p instanceof SqlPredicate.Or or) {
            sink.addAll(or.parts());
        } else {
            sink.add(p);
        }
    }

    private Translation translateIsNull(IsNull isNull) {
        if (isNull.field() instanceof NamedExpression ne && TYPE_SUPPORTED.test(ne.dataType()) && isVirtualColumn(ne) == false) {
            return new Translation(new SqlPredicate.IsNull(ne.name()), false);
        }
        return null;
    }

    private Translation translateIsNotNull(IsNotNull isNotNull) {
        if (isNotNull.field() instanceof NamedExpression ne && TYPE_SUPPORTED.test(ne.dataType()) && isVirtualColumn(ne) == false) {
            return new Translation(new SqlPredicate.IsNotNull(ne.name()), false);
        }
        return null;
    }

    private Translation translateComparison(EsqlBinaryComparison bc, FoldContext foldCtx) {
        // Both orientations supported: "column op literal" and "literal op column".
        NamedExpression columnSide;
        Expression literalSide;
        boolean commuted;
        if (bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            columnSide = ne;
            literalSide = bc.right();
            commuted = false;
        } else if (bc.right() instanceof NamedExpression ne && bc.left().foldable()) {
            columnSide = ne;
            literalSide = bc.left();
            commuted = true;
        } else {
            return null;
        }
        if (isVirtualColumn(columnSide) || TYPE_SUPPORTED.test(columnSide.dataType()) == false) {
            return null;
        }
        CompOp op = compOpOf(bc);
        if (op == null) {
            return null;
        }
        if (commuted) {
            op = op.commute();
        }
        // Collation safety on KEYWORD: only EQ produces a guaranteed superset under arbitrary collation. Any other
        // comparison (NEQ, ordering) can drop rows ESQL would have kept; RECHECK cannot recover those.
        if (isStringLikeColumn(columnSide.dataType()) && op != CompOp.EQ) {
            return null;
        }
        Object value = foldValue(literalSide, foldCtx);
        if (value == null) {
            // null in a binary comparison always yields null -- never matches. Translating to "x = NULL" would be
            // wrong on every SQL dialect. Refuse and let the engine evaluate it.
            return null;
        }
        Object coerced = coerceLiteral(value, columnSide.dataType());
        if (coerced == null) {
            // Coercion refused (numeric out of range, fractional value into integer column, ...). A predicate that
            // would silently truncate could match rows ESQL never would; refuse rather than emit a wrong predicate.
            return null;
        }
        SqlParam param = new SqlParam(coerced, columnSide.dataType());
        boolean recheck = isStringLikeColumn(columnSide.dataType());
        return new Translation(new SqlPredicate.Comparison(columnSide.name(), op, param), recheck);
    }

    private Translation translateIn(In inExpr, FoldContext foldCtx) {
        if (inExpr.value() instanceof NamedExpression ne) {
            return translateInForColumn(inExpr, ne, foldCtx);
        }
        return null;
    }

    private Translation translateInForColumn(In inExpr, NamedExpression ne, FoldContext foldCtx) {
        if (isVirtualColumn(ne) || TYPE_SUPPORTED.test(ne.dataType()) == false) {
            return null;
        }
        List<SqlParam> values = new ArrayList<>(inExpr.list().size());
        for (Expression item : inExpr.list()) {
            if (item.foldable() == false) {
                return null;
            }
            Object v = foldValue(item, foldCtx);
            if (v == null) {
                // IN with no NULLs only. Mixing NULL into a list silently changes the IN semantics under
                // three-valued logic on most vendors; we'd need a separate OR-IS-NULL leg to be safe. Refuse it
                // here -- the engine evaluates it.
                return null;
            }
            Object coerced = coerceLiteral(v, ne.dataType());
            if (coerced == null) {
                return null;
            }
            values.add(new SqlParam(coerced, ne.dataType()));
        }
        if (values.isEmpty()) {
            return null;
        }
        boolean recheck = isStringLikeColumn(ne.dataType());
        return new Translation(new SqlPredicate.InList(ne.name(), values), recheck);
    }

    private Translation translateRange(Range range, FoldContext foldCtx) {
        if (range.value() instanceof NamedExpression ne) {
            return translateRangeForColumn(range, ne, foldCtx);
        }
        return null;
    }

    private Translation translateRangeForColumn(Range range, NamedExpression ne, FoldContext foldCtx) {
        if (isVirtualColumn(ne)
            || TYPE_SUPPORTED.test(ne.dataType()) == false
            || range.lower().foldable() == false
            || range.upper().foldable() == false) {
            return null;
        }
        // Collation safety: ordering on KEYWORD is collation-defined; SQL BETWEEN/<-pair can exclude rows ESQL would
        // include. RECHECK cannot recover them.
        if (isStringLikeColumn(ne.dataType())) {
            return null;
        }
        Object low = foldValue(range.lower(), foldCtx);
        Object high = foldValue(range.upper(), foldCtx);
        if (low == null || high == null) {
            return null;
        }
        Object coercedLow = coerceLiteral(low, ne.dataType());
        Object coercedHigh = coerceLiteral(high, ne.dataType());
        if (coercedLow == null || coercedHigh == null) {
            return null;
        }
        SqlParam lowerParam = new SqlParam(coercedLow, ne.dataType());
        SqlParam upperParam = new SqlParam(coercedHigh, ne.dataType());
        boolean recheck = isStringLikeColumn(ne.dataType());
        return new Translation(
            new SqlPredicate.Range(ne.name(), lowerParam, range.includeLower(), upperParam, range.includeUpper()),
            recheck
        );
    }

    private Translation translateStartsWith(StartsWith sw, FoldContext foldCtx) {
        return translateBuiltinLike(sw.singleValueField(), sw.prefix(), foldCtx, LikeShape.PREFIX);
    }

    private Translation translateEndsWith(EndsWith ew, FoldContext foldCtx) {
        return translateBuiltinLike(ew.singleValueField(), ew.suffix(), foldCtx, LikeShape.SUFFIX);
    }

    private Translation translateContains(Contains c, FoldContext foldCtx) {
        return translateBuiltinLike(c.singleValueField(), c.substr(), foldCtx, LikeShape.CONTAINS);
    }

    private enum LikeShape {
        PREFIX,   // literal%
        SUFFIX,   // %literal
        CONTAINS  // %literal%
    }

    private Translation translateBuiltinLike(Expression field, Expression literalExpr, FoldContext foldCtx, LikeShape shape) {
        if (field instanceof NamedExpression ne) {
            return translateBuiltinLikeForColumn(ne, literalExpr, foldCtx, shape);
        }
        return null;
    }

    private Translation translateBuiltinLikeForColumn(NamedExpression ne, Expression literalExpr, FoldContext foldCtx, LikeShape shape) {
        if (isVirtualColumn(ne) || ne.dataType() != DataType.KEYWORD || literalExpr.foldable() == false) {
            return null;
        }
        Object value = foldValue(literalExpr, foldCtx);
        if (value == null) {
            return null;
        }
        String escaped = escapeLikeLiteral(bytesRefToString(value));
        String pattern = switch (shape) {
            case PREFIX -> escaped + "%";
            case SUFFIX -> "%" + escaped;
            case CONTAINS -> "%" + escaped + "%";
        };
        // string-like under GenericDialect always RECHECKs (vendor collation might differ from ESQL's byte-exact match).
        return new Translation(new SqlPredicate.Like(ne.name(), pattern), true);
    }

    private Translation translateWildcardLike(WildcardLike wl) {
        if (wl.field() instanceof NamedExpression ne) {
            return translateWildcardLikeForColumn(wl, ne);
        }
        return null;
    }

    private Translation translateWildcardLikeForColumn(WildcardLike wl, NamedExpression ne) {
        if (isVirtualColumn(ne) || ne.dataType() != DataType.KEYWORD) {
            return null;
        }
        if (wl.caseInsensitive()) {
            // Case-insensitive LIKE requires either ILIKE (vendor-specific) or a UPPER(col) wrapper which prevents
            // index use. Refuse it here to keep the spec narrow; the engine evaluates it.
            return null;
        }
        String pattern = esqlWildcardToJdbcLike(wl.pattern().pattern());
        return new Translation(new SqlPredicate.Like(ne.name(), pattern), true);
    }

    private static CompOp compOpOf(EsqlBinaryComparison bc) {
        if (bc instanceof Equals) return CompOp.EQ;
        if (bc instanceof NotEquals) return CompOp.NEQ;
        if (bc instanceof LessThan) return CompOp.LT;
        if (bc instanceof LessThanOrEqual) return CompOp.LTE;
        if (bc instanceof GreaterThan) return CompOp.GT;
        if (bc instanceof GreaterThanOrEqual) return CompOp.GTE;
        return null;
    }

    private static boolean isVirtualColumn(Expression e) {
        return org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates.isVirtualColumn(e);
    }

    private static boolean isStringLikeColumn(DataType type) {
        return type == DataType.KEYWORD;
    }

    private static Object foldValue(Expression e, FoldContext foldCtx) {
        Object folded = e.fold(foldCtx);
        // Unwrap BytesRef -> String at translation time so SqlRenderer doesn't have to know about ESQL's internal
        // string representation. The bound JDBC parameter is a plain String (or null).
        if (folded instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return folded;
    }

    /**
     * Coerce an ESQL-folded literal into the Java type expected by JDBC for the column's declared ESQL type. Most
     * values arrive in the right type already; widening / narrowing is permitted ONLY when the conversion is
     * lossless. Returns {@code null} when the value cannot be safely coerced -- callers treat that as
     * "not pushable" so the engine evaluates the original expression.
     * <p>
     * Why so paranoid: integer columns are non-RECHECK, so the pushed predicate replaces the engine-side filter
     * entirely. A silent {@code longValue()} truncation would turn {@code int_col == 1.9} (which matches no rows
     * under ESQL's typing) into {@code int_col == 1} (which can match real rows). The analyzer typically rejects
     * cross-type comparisons, but this layer must not depend on that silently.
     */
    private static Object coerceLiteral(Object value, DataType columnType) {
        if (value == null) {
            return null;
        }
        return switch (columnType) {
            case BYTE -> narrowToInteger(value, Byte.MIN_VALUE, Byte.MAX_VALUE, l -> (byte) l);
            case SHORT -> narrowToInteger(value, Short.MIN_VALUE, Short.MAX_VALUE, l -> (short) l);
            case INTEGER -> narrowToInteger(value, Integer.MIN_VALUE, Integer.MAX_VALUE, l -> (int) l);
            case LONG -> coerceToLong(value);
            case FLOAT -> coerceToFloat(value);
            case DOUBLE -> coerceToDouble(value);
            // DATETIME literals in ESQL are Long (UTC epoch millis); convert to Instant so JdbcDialect.bindParam can
            // bind it as setTimestamp(Timestamp.from(instant)) -- the driver-portable coercion that default bindParam
            // now applies for DATETIME (a bare setObject(Instant) is handled inconsistently by pgjdbc across versions).
            case DATETIME -> value instanceof Long millis ? java.time.Instant.ofEpochMilli(millis) : null;
            case KEYWORD -> bytesRefToString(value);
            case BOOLEAN -> value instanceof Boolean ? value : null;
            default -> value;
        };
    }

    /**
     * Project a numeric literal onto a narrower integer column. Refuses fractional inputs (a Double / Float /
     * BigDecimal whose value differs from its truncated long form), non-finite inputs, and out-of-range inputs.
     */
    private static Object narrowToInteger(Object value, long min, long max, java.util.function.LongFunction<Object> cast) {
        Long integral = asLosslessLong(value);
        if (integral == null) {
            return null;
        }
        return (integral >= min && integral <= max) ? cast.apply(integral) : null;
    }

    private static Object coerceToLong(Object value) {
        return asLosslessLong(value);
    }

    private static Object coerceToFloat(Object value) {
        // Allow only when the resulting float exactly round-trips back to the source double (no precision loss),
        // and the source is finite.
        if (value instanceof Number n) {
            double d = n.doubleValue();
            if (Double.isFinite(d) == false) {
                return null;
            }
            float f = (float) d;
            if (Float.isFinite(f) == false) {
                return null;
            }
            return ((double) f) == d ? f : null;
        }
        return null;
    }

    private static Object coerceToDouble(Object value) {
        if (value instanceof Number n) {
            double d = n.doubleValue();
            // Float -> Double is always exact; Long -> Double can lose precision past 2^53. Refuse the lossy case.
            if (value instanceof Long l) {
                long round = (long) d;
                if (round != l) {
                    return null;
                }
            }
            return Double.isFinite(d) ? d : null;
        }
        return null;
    }

    /**
     * Returns the value as a {@code long} iff it's an integral number that fits in long without loss. Refuses
     * fractional doubles/floats, non-finite values, and BigInteger/BigDecimal that don't fit.
     * <p>
     * For {@code Double}/{@code Float} we route through {@link java.math.BigDecimal#longValueExact()} because the
     * obvious-looking range/round-trip checks all have a corner case at {@link Long#MAX_VALUE}:
     * {@code (double) Long.MAX_VALUE} rounds up to 2<sup>63</sup> (which IS representable as a double), so naive
     * checks admit values that {@link Double#longValue()} silently saturates back to {@code Long.MAX_VALUE}.
     * {@code BigDecimal.longValueExact()} throws when the value can't be represented as a long with no loss --
     * including the saturation case -- giving us the strictness we want.
     */
    private static Long asLosslessLong(Object value) {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        if (value instanceof Double d) {
            if (Double.isFinite(d) == false) {
                return null;
            }
            try {
                return java.math.BigDecimal.valueOf(d).longValueExact();
            } catch (ArithmeticException ignored) {
                return null;
            }
        }
        if (value instanceof Float f) {
            if (Float.isFinite(f) == false) {
                return null;
            }
            try {
                return java.math.BigDecimal.valueOf(f.doubleValue()).longValueExact();
            } catch (ArithmeticException ignored) {
                return null;
            }
        }
        if (value instanceof java.math.BigInteger bi) {
            try {
                return bi.longValueExact();
            } catch (ArithmeticException ignored) {
                return null;
            }
        }
        if (value instanceof java.math.BigDecimal bd) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException ignored) {
                return null;
            }
        }
        return null;
    }

    private static String bytesRefToString(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return value.toString();
    }

    /**
     * Escapes literal {@code %}, {@code _} and {@code \} so they survive into a JDBC LIKE pattern. The renderer
     * always emits {@code ESCAPE '\\'}, so backslash is the escape character.
     */
    static String escapeLikeLiteral(String literal) {
        if (literal == null || literal.isEmpty()) {
            return literal == null ? "" : literal;
        }
        StringBuilder sb = new StringBuilder(literal.length() + 4);
        for (int i = 0; i < literal.length(); i++) {
            char c = literal.charAt(i);
            if (c == '%' || c == '_' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Translates an ESQL wildcard pattern ({@code *}, {@code ?}, {@code \}-escaped) into the JDBC LIKE form
     * ({@code %}, {@code _}, {@code \}-escaped) without changing the underlying language. The renderer adds
     * {@code ESCAPE '\\'} downstream so the produced pattern is unambiguous on every vendor.
     */
    static String esqlWildcardToJdbcLike(String esqlPattern) {
        if (esqlPattern == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(esqlPattern.length() + 4);
        for (int i = 0; i < esqlPattern.length(); i++) {
            char c = esqlPattern.charAt(i);
            if (c == '\\' && i + 1 < esqlPattern.length()) {
                // Escape sequence in ESQL syntax -- emit the next char as a literal, escaping it again if it is a
                // JDBC LIKE metacharacter.
                char next = esqlPattern.charAt(++i);
                if (next == '%' || next == '_' || next == '\\') {
                    sb.append('\\');
                }
                sb.append(next);
            } else if (c == '*') {
                sb.append('%');
            } else if (c == '?') {
                sb.append('_');
            } else if (c == '%' || c == '_') {
                // Pre-existing JDBC metacharacters in the literal: escape so they match themselves.
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}

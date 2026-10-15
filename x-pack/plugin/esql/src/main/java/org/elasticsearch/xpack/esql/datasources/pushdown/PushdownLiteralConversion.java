/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions.BoundOp;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCompare;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvContains;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvIntersects;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.BinaryComparisonOperation;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts foldable literals from a mixed date or numeric type into the column's
 * {@link DataType} so dataset filter pushdown can prune without becoming stricter than
 * the ES|QL evaluator.
 * <p>
 * This is the literal-type → column-type step. It is not
 * {@link DeclaredTypeCoercions#rawBoundFor}, which maps a decoded column-domain bound onto
 * physical file ticks.
 * <p>
 * Out-of-range numerics become domain tautologies / contradictions that preserve null
 * semantics (e.g. {@code integer <= 3000000000} → {@code integer <= Integer.MAX_VALUE}),
 * never a truncated int. Mixed {@code mv_*} leaves are left unchanged so two-valued
 * null/empty semantics under {@code NOT} stay intact.
 */
public final class PushdownLiteralConversion {

    private static final long NANOS_PER_MILLI = 1_000_000L;

    /**
     * Largest magnitude at which every integer is uniquely representable as a {@code double}.
     * Above this, the ES|QL evaluator promotes {@code long} to {@code double} for mixed compares,
     * so several long values match one double; a point rewrite to a single long would be stricter.
     */
    private static final double MAX_EXACT_LONG_IN_DOUBLE = 0x1p53; // 2^53

    private PushdownLiteralConversion() {}

    /**
     * Rewrites {@code expr} so every convertible mixed date/numeric leaf carries a
     * column-typed literal (and a possibly widened operator). Idempotent. Leaves that
     * cannot be converted safely are unchanged so {@link PushdownPredicates#isAgreeingPushdownLiteral}
     * still declines them.
     */
    public static Expression rewrite(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            return rewriteComparison(bc);
        }
        if (expr instanceof In in) {
            return rewriteIn(in);
        }
        if (expr instanceof Range range) {
            return rewriteRange(range);
        }
        // Mixed mv_* leaves are intentionally not converted here. Rewriting them to scalar
        // comparisons (or domain tautology/contradiction) would change two-valued null/empty
        // semantics — especially under NOT — when Parquet RECHECK puts the rewritten tree in
        // FilterExec. They stay declined via isAgreeingPushdownLiteral until a dedicated mv_
        // conversion lands.
        if (expr instanceof MvContains || expr instanceof MvIntersects || expr instanceof MvInRange || expr instanceof MvCompare) {
            return expr;
        }
        if (expr instanceof And and) {
            Expression left = rewrite(and.left());
            Expression right = rewrite(and.right());
            return left == and.left() && right == and.right() ? and : new And(and.source(), left, right);
        }
        if (expr instanceof Or or) {
            Expression left = rewrite(or.left());
            Expression right = rewrite(or.right());
            return left == or.left() && right == or.right() ? or : new Or(or.source(), left, right);
        }
        if (expr instanceof Not not) {
            Expression field = rewrite(not.field());
            return field == not.field() ? not : new Not(not.source(), field);
        }
        return expr;
    }

    private static Expression rewriteComparison(EsqlBinaryComparison bc) {
        BoundOp op = boundOp(bc);
        if (op == null) {
            return bc;
        }
        if (bc.left() instanceof NamedExpression field && bc.right().foldable()) {
            Converted converted = convert(field.dataType(), bc.right(), op);
            if (converted == null) {
                return bc;
            }
            return buildComparison(bc.source(), field, converted);
        }
        if (bc.right() instanceof NamedExpression field && bc.left().foldable()) {
            // literal OP field → field swap(OP) literal
            BoundOp swapped = swap(op);
            Converted converted = convert(field.dataType(), bc.left(), swapped);
            if (converted == null) {
                return bc;
            }
            return buildComparison(bc.source(), field, converted);
        }
        return bc;
    }

    private static Expression rewriteIn(In in) {
        if (in.value() instanceof NamedExpression field) {
            return rewriteIn(in, field);
        }
        return in;
    }

    private static Expression rewriteIn(In in, NamedExpression field) {
        DataType columnType = field.dataType();
        List<Expression> kept = new ArrayList<>();
        boolean changed = false;
        for (Expression item : in.list()) {
            if (item.foldable() == false) {
                return in;
            }
            if (PushdownPredicates.isAgreeingPushdownLiteral(columnType, item)) {
                kept.add(item);
                continue;
            }
            Converted converted = convert(columnType, item, BoundOp.EQ);
            changed = true;
            if (converted == null) {
                return in;
            }
            if (converted.kind == Kind.CONTRADICTION) {
                continue; // drop unreachable list members
            }
            if (converted.kind == Kind.TAUTOLOGY) {
                // IN with a member that is always true for the domain is itself always true for non-nulls
                return domainTautology(in.source(), field, columnType);
            }
            kept.add(converted.literal);
        }
        if (changed == false) {
            return in;
        }
        if (kept.isEmpty()) {
            return domainContradiction(in.source(), field, columnType);
        }
        if (kept.size() == 1) {
            return new Equals(in.source(), field, kept.getFirst(), null);
        }
        return new In(in.source(), field, kept);
    }

    private static Expression rewriteRange(Range range) {
        if (range.value() instanceof NamedExpression field) {
            return rewriteRange(range, field);
        }
        return range;
    }

    private static Expression rewriteRange(Range range, NamedExpression field) {
        DataType columnType = field.dataType();
        BoundOp lowerOp = range.includeLower() ? BoundOp.GTE : BoundOp.GT;
        BoundOp upperOp = range.includeUpper() ? BoundOp.LTE : BoundOp.LT;
        boolean lowerNeeds = range.lower().foldable() && PushdownPredicates.isAgreeingPushdownLiteral(columnType, range.lower()) == false;
        boolean upperNeeds = range.upper().foldable() && PushdownPredicates.isAgreeingPushdownLiteral(columnType, range.upper()) == false;
        if (lowerNeeds == false && upperNeeds == false) {
            return range;
        }
        // Both bounds must be Literals: convert() only accepts Literals, and the agreeing side
        // is re-wrapped via Converted.converted((Literal) ...). A mixed Literal + agreeing
        // non-Literal (foldable or not) would otherwise ClassCastException on the cast.
        if (range.lower() instanceof Literal == false || range.upper() instanceof Literal == false) {
            return range;
        }
        Converted lower = lowerNeeds ? convert(columnType, range.lower(), lowerOp) : null;
        Converted upper = upperNeeds ? convert(columnType, range.upper(), upperOp) : null;
        if (lowerNeeds && lower == null || upperNeeds && upper == null) {
            return range; // decline: a mixed bound we cannot convert
        }
        if (lower != null && lower.kind == Kind.CONTRADICTION || upper != null && upper.kind == Kind.CONTRADICTION) {
            return domainContradiction(range.source(), field, columnType);
        }
        boolean lowerTaut = lower != null && lower.kind == Kind.TAUTOLOGY;
        boolean upperTaut = upper != null && upper.kind == Kind.TAUTOLOGY;
        if (lowerTaut && upperTaut) {
            return domainTautology(range.source(), field, columnType);
        }
        Expression lo = lowerTaut
            ? null
            : buildComparison(range.source(), field, lower != null ? lower : Converted.converted((Literal) range.lower(), lowerOp));
        Expression hi = upperTaut
            ? null
            : buildComparison(range.source(), field, upper != null ? upper : Converted.converted((Literal) range.upper(), upperOp));
        if (lo == null) {
            return hi;
        }
        if (hi == null) {
            return lo;
        }
        return new And(range.source(), lo, hi);
    }

    /**
     * Converts a {@link Literal} into the column domain for {@code op}.
     * Returns {@code null} when the pair is already agreeing, the expression is not a
     * {@link Literal}, or conversion is unsafe.
     */
    @Nullable
    static Converted convert(DataType columnType, Expression literalExpr, BoundOp op) {
        if (literalExpr instanceof Literal == false) {
            return null;
        }
        DataType literalType = literalExpr.dataType();
        if (columnType == literalType) {
            return null;
        }
        Object value = ((Literal) literalExpr).value();
        // Scalar only: MV array literals hold a List (still typed INTEGER/LONG/DOUBLE).
        if (value instanceof Number == false) {
            return null;
        }
        Source source = literalExpr.source();
        if (columnType.isDate() && literalType.isDate()) {
            return convertTemporal(source, columnType, literalType, value, op);
        }
        if (columnType.isNumeric() && literalType.isNumeric()) {
            return convertNumeric(source, columnType, literalType, value, op);
        }
        return null;
    }

    @Nullable
    private static Converted convertTemporal(Source source, DataType columnType, DataType literalType, Object value, BoundOp op) {
        long raw = ((Number) value).longValue();
        if (columnType == DataType.DATE_NANOS && literalType == DataType.DATETIME) {
            try {
                long nanos = DateUtils.toNanoSeconds(raw);
                return Converted.converted(new Literal(source, nanos, DataType.DATE_NANOS), op);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        if (columnType == DataType.DATETIME && literalType == DataType.DATE_NANOS) {
            return convertNanosLiteralToMillis(source, raw, op);
        }
        return null;
    }

    /**
     * Column is millis; literal is nanos. Evaluator compares via {@link DateUtils#compareNanosToMillis}.
     * Produce a millis bound that is never stricter; widen the operator when a sub-millisecond
     * literal would otherwise exclude a matching millisecond instant.
     */
    @Nullable
    private static Converted convertNanosLiteralToMillis(Source source, long nanos, BoundOp op) {
        if (nanos < 0) {
            return null;
        }
        long floor = Math.floorDiv(nanos, NANOS_PER_MILLI);
        long rem = nanos % NANOS_PER_MILLI;
        boolean exact = rem == 0;
        return switch (op) {
            case EQ -> {
                if (exact) {
                    yield Converted.converted(new Literal(source, floor, DataType.DATETIME), BoundOp.EQ);
                }
                // No millisecond instant equals a sub-ms nanos instant.
                yield Converted.contradiction();
            }
            case NOT_EQ -> {
                if (exact) {
                    yield Converted.converted(new Literal(source, floor, DataType.DATETIME), BoundOp.NOT_EQ);
                }
                // Every millis value differs from a sub-ms instant.
                yield Converted.tautology();
            }
            // m*1e6 < nanos ⟺ m < ceilDiv(nanos, 1e6)
            case LT -> Converted.converted(new Literal(source, Math.ceilDiv(nanos, NANOS_PER_MILLI), DataType.DATETIME), BoundOp.LT);
            // m*1e6 <= nanos ⟺ m <= floorDiv(nanos, 1e6)
            case LTE -> Converted.converted(new Literal(source, floor, DataType.DATETIME), BoundOp.LTE);
            // m*1e6 > nanos ⟺ m > floorDiv(nanos, 1e6)
            case GT -> Converted.converted(new Literal(source, floor, DataType.DATETIME), BoundOp.GT);
            // m*1e6 >= nanos ⟺ m >= ceilDiv(nanos, 1e6)
            case GTE -> Converted.converted(new Literal(source, Math.ceilDiv(nanos, NANOS_PER_MILLI), DataType.DATETIME), BoundOp.GTE);
        };
    }

    @Nullable
    private static Converted convertNumeric(Source source, DataType columnType, DataType literalType, Object value, BoundOp op) {
        return switch (columnType) {
            case LONG -> convertToLong(source, literalType, value, op);
            case INTEGER -> convertToInteger(source, literalType, value, op);
            case DOUBLE -> convertToDouble(source, literalType, value, op);
            default -> null;
        };
    }

    @Nullable
    private static Converted convertToLong(Source source, DataType literalType, Object value, BoundOp op) {
        if (literalType == DataType.INTEGER) {
            long widened = ((Number) value).longValue();
            return Converted.converted(new Literal(source, widened, DataType.LONG), op);
        }
        if (literalType == DataType.DOUBLE) {
            return convertDoubleToIntegral(source, DataType.LONG, ((Number) value).doubleValue(), Long.MIN_VALUE, Long.MAX_VALUE, op);
        }
        return null;
    }

    @Nullable
    private static Converted convertToInteger(Source source, DataType literalType, Object value, BoundOp op) {
        if (literalType == DataType.LONG) {
            long v = ((Number) value).longValue();
            return convertIntegralOutOfRangeOrExact(source, DataType.INTEGER, v, Integer.MIN_VALUE, Integer.MAX_VALUE, op);
        }
        if (literalType == DataType.DOUBLE) {
            return convertDoubleToIntegral(
                source,
                DataType.INTEGER,
                ((Number) value).doubleValue(),
                Integer.MIN_VALUE,
                Integer.MAX_VALUE,
                op
            );
        }
        return null;
    }

    @Nullable
    private static Converted convertToDouble(Source source, DataType literalType, Object value, BoundOp op) {
        if (literalType == DataType.INTEGER || literalType == DataType.LONG) {
            // Exact widen for values in the continuous double range of integers that are exactly representable.
            // Whole integers up to 2^53 are exact; beyond that we still widen with the same bit pattern the
            // evaluator uses via cast (doubleValue), which matches ES|QL's numeric promotion.
            double d = ((Number) value).doubleValue();
            return Converted.converted(new Literal(source, d, DataType.DOUBLE), op);
        }
        return null;
    }

    /**
     * Long/double literal against an integral column: exact convert, op-aware integral bound, or
     * domain tautology/contradiction — never a truncated cast.
     */
    @Nullable
    private static Converted convertIntegralOutOfRangeOrExact(Source source, DataType columnType, long v, long min, long max, BoundOp op) {
        if (v >= min && v <= max) {
            Number boxed = columnType == DataType.INTEGER ? (int) v : v;
            return Converted.converted(new Literal(source, boxed, columnType), op);
        }
        return outOfRangeIntegral(op, v, min, max);
    }

    @Nullable
    private static Converted convertDoubleToIntegral(Source source, DataType columnType, double d, long min, long max, BoundOp op) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return null;
        }
        if (d > max) {
            return outOfRangeIntegral(op, d, min, max);
        }
        if (d < min) {
            return outOfRangeIntegral(op, d, min, max);
        }
        // LONG columns: at |d| >= 2^53 a double is not an injective long preimage
        // ((double)(2^53+1) == (double)2^53). The evaluator compares in double space; a point /
        // floor / ceil rewrite would be stricter or looser. Decline the non-injective range.
        if (columnType == DataType.LONG && Math.abs(d) >= MAX_EXACT_LONG_IN_DOUBLE) {
            return null;
        }
        // Exact whole number in range → keep op.
        if (d == Math.rint(d)) {
            long asLong = (long) d;
            if ((double) asLong == d) {
                Number boxed = columnType == DataType.INTEGER ? (int) asLong : asLong;
                return Converted.converted(new Literal(source, boxed, columnType), op);
            }
        }
        // In-range non-integral: op-aware outward bound (int_col < 5.5 → int_col <= 5).
        // LT/LTE and GT/GTE collapse: for integers, col < 5.5 and col <= 5.5 are both col <= 5.
        return switch (op) {
            case EQ -> Converted.contradiction();
            case NOT_EQ -> Converted.tautology();
            case LT, LTE -> {
                // col < d / col <= d → col <= floor(d)
                long bound = (long) Math.floor(d);
                yield Converted.converted(integralLiteral(source, columnType, bound), BoundOp.LTE);
            }
            case GT, GTE -> {
                // col > d / col >= d → col >= ceil(d)
                long bound = (long) Math.ceil(d);
                yield Converted.converted(integralLiteral(source, columnType, bound), BoundOp.GTE);
            }
        };
    }

    private static Literal integralLiteral(Source source, DataType columnType, long bound) {
        Number boxed = columnType == DataType.INTEGER ? (int) bound : bound;
        return new Literal(source, boxed, columnType);
    }

    /**
     * Literal strictly outside {@code [min, max]}. {@code v} is only used to know which side.
     */
    private static Converted outOfRangeIntegral(BoundOp op, double v, long min, long max) {
        boolean above = v > max;
        return switch (op) {
            case EQ -> Converted.contradiction();
            case NOT_EQ -> Converted.tautology();
            case LT, LTE -> above ? Converted.tautology() : Converted.contradiction();
            case GT, GTE -> above ? Converted.contradiction() : Converted.tautology();
        };
    }

    private static Expression buildComparison(Source source, NamedExpression field, Converted converted) {
        DataType columnType = field.dataType();
        if (converted.kind == Kind.TAUTOLOGY) {
            return domainTautology(source, field, columnType);
        }
        if (converted.kind == Kind.CONTRADICTION) {
            return domainContradiction(source, field, columnType);
        }
        BinaryComparisonOperation bop = switch (converted.op) {
            case EQ -> BinaryComparisonOperation.EQ;
            case NOT_EQ -> BinaryComparisonOperation.NEQ;
            case GT -> BinaryComparisonOperation.GT;
            case GTE -> BinaryComparisonOperation.GTE;
            case LT -> BinaryComparisonOperation.LT;
            case LTE -> BinaryComparisonOperation.LTE;
        };
        return bop.buildNewInstance(source, field, converted.literal);
    }

    /**
     * Always-true for every non-null value of an integral/date column (nulls still fail WHERE).
     * Expressed as {@code col <= MAX} / {@code col >= MIN} so null semantics match a real comparison.
     */
    private static Expression domainTautology(Source source, NamedExpression field, DataType columnType) {
        if (columnType == DataType.INTEGER) {
            return new LessThanOrEqual(source, field, new Literal(source, Integer.MAX_VALUE, DataType.INTEGER), null);
        }
        if (columnType == DataType.LONG) {
            return new LessThanOrEqual(source, field, new Literal(source, Long.MAX_VALUE, DataType.LONG), null);
        }
        if (columnType == DataType.DATETIME || columnType == DataType.DATE_NANOS) {
            // A comparison that every non-null value in the column unit satisfies, preserving
            // WHERE's rejection of nulls (unlike a bare true literal).
            return new LessThanOrEqual(source, field, new Literal(source, Long.MAX_VALUE, columnType), null);
        }
        // DOUBLE is never a tautology/contradiction target: convertToDouble only exact-widens.
        throw new IllegalArgumentException("no domain tautology for [" + columnType + "]");
    }

    /** Always-false for every value (including null → UNKNOWN → dropped by WHERE). */
    private static Expression domainContradiction(Source source, NamedExpression field, DataType columnType) {
        if (columnType == DataType.INTEGER) {
            return new LessThan(source, field, new Literal(source, Integer.MIN_VALUE, DataType.INTEGER), null);
        }
        if (columnType == DataType.LONG) {
            return new LessThan(source, field, new Literal(source, Long.MIN_VALUE, DataType.LONG), null);
        }
        if (columnType == DataType.DATETIME || columnType == DataType.DATE_NANOS) {
            return new LessThan(source, field, new Literal(source, Long.MIN_VALUE, columnType), null);
        }
        // DOUBLE is never a tautology/contradiction target: convertToDouble only exact-widens.
        throw new IllegalArgumentException("no domain contradiction for [" + columnType + "]");
    }

    @Nullable
    private static BoundOp boundOp(EsqlBinaryComparison bc) {
        if (bc instanceof Equals) {
            return BoundOp.EQ;
        }
        if (bc instanceof NotEquals) {
            return BoundOp.NOT_EQ;
        }
        if (bc instanceof GreaterThan) {
            return BoundOp.GT;
        }
        if (bc instanceof GreaterThanOrEqual) {
            return BoundOp.GTE;
        }
        if (bc instanceof LessThan) {
            return BoundOp.LT;
        }
        if (bc instanceof LessThanOrEqual) {
            return BoundOp.LTE;
        }
        return null;
    }

    private static BoundOp swap(BoundOp op) {
        return switch (op) {
            case EQ -> BoundOp.EQ;
            case NOT_EQ -> BoundOp.NOT_EQ;
            case GT -> BoundOp.LT;
            case GTE -> BoundOp.LTE;
            case LT -> BoundOp.GT;
            case LTE -> BoundOp.GTE;
        };
    }

    enum Kind {
        CONVERTED,
        TAUTOLOGY,
        CONTRADICTION
    }

    /**
     * Result of converting one literal. {@link Kind#TAUTOLOGY}/{@link Kind#CONTRADICTION} carry no
     * literal; callers materialize domain predicates via {@link #domainTautology}/{@link #domainContradiction}.
     */
    record Converted(Kind kind, @Nullable Literal literal, @Nullable BoundOp op) {
        static Converted converted(Literal literal, BoundOp op) {
            return new Converted(Kind.CONVERTED, literal, op);
        }

        static Converted tautology() {
            return new Converted(Kind.TAUTOLOGY, null, null);
        }

        static Converted contradiction() {
            return new Converted(Kind.CONTRADICTION, null, null);
        }
    }
}

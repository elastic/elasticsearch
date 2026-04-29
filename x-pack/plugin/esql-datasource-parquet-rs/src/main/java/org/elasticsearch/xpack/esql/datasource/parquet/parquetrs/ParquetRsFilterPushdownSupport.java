/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
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
import java.util.function.Predicate;

/**
 * parquet-rs filter pushdown that translates ESQL filter expressions into
 * native FilterExpr trees via JNI calls.
 * <p>
 * parquet-rs applies RowFilter at the row level during scan, so pushed filters
 * use {@link Pushability#YES}.
 */
public class ParquetRsFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(ParquetRsFilterPushdownSupport.class);

    static final Predicate<DataType> TYPE_SUPPORTED = dt -> dt == DataType.INTEGER
        || dt == DataType.LONG
        || dt == DataType.DOUBLE
        || dt == DataType.KEYWORD
        || dt == DataType.BOOLEAN
        || dt == DataType.DATETIME;

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushed = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>();

        for (Expression filter : filters) {
            if (canConvert(filter)) {
                pushed.add(filter);
            } else {
                remainder.add(filter);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        // Translation to native FilterExpr is deferred to ParquetRsFormatReader.read(): the handle
        // is allocated and freed inside that single call. Doing it here would create a native handle
        // that has no defined owner across the optimizer / per-query / per-file lifecycle and would
        // leak on every query. See ParquetRsPushedFilter for details.
        logger.debug("parquet-rs filter pushdown: accepted {} of {} expressions", pushed.size(), filters.size());
        return new PushdownResult(new ParquetRsPushedFilter(pushed), pushed, remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        if (canConvert(expr)) {
            return Pushability.YES;
        }
        return Pushability.NO;
    }

    static boolean canConvert(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc) {
            if (PushdownPredicates.isComparison(bc, TYPE_SUPPORTED) == false) {
                return false;
            }
            // PushdownPredicates.isComparison accepts any foldable RHS, but translateExpression
            // only handles Literal RHS. ConstantFolding reduces every foldable to a Literal before
            // pushdown, but require Literal explicitly here to keep canConvert and translateExpression
            // in sync, mirroring the same tightening applied to In below.
            if (bc.right() instanceof Literal lit) {
                if (lit.value() == null) {
                    return false;
                }
                if (bc.left() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                    return bc instanceof Equals || bc instanceof NotEquals;
                }
                return true;
            }
            return false;
        }
        if (expr instanceof In inExpr) {
            // PushdownPredicates.isIn accepts any foldable item, but translateIn only handles
            // Literal instances (and would silently drop other foldables). In current ESQL the
            // optimizer's ConstantFolding reduces every foldable to a Literal before pushdown, so
            // this is currently unreachable, but require Literal explicitly to keep canConvert
            // and translateIn in sync and prevent silent data loss if that invariant ever breaks.
            if (PushdownPredicates.isIn(inExpr, TYPE_SUPPORTED) == false) {
                return false;
            }
            for (Expression item : inExpr.list()) {
                if (item instanceof Literal == false) {
                    return false;
                }
            }
            return true;
        }
        if (expr instanceof IsNull isNull) {
            return PushdownPredicates.isIsNull(isNull, TYPE_SUPPORTED);
        }
        if (expr instanceof IsNotNull isNotNull) {
            return PushdownPredicates.isIsNotNull(isNotNull, TYPE_SUPPORTED);
        }
        if (expr instanceof Range range) {
            if (range.value() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return false;
            }
            return PushdownPredicates.isRange(range, TYPE_SUPPORTED);
        }
        if (expr instanceof And and) {
            // Both sides must be convertible: PushFiltersToSource removes the FilterExec when the
            // remainder is empty, so the source becomes solely responsible for the predicate.
            // Allowing a partially-convertible And here would force the And translation branch to
            // either drop the non-convertible side (returning too many rows) or fail at execution.
            // Top-level conjuncts are pre-split by Predicates.splitAnd, so this only restricts ANDs
            // nested under Or/Not, where partial pushdown is unsound.
            return canConvert(and.left()) && canConvert(and.right());
        }
        if (expr instanceof Or or) {
            return canConvert(or.left()) && canConvert(or.right());
        }
        if (expr instanceof Not not) {
            return canConvert(not.field());
        }
        if (expr instanceof StartsWith sw) {
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD)
                && sw.prefix() instanceof Literal lit
                && lit.value() != null;
        }
        if (expr instanceof WildcardLike wl) {
            return wl.field() instanceof NamedExpression ne && (ne.dataType() == DataType.KEYWORD || ne.dataType() == DataType.TEXT);
        }
        return false;
    }

    /**
     * Translates a list of ESQL filter expressions into a single native {@code FilterExpr} handle
     * (logically AND-ed together). Returns {@code 0} when nothing translatable remained.
     * <p>
     * The returned handle is owned by the caller and must be freed via
     * {@link ParquetRsBridge#freeExpr} (or consumed by another {@code create*} bridge call) — it
     * will typically be passed to {@link ParquetRsBridge#openReader} and freed in a {@code finally}
     * block. Intended to be called from {@link ParquetRsFormatReader#read} so the native handle's
     * lifetime is bounded by a single read; see {@link ParquetRsPushedFilter}.
     */
    static long translateExpressions(List<Expression> expressions) {
        ExprHandle combined = null;
        try {
            for (Expression expr : expressions) {
                ExprHandle next = ExprHandle.of(translateExpressionRequired(expr));
                if (combined == null) {
                    combined = next;
                } else {
                    // createAnd consumes both inputs (success or failure), so release
                    // both wrappers before the call. If createAnd throws, the wrappers
                    // are already zeroed, so the catch block will not double-free them.
                    long left = combined.release();
                    long right = next.release();
                    combined = ExprHandle.of(ParquetRsBridge.createAnd(left, right));
                }
            }
            return combined != null ? combined.release() : 0;
        } catch (Throwable t) {
            Releasables.close(combined);
            throw t;
        }
    }

    /**
     * Wraps {@link #translateExpression} with a contract assertion: every expression passed in
     * here must already have been accepted by {@link #canConvert}. A {@code 0} return signals
     * a drift between {@code canConvert} and {@code translateExpression} — pushing on regardless
     * would silently drop a filter the optimizer promised the source would apply, producing
     * wrong query results. Failing loudly surfaces the bug in tests instead.
     */
    private static long translateExpressionRequired(Expression expr) {
        long handle = translateExpression(expr);
        if (handle == 0) {
            throw new IllegalStateException("translateExpression returned 0 for expression accepted by canConvert: [" + expr + "]");
        }
        return handle;
    }

    private static long translateExpression(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right() instanceof Literal lit) {
            Object value = lit.value();
            if (value == null) {
                return 0;
            }
            try (
                ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()));
                ExprHandle litH = ExprHandle.of(createLiteral(ne.dataType(), value))
            ) {
                if (litH.get() == 0) {
                    return 0;
                }
                return createComparison(bc, col.release(), litH.release());
            }
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return translateIn(ne, inExpr.list());
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            try (ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()))) {
                return ParquetRsBridge.createIsNull(col.release());
            }
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            try (ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()))) {
                return ParquetRsBridge.createIsNotNull(col.release());
            }
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return translateRange(ne, range);
        }
        if (expr instanceof And and) {
            // canConvert(And) requires both sides convertible, so both inner translations must succeed.
            try (
                ExprHandle left = ExprHandle.of(translateExpressionRequired(and.left()));
                ExprHandle right = ExprHandle.of(translateExpressionRequired(and.right()))
            ) {
                return ParquetRsBridge.createAnd(left.release(), right.release());
            }
        }
        if (expr instanceof Or or) {
            // canConvert(Or) requires both sides convertible, so both inner translations must succeed.
            try (
                ExprHandle left = ExprHandle.of(translateExpressionRequired(or.left()));
                ExprHandle right = ExprHandle.of(translateExpressionRequired(or.right()))
            ) {
                return ParquetRsBridge.createOr(left.release(), right.release());
            }
        }
        if (expr instanceof Not not) {
            // canConvert(Not) requires the inner field convertible, so translation must succeed.
            try (ExprHandle inner = ExprHandle.of(translateExpressionRequired(not.field()))) {
                return ParquetRsBridge.createNot(inner.release());
            }
        }
        if (expr instanceof WildcardLike wl && wl.field() instanceof NamedExpression ne) {
            try (ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()))) {
                String sqlPattern = esqlWildcardToSqlLike(wl.pattern().pattern());
                return ParquetRsBridge.createLike(col.release(), sqlPattern, wl.caseInsensitive());
            }
        }
        if (expr instanceof StartsWith sw
            && sw.singleValueField() instanceof NamedExpression ne
            && sw.prefix() instanceof Literal prefixLit) {
            if (prefixLit.value() == null) {
                return 0;
            }
            BytesRef prefix = (BytesRef) prefixLit.value();
            try (ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()))) {
                BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(prefix);
                return ParquetRsBridge.createStartsWith(col.release(), prefix.utf8ToString(), upper != null ? upper.utf8ToString() : null);
            }
        }
        return 0;
    }

    private static long translateIn(NamedExpression ne, List<Expression> items) {
        List<ExprHandle> litHandles = new ArrayList<>();
        ExprHandle col = null;
        try {
            for (Expression item : items) {
                if (item instanceof Literal lit && lit.value() != null) {
                    long h = createLiteral(ne.dataType(), lit.value());
                    if (h != 0) {
                        litHandles.add(ExprHandle.of(h));
                    }
                }
            }
            if (litHandles.isEmpty()) {
                return 0;
            }
            col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()));
            long[] raw = ExprHandle.releaseAll(litHandles);
            return ParquetRsBridge.createInList(col.release(), raw);
        } catch (Throwable t) {
            Releasables.close(litHandles);
            Releasables.close(col);
            throw t;
        }
    }

    private static long translateRange(NamedExpression ne, Range range) {
        if (range.lower() instanceof Literal lowerLit
            && range.upper() instanceof Literal upperLit
            && lowerLit.value() != null
            && upperLit.value() != null) {
            return translateRangeBounds(ne, range, lowerLit.value(), upperLit.value());
        }
        return 0;
    }

    private static long translateRangeBounds(NamedExpression ne, Range range, Object lower, Object upper) {
        try (ExprHandle lowerBound = ExprHandle.of(buildBound(ne, lower, range.includeLower(), true))) {
            if (lowerBound.get() == 0) {
                return 0;
            }
            try (ExprHandle upperBound = ExprHandle.of(buildBound(ne, upper, range.includeUpper(), false))) {
                if (upperBound.get() == 0) {
                    return 0;
                }
                return ParquetRsBridge.createAnd(lowerBound.release(), upperBound.release());
            }
        }
    }

    /**
     * Builds one half of a range: {@code col >[=] lit} when {@code isLower}, otherwise {@code col <[=] lit}.
     * Returns 0 if the literal could not be created; never leaks intermediate handles.
     */
    private static long buildBound(NamedExpression ne, Object value, boolean inclusive, boolean isLower) {
        try (
            ExprHandle col = ExprHandle.of(ParquetRsBridge.createColumn(ne.name()));
            ExprHandle lit = ExprHandle.of(createLiteral(ne.dataType(), value))
        ) {
            if (lit.get() == 0) {
                return 0;
            }
            long c = col.release();
            long l = lit.release();
            if (isLower) {
                return inclusive ? ParquetRsBridge.createGreaterThanOrEqual(c, l) : ParquetRsBridge.createGreaterThan(c, l);
            }
            return inclusive ? ParquetRsBridge.createLessThanOrEqual(c, l) : ParquetRsBridge.createLessThan(c, l);
        }
    }

    private static long createComparison(EsqlBinaryComparison bc, long colHandle, long litHandle) {
        return switch (bc) {
            case Equals ignored -> ParquetRsBridge.createEquals(colHandle, litHandle);
            case NotEquals ignored -> ParquetRsBridge.createNotEquals(colHandle, litHandle);
            case GreaterThan ignored -> ParquetRsBridge.createGreaterThan(colHandle, litHandle);
            case GreaterThanOrEqual ignored -> ParquetRsBridge.createGreaterThanOrEqual(colHandle, litHandle);
            case LessThan ignored -> ParquetRsBridge.createLessThan(colHandle, litHandle);
            case LessThanOrEqual ignored -> ParquetRsBridge.createLessThanOrEqual(colHandle, litHandle);
            default -> {
                ParquetRsBridge.freeExpr(colHandle);
                ParquetRsBridge.freeExpr(litHandle);
                yield 0;
            }
        };
    }

    /**
     * Translates an ESQL {@code LIKE} pattern into a SQL {@code LIKE} pattern accepted
     * by parquet-rs, mapping wildcards and re-escaping the SQL special characters that
     * ESQL treats as literals:
     * <ul>
     *   <li>{@code *} -> {@code %} (any sequence)</li>
     *   <li>{@code ?} -> {@code _} (any single char)</li>
     *   <li>{@code %} -> {@code \%}, {@code _} -> {@code \_} (literal in ESQL, wildcard in SQL)</li>
     *   <li>{@code \X} (any escaped char) -> {@code \X} verbatim, so the next char is left as-is</li>
     * </ul>
     *
     * <p>Trailing-backslash edge case: an ESQL pattern ending in a single unmatched {@code \}
     * is emitted as {@code \\} (escape an escape) rather than dropping it. This keeps the SQL
     * pattern syntactically valid — a lone trailing {@code \} would otherwise be an incomplete
     * escape sequence in SQL {@code LIKE} — and treats the input symmetrically with how the
     * loop preserves any other escaped sequence.
     */
    static String esqlWildcardToSqlLike(String esqlPattern) {
        StringBuilder sb = new StringBuilder(esqlPattern.length());
        boolean escaped = false;
        for (int i = 0; i < esqlPattern.length(); i++) {
            char c = esqlPattern.charAt(i);
            if (escaped) {
                sb.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                sb.append(c);
                continue;
            }
            switch (c) {
                case '*' -> sb.append('%');
                case '?' -> sb.append('_');
                case '%' -> sb.append("\\%");
                case '_' -> sb.append("\\_");
                default -> sb.append(c);
            }
        }
        if (escaped) {
            // See class-level note on trailing-backslash handling: emit "\\" so the SQL pattern
            // never ends with an incomplete escape sequence.
            sb.append('\\');
        }
        return sb.toString();
    }

    private static long createLiteral(DataType dataType, Object value) {
        return switch (dataType) {
            case INTEGER -> ParquetRsBridge.createLiteralInt(((Number) value).intValue());
            case LONG -> ParquetRsBridge.createLiteralLong(((Number) value).longValue());
            case DATETIME -> ParquetRsBridge.createLiteralTimestampMillis(((Number) value).longValue());
            case DOUBLE -> ParquetRsBridge.createLiteralDouble(((Number) value).doubleValue());
            case BOOLEAN -> ParquetRsBridge.createLiteralBool((Boolean) value);
            case KEYWORD -> {
                if (value instanceof BytesRef br) {
                    yield ParquetRsBridge.createLiteralString(br.utf8ToString());
                }
                yield ParquetRsBridge.createLiteralString(value.toString());
            }
            default -> 0;
        };
    }
}

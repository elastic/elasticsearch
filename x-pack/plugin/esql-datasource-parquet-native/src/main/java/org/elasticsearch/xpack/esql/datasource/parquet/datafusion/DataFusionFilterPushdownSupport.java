/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.PushdownPredicates;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
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

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * DataFusion-specific filter pushdown that translates ESQL filter expressions into
 * DataFusion {@code Expr} trees via JNI calls to the native library.
 * <p>
 * Unlike the Java-based Parquet reader which only does row-group/page skipping
 * (RECHECK semantics), DataFusion applies filters at the row level during scan,
 * so pushed filters use {@link Pushability#YES} — the engine can safely remove
 * the pushed predicates from FilterExec.
 */
public class DataFusionFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(DataFusionFilterPushdownSupport.class);

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

        long filterHandle = translateExpressions(pushed);
        if (filterHandle == 0) {
            return PushdownResult.none(filters);
        }

        logger.debug("DataFusion filter pushdown: translated {} of {} expressions", pushed.size(), filters.size());
        return new PushdownResult(new DataFusionPushedFilter(filterHandle), pushed, remainder);
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
            if (bc.right().foldable() && literalValueOf(bc.right()) == null) {
                return false;
            }
            if (bc.left() instanceof NamedExpression ne && ne.dataType() == DataType.BOOLEAN) {
                return bc instanceof Equals || bc instanceof NotEquals;
            }
            return true;
        }
        if (expr instanceof In inExpr) {
            return PushdownPredicates.isIn(inExpr, TYPE_SUPPORTED);
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
            return canConvert(and.left()) || canConvert(and.right());
        }
        if (expr instanceof Or or) {
            return canConvert(or.left()) && canConvert(or.right());
        }
        if (expr instanceof Not not) {
            return canConvert(not.field());
        }
        if (expr instanceof StartsWith sw) {
            return PushdownPredicates.isStartsWith(sw, dt -> dt == DataType.KEYWORD) && literalValueOf(sw.prefix()) != null;
        }
        return false;
    }

    /**
     * Translates a list of ESQL expressions into a single DataFusion Expr handle by AND-combining them.
     */
    private static long translateExpressions(List<Expression> expressions) {
        long combined = 0;
        for (Expression expr : expressions) {
            long handle = translateExpression(expr);
            if (handle == 0) {
                continue;
            }
            if (combined == 0) {
                combined = handle;
            } else {
                combined = DataFusionBridge.createAnd(combined, handle);
            }
        }
        return combined;
    }

    private static long translateExpression(Expression expr) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            Object value = literalValueOf(bc.right());
            if (value == null) {
                return 0;
            }
            long colHandle = DataFusionBridge.createColumn(ne.name());
            long litHandle = createLiteral(ne.dataType(), value);
            if (litHandle == 0) {
                DataFusionBridge.freeExpr(colHandle);
                return 0;
            }
            return createComparison(bc, colHandle, litHandle);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return translateIn(ne, inExpr.list());
        }
        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            long colHandle = DataFusionBridge.createColumn(ne.name());
            return DataFusionBridge.createIsNull(colHandle);
        }
        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            long colHandle = DataFusionBridge.createColumn(ne.name());
            return DataFusionBridge.createIsNotNull(colHandle);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return translateRange(ne, range);
        }
        if (expr instanceof And and) {
            return translateAnd(and);
        }
        if (expr instanceof Or or) {
            long leftHandle = translateExpression(or.left());
            long rightHandle = translateExpression(or.right());
            if (leftHandle != 0 && rightHandle != 0) {
                return DataFusionBridge.createOr(leftHandle, rightHandle);
            }
            if (leftHandle != 0) DataFusionBridge.freeExpr(leftHandle);
            if (rightHandle != 0) DataFusionBridge.freeExpr(rightHandle);
            return 0;
        }
        if (expr instanceof Not not) {
            long innerHandle = translateExpression(not.field());
            if (innerHandle != 0) {
                return DataFusionBridge.createNot(innerHandle);
            }
            return 0;
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            Object prefixValue = literalValueOf(sw.prefix());
            if (prefixValue == null) {
                return 0;
            }
            BytesRef prefix = (BytesRef) prefixValue;
            long colHandle = DataFusionBridge.createColumn(ne.name());
            BytesRef upper = StringPrefixUtils.nextPrefixUpperBound(prefix);
            return DataFusionBridge.createStartsWith(colHandle, prefix.utf8ToString(), upper != null ? upper.utf8ToString() : null);
        }
        return 0;
    }

    private static long translateAnd(And and) {
        boolean leftConvertible = canConvert(and.left());
        boolean rightConvertible = canConvert(and.right());
        if (leftConvertible && rightConvertible) {
            long leftHandle = translateExpression(and.left());
            long rightHandle = translateExpression(and.right());
            if (leftHandle != 0 && rightHandle != 0) {
                return DataFusionBridge.createAnd(leftHandle, rightHandle);
            }
            return leftHandle != 0 ? leftHandle : rightHandle;
        } else if (leftConvertible) {
            return translateExpression(and.left());
        } else {
            return translateExpression(and.right());
        }
    }

    private static long translateIn(NamedExpression ne, List<Expression> items) {
        List<Long> litHandles = new ArrayList<>();
        for (Expression item : items) {
            Object val = literalValueOf(item);
            if (val != null) {
                long h = createLiteral(ne.dataType(), val);
                if (h != 0) {
                    litHandles.add(h);
                }
            }
        }
        if (litHandles.isEmpty()) {
            return 0;
        }
        long colHandle = DataFusionBridge.createColumn(ne.name());
        long[] handles = litHandles.stream().mapToLong(Long::longValue).toArray();
        return DataFusionBridge.createInList(colHandle, handles);
    }

    private static long translateRange(NamedExpression ne, Range range) {
        Object lower = literalValueOf(range.lower());
        Object upper = literalValueOf(range.upper());
        if (lower == null || upper == null) {
            return 0;
        }

        long colLower = DataFusionBridge.createColumn(ne.name());
        long litLower = createLiteral(ne.dataType(), lower);
        if (litLower == 0) {
            DataFusionBridge.freeExpr(colLower);
            return 0;
        }
        long lowerBound = range.includeLower()
            ? DataFusionBridge.createGreaterThanOrEqual(colLower, litLower)
            : DataFusionBridge.createGreaterThan(colLower, litLower);

        long colUpper = DataFusionBridge.createColumn(ne.name());
        long litUpper = createLiteral(ne.dataType(), upper);
        if (litUpper == 0) {
            DataFusionBridge.freeExpr(colUpper);
            DataFusionBridge.freeExpr(lowerBound);
            return 0;
        }
        long upperBound = range.includeUpper()
            ? DataFusionBridge.createLessThanOrEqual(colUpper, litUpper)
            : DataFusionBridge.createLessThan(colUpper, litUpper);

        return DataFusionBridge.createAnd(lowerBound, upperBound);
    }

    private static long createComparison(EsqlBinaryComparison bc, long colHandle, long litHandle) {
        return switch (bc) {
            case Equals ignored -> DataFusionBridge.createEquals(colHandle, litHandle);
            case NotEquals ignored -> DataFusionBridge.createNotEquals(colHandle, litHandle);
            case GreaterThan ignored -> DataFusionBridge.createGreaterThan(colHandle, litHandle);
            case GreaterThanOrEqual ignored -> DataFusionBridge.createGreaterThanOrEqual(colHandle, litHandle);
            case LessThan ignored -> DataFusionBridge.createLessThan(colHandle, litHandle);
            case LessThanOrEqual ignored -> DataFusionBridge.createLessThanOrEqual(colHandle, litHandle);
            default -> {
                DataFusionBridge.freeExpr(colHandle);
                DataFusionBridge.freeExpr(litHandle);
                yield 0;
            }
        };
    }

    private static long createLiteral(DataType dataType, Object value) {
        return switch (dataType) {
            case INTEGER -> DataFusionBridge.createLiteralInt(((Number) value).intValue());
            case LONG, DATETIME -> DataFusionBridge.createLiteralLong(((Number) value).longValue());
            case DOUBLE -> DataFusionBridge.createLiteralDouble(((Number) value).doubleValue());
            case BOOLEAN -> DataFusionBridge.createLiteralBool((Boolean) value);
            case KEYWORD -> {
                if (value instanceof BytesRef br) {
                    yield DataFusionBridge.createLiteralString(br.utf8ToString());
                }
                yield DataFusionBridge.createLiteralString(value.toString());
            }
            default -> 0;
        };
    }
}

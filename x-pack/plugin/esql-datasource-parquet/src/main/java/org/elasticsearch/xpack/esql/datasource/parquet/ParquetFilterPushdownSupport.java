/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parquet-specific filter pushdown support that translates ESQL filter expressions
 * to parquet-java {@link FilterPredicate} objects.
 * <p>
 * When set on {@link org.apache.parquet.ParquetReadOptions}, parquet-java automatically
 * applies three levels of row-group filtering:
 * <ol>
 *   <li>Statistics (min/max) — skips row groups where value is outside range</li>
 *   <li>Dictionary — skips row groups where dictionary-encoded column doesn't contain value</li>
 *   <li>Bloom filter — skips row groups where bloom filter says value definitely absent</li>
 * </ol>
 * <p>
 * All pushed filters use {@link Pushability#RECHECK} semantics: the original filter remains
 * in FilterExec for per-row correctness since row-group skipping is an optimization.
 */
public class ParquetFilterPushdownSupport implements FilterPushdownSupport {

    private static final Logger logger = LogManager.getLogger(ParquetFilterPushdownSupport.class);

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<FilterPredicate> pushed = new ArrayList<>();
        // All filters are returned as remainder (RECHECK semantics)
        List<Expression> remainder = new ArrayList<>(filters);

        for (Expression filter : filters) {
            FilterPredicate predicate = translateExpression(filter);
            if (predicate != null) {
                pushed.add(predicate);
            }
        }

        if (pushed.isEmpty()) {
            return PushdownResult.none(filters);
        }

        // Combine all pushed predicates with AND
        FilterPredicate combined = pushed.get(0);
        for (int i = 1; i < pushed.size(); i++) {
            combined = FilterApi.and(combined, pushed.get(i));
        }

        logger.debug("Parquet filter pushdown: translated {} of {} expressions", pushed.size(), filters.size());
        return new PushdownResult(FilterCompat.get(combined), remainder);
    }

    @Override
    public Pushability canPush(Expression expr) {
        if (translateExpression(expr) != null) {
            return Pushability.RECHECK;
        }
        return Pushability.NO;
    }

    /**
     * Translates an ESQL expression to a parquet-java FilterPredicate.
     * Returns null if the expression cannot be translated.
     */
    private FilterPredicate translateExpression(Expression expr) {
        if (expr instanceof Equals eq) {
            return translateEquals(eq);
        } else if (expr instanceof In in) {
            return translateIn(in);
        } else if (expr instanceof GreaterThan gt) {
            return translateComparison(gt.left(), gt.right(), CompOp.GT);
        } else if (expr instanceof GreaterThanOrEqual gte) {
            return translateComparison(gte.left(), gte.right(), CompOp.GTE);
        } else if (expr instanceof LessThan lt) {
            return translateComparison(lt.left(), lt.right(), CompOp.LT);
        } else if (expr instanceof LessThanOrEqual lte) {
            return translateComparison(lte.left(), lte.right(), CompOp.LTE);
        }
        return null;
    }

    private FilterPredicate translateEquals(Equals eq) {
        Expression left = eq.left();
        Expression right = eq.right();

        // Pattern: column = literal
        if (left instanceof Attribute attr && right instanceof Literal lit) {
            return buildEqualityPredicate(attr, lit);
        }
        // Pattern: literal = column (swapped)
        if (right instanceof Attribute attr && left instanceof Literal lit) {
            return buildEqualityPredicate(attr, lit);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private FilterPredicate translateIn(In in) {
        if (in.value() instanceof Attribute == false) {
            return null;
        }
        Attribute attr = (Attribute) in.value();
        DataType dataType = attr.dataType();
        String columnName = attr.name();

        // Collect all literal values
        List<Object> values = new ArrayList<>();
        for (Expression child : in.list()) {
            if (child instanceof Literal lit && lit.value() != null) {
                values.add(lit.value());
            } else {
                // Non-literal in IN list — can't push
                return null;
            }
        }

        if (values.isEmpty()) {
            return null;
        }

        if (dataType == DataType.INTEGER) {
            Operators.IntColumn col = FilterApi.intColumn(columnName);
            Set<Integer> intValues = new HashSet<>();
            for (Object v : values) {
                intValues.add(((Number) v).intValue());
            }
            return FilterApi.in(col, intValues);
        } else if (dataType == DataType.LONG) {
            Operators.LongColumn col = FilterApi.longColumn(columnName);
            Set<Long> longValues = new HashSet<>();
            for (Object v : values) {
                longValues.add(((Number) v).longValue());
            }
            return FilterApi.in(col, longValues);
        } else if (dataType == DataType.DOUBLE) {
            Operators.DoubleColumn col = FilterApi.doubleColumn(columnName);
            Set<Double> doubleValues = new HashSet<>();
            for (Object v : values) {
                doubleValues.add(((Number) v).doubleValue());
            }
            return FilterApi.in(col, doubleValues);
        } else if (dataType == DataType.KEYWORD) {
            Operators.BinaryColumn col = FilterApi.binaryColumn(columnName);
            Set<Binary> binaryValues = new HashSet<>();
            for (Object v : values) {
                binaryValues.add(toBinary(v));
            }
            return FilterApi.in(col, binaryValues);
        }
        return null;
    }

    private enum CompOp {
        GT,
        GTE,
        LT,
        LTE
    }

    private FilterPredicate translateComparison(Expression left, Expression right, CompOp op) {
        // Pattern: column op literal
        if (left instanceof Attribute attr && right instanceof Literal lit) {
            return buildComparisonPredicate(attr, lit, op);
        }
        // Pattern: literal op column (swap the operator direction)
        if (right instanceof Attribute attr && left instanceof Literal lit) {
            CompOp swapped = switch (op) {
                case GT -> CompOp.LT;
                case GTE -> CompOp.LTE;
                case LT -> CompOp.GT;
                case LTE -> CompOp.GTE;
            };
            return buildComparisonPredicate(attr, lit, swapped);
        }
        return null;
    }

    private FilterPredicate buildEqualityPredicate(Attribute attr, Literal lit) {
        if (lit.value() == null) {
            return null; // NULL comparisons can't use bloom filters
        }

        DataType dataType = attr.dataType();
        String columnName = attr.name();

        if (dataType == DataType.INTEGER) {
            return FilterApi.eq(FilterApi.intColumn(columnName), ((Number) lit.value()).intValue());
        } else if (dataType == DataType.LONG) {
            return FilterApi.eq(FilterApi.longColumn(columnName), ((Number) lit.value()).longValue());
        } else if (dataType == DataType.DOUBLE) {
            return FilterApi.eq(FilterApi.doubleColumn(columnName), ((Number) lit.value()).doubleValue());
        } else if (dataType == DataType.KEYWORD) {
            return FilterApi.eq(FilterApi.binaryColumn(columnName), toBinary(lit.value()));
        } else if (dataType == DataType.BOOLEAN) {
            return FilterApi.eq(FilterApi.booleanColumn(columnName), (Boolean) lit.value());
        }
        return null;
    }

    private FilterPredicate buildComparisonPredicate(Attribute attr, Literal lit, CompOp op) {
        if (lit.value() == null) {
            return null;
        }

        DataType dataType = attr.dataType();
        String columnName = attr.name();

        if (dataType == DataType.INTEGER) {
            int value = ((Number) lit.value()).intValue();
            return switch (op) {
                case GT -> FilterApi.gt(FilterApi.intColumn(columnName), value);
                case GTE -> FilterApi.gtEq(FilterApi.intColumn(columnName), value);
                case LT -> FilterApi.lt(FilterApi.intColumn(columnName), value);
                case LTE -> FilterApi.ltEq(FilterApi.intColumn(columnName), value);
            };
        } else if (dataType == DataType.LONG) {
            long value = ((Number) lit.value()).longValue();
            return switch (op) {
                case GT -> FilterApi.gt(FilterApi.longColumn(columnName), value);
                case GTE -> FilterApi.gtEq(FilterApi.longColumn(columnName), value);
                case LT -> FilterApi.lt(FilterApi.longColumn(columnName), value);
                case LTE -> FilterApi.ltEq(FilterApi.longColumn(columnName), value);
            };
        } else if (dataType == DataType.DOUBLE) {
            double value = ((Number) lit.value()).doubleValue();
            return switch (op) {
                case GT -> FilterApi.gt(FilterApi.doubleColumn(columnName), value);
                case GTE -> FilterApi.gtEq(FilterApi.doubleColumn(columnName), value);
                case LT -> FilterApi.lt(FilterApi.doubleColumn(columnName), value);
                case LTE -> FilterApi.ltEq(FilterApi.doubleColumn(columnName), value);
            };
        } else if (dataType == DataType.KEYWORD) {
            Binary value = toBinary(lit.value());
            return switch (op) {
                case GT -> FilterApi.gt(FilterApi.binaryColumn(columnName), value);
                case GTE -> FilterApi.gtEq(FilterApi.binaryColumn(columnName), value);
                case LT -> FilterApi.lt(FilterApi.binaryColumn(columnName), value);
                case LTE -> FilterApi.ltEq(FilterApi.binaryColumn(columnName), value);
            };
        }
        return null;
    }

    /**
     * Converts an ESQL literal value to a Parquet Binary.
     * KEYWORD literals are stored as BytesRef in ESQL.
     */
    private static Binary toBinary(Object value) {
        if (value instanceof BytesRef bytesRef) {
            return Binary.fromConstantByteArray(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }
        return Binary.fromString(value.toString());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.lucene.util.BytesRef;
import org.apache.orc.TypeDescription;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.pushdown.StringPrefixUtils;
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

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.Foldables.literalValueOf;

/**
 * Holds validated ESQL filter expressions whose translation to an ORC {@link SearchArgument}
 * is deferred until read time when the per-file schema ({@link TypeDescription}) is available.
 * <p>
 * This deferred approach enables correct pushdown for:
 * <ul>
 *   <li>DATETIME columns stored as ORC DATE — uses {@link PredicateLeaf.Type#DATE} with
 *       {@link java.sql.Date} literals instead of TIMESTAMP</li>
 *   <li>DOUBLE columns stored as ORC DECIMAL — uses {@link PredicateLeaf.Type#DECIMAL} with
 *       {@link HiveDecimalWritable} literals instead of FLOAT</li>
 * </ul>
 * Without the schema, ESQL's DATETIME type always maps to TIMESTAMP and DOUBLE always maps
 * to FLOAT, which causes incorrect stripe/row-group skipping when the actual column uses a
 * different ORC type.
 */
record OrcPushedExpressions(List<Expression> expressions) {

    /**
     * Builds an ORC {@link SearchArgument} using the actual file schema for correct type mapping.
     *
     * @param schema the ORC file's TypeDescription (from reader.getSchema())
     * @return the SearchArgument, or null if no expressions could be converted
     */
    SearchArgument toSearchArgument(TypeDescription schema) {
        Map<String, TypeDescription.Category> columnTypes = buildColumnTypeMap(schema);

        List<Expression> convertible = new ArrayList<>();
        for (Expression filter : expressions) {
            if (OrcPushdownFilters.canConvert(filter)) {
                convertible.add(filter);
            }
        }
        if (convertible.isEmpty()) {
            return null;
        }

        SearchArgument.Builder builder = SearchArgumentFactory.newBuilder();
        if (convertible.size() == 1) {
            buildPredicate(builder, convertible.get(0), columnTypes);
        } else {
            builder.startAnd();
            for (Expression expr : convertible) {
                buildPredicate(builder, expr, columnTypes);
            }
            builder.end();
        }
        return builder.build();
    }

    private static Map<String, TypeDescription.Category> buildColumnTypeMap(TypeDescription schema) {
        Map<String, TypeDescription.Category> map = new HashMap<>();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++) {
            map.put(fieldNames.get(i), children.get(i).getCategory());
        }
        return map;
    }

    /**
     * Maps ESQL DataType to ORC PredicateLeaf.Type, overriding based on actual column type
     * when the ESQL type is ambiguous (DATETIME can be DATE or TIMESTAMP, DOUBLE can be DECIMAL).
     */
    private static PredicateLeaf.Type resolveType(DataType dataType, String columnName, Map<String, TypeDescription.Category> columnTypes) {
        TypeDescription.Category category = columnTypes.get(columnName);
        if (dataType == DataType.DATETIME && category == TypeDescription.Category.DATE) {
            return PredicateLeaf.Type.DATE;
        }
        if (dataType == DataType.DOUBLE && category == TypeDescription.Category.DECIMAL) {
            return PredicateLeaf.Type.DECIMAL;
        }
        return OrcPushdownFilters.resolveType(dataType);
    }

    /**
     * Converts an ESQL literal value to the Java type expected by ORC's SearchArgument builder,
     * taking the actual column type into account for correct conversion.
     */
    private static Object convertLiteral(
        Object value,
        DataType dataType,
        String columnName,
        Map<String, TypeDescription.Category> columnTypes
    ) {
        if (value == null) {
            return null;
        }
        TypeDescription.Category category = columnTypes.get(columnName);

        if (dataType == DataType.DATETIME && category == TypeDescription.Category.DATE) {
            long millis = ((Number) value).longValue();
            return new Date(millis);
        }
        if (dataType == DataType.DOUBLE && category == TypeDescription.Category.DECIMAL) {
            double d = ((Number) value).doubleValue();
            return new HiveDecimalWritable(HiveDecimal.create(BigDecimal.valueOf(d)));
        }
        return OrcPushdownFilters.convertLiteral(value, dataType);
    }

    private static void buildPredicate(SearchArgument.Builder builder, Expression expr, Map<String, TypeDescription.Category> columnTypes) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne && bc.right().foldable()) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType(), name, columnTypes);
            Object value = convertLiteral(literalValueOf(bc.right()), ne.dataType(), name, columnTypes);

            switch (bc) {
                case Equals ignored -> builder.startAnd().equals(name, type, value).end();
                case NotEquals ignored -> builder.startNot().equals(name, type, value).end();
                case LessThan ignored -> builder.startAnd().lessThan(name, type, value).end();
                case LessThanOrEqual ignored -> builder.startAnd().lessThanEquals(name, type, value).end();
                case GreaterThan ignored -> builder.startNot().lessThanEquals(name, type, value).end();
                case GreaterThanOrEqual ignored -> builder.startNot().lessThan(name, type, value).end();
                default -> throw new QlIllegalArgumentException("Unexpected comparison: " + bc.getClass().getSimpleName());
            }
            return;
        }

        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType(), name, columnTypes);

            List<Object> values = new ArrayList<>();
            for (Expression item : inExpr.list()) {
                Object val = literalValueOf(item);
                if (val != null) {
                    values.add(convertLiteral(val, ne.dataType(), name, columnTypes));
                }
            }
            builder.startAnd().in(name, type, values.toArray(new Object[0])).end();
            return;
        }

        if (expr instanceof IsNull isNull && isNull.field() instanceof NamedExpression ne) {
            PredicateLeaf.Type type = resolveType(ne.dataType(), ne.name(), columnTypes);
            builder.startAnd().isNull(ne.name(), type).end();
            return;
        }

        if (expr instanceof IsNotNull isNotNull && isNotNull.field() instanceof NamedExpression ne) {
            PredicateLeaf.Type type = resolveType(ne.dataType(), ne.name(), columnTypes);
            builder.startNot().isNull(ne.name(), type).end();
            return;
        }

        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType(), name, columnTypes);
            Object lower = convertLiteral(literalValueOf(range.lower()), ne.dataType(), name, columnTypes);
            Object upper = convertLiteral(literalValueOf(range.upper()), ne.dataType(), name, columnTypes);

            if (range.includeLower() && range.includeUpper()) {
                builder.startAnd().between(name, type, lower, upper).end();
            } else {
                builder.startAnd();
                if (range.includeLower()) {
                    builder.startNot().lessThan(name, type, lower).end();
                } else {
                    builder.startNot().lessThanEquals(name, type, lower).end();
                }
                if (range.includeUpper()) {
                    builder.lessThanEquals(name, type, upper);
                } else {
                    builder.lessThan(name, type, upper);
                }
                builder.end();
            }
            return;
        }

        if (expr instanceof And and) {
            boolean leftConvertible = OrcPushdownFilters.canConvert(and.left());
            boolean rightConvertible = OrcPushdownFilters.canConvert(and.right());
            if (leftConvertible && rightConvertible) {
                builder.startAnd();
                buildPredicate(builder, and.left(), columnTypes);
                buildPredicate(builder, and.right(), columnTypes);
                builder.end();
            } else if (leftConvertible) {
                buildPredicate(builder, and.left(), columnTypes);
            } else if (rightConvertible) {
                buildPredicate(builder, and.right(), columnTypes);
            }
            return;
        }

        if (expr instanceof Or or) {
            builder.startOr();
            buildPredicate(builder, or.left(), columnTypes);
            buildPredicate(builder, or.right(), columnTypes);
            builder.end();
            return;
        }

        if (expr instanceof Not not) {
            builder.startNot();
            buildPredicate(builder, not.field(), columnTypes);
            builder.end();
            return;
        }

        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne && sw.prefix().foldable()) {
            Object prefixValue = literalValueOf(sw.prefix());
            if (prefixValue == null) {
                return;
            }
            String name = ne.name();
            PredicateLeaf.Type type = resolveType(ne.dataType(), name, columnTypes);
            String prefix = BytesRefs.toString(prefixValue);
            BytesRef upperBytes = StringPrefixUtils.nextPrefixUpperBound(new BytesRef(prefix));

            if (upperBytes != null) {
                String upper = upperBytes.utf8ToString();
                builder.startAnd();
                builder.startNot().lessThan(name, type, prefix).end();
                builder.lessThan(name, type, upper);
                builder.end();
            } else {
                builder.startNot().lessThan(name, type, prefix).end();
            }
            return;
        }

        throw new QlIllegalArgumentException("Unexpected expression type: " + expr.getClass().getSimpleName());
    }
}

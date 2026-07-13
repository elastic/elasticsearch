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
            if (OrcPushdownFilters.canConvert(filter) && allLeavesPhysicallyCompatible(filter, columnTypes)) {
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

    /**
     * Maximum recursion depth for nested STRUCT flattening; mirrors
     * {@code OrcFormatReader.MAX_STRUCT_FLATTENING_DEPTH}. The two must stay in lock-step:
     * a path the reader-side flattener emits as UNSUPPORTED has no corresponding ESQL
     * attribute, so publishing a column-type entry for it here would yield a stat that no
     * predicate could ever bind to. We re-declare the value rather than depend on the reader
     * class so the pushdown package stays free of reader-internal coupling; the
     * {@code OrcFormatReader} class has a regression test that asserts the two values agree.
     */
    static final int MAX_STRUCT_FLATTENING_DEPTH = 64;

    /**
     * Builds a map from dotted attribute name to the leaf {@link TypeDescription.Category}.
     * Recurses into STRUCT children so {@code event.action} resolves to the leaf's category,
     * not the parent STRUCT. Per the prior PR's D2 precedence (same as Parquet T1), a literal
     * top-level field literally named {@code "event.action"} wins over a nested STRUCT walk
     * to {@code event -> action} — produce a single entry keyed by the literal name and skip
     * the conflicting nested traversal at that path. Stops descending at non-STRUCT types
     * (LIST, MAP, primitives), matching {@code OrcFormatReader.walkDottedLeaves}.
     *
     * <p>The map carries both top-level entries (e.g. {@code "id" -> LONG}) and dotted leaves
     * (e.g. {@code "event.action" -> STRING}); top-level non-STRUCT names retain their old
     * shape for backward compatibility with the existing tests.
     */
    static Map<String, TypeDescription.Category> buildColumnTypeMap(TypeDescription schema) {
        Map<String, TypeDescription.Category> map = new HashMap<>();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++) {
            collectColumnTypes(children.get(i), fieldNames.get(i), 1, map);
        }
        return map;
    }

    private static void collectColumnTypes(TypeDescription type, String dottedPath, int depth, Map<String, TypeDescription.Category> out) {
        // D2 precedence: a literal field name already registered at this path wins over a
        // deeper recursion that would otherwise compose the same dotted string.
        if (out.containsKey(dottedPath)) {
            return;
        }
        if (depth > MAX_STRUCT_FLATTENING_DEPTH) {
            // The flattener emits the over-cap group as a single UNSUPPORTED attribute. There
            // is no corresponding ESQL attribute for any leaf below it, so no predicate can
            // bind to one — leave the entry off the map entirely.
            return;
        }
        if (type.getCategory() == TypeDescription.Category.STRUCT) {
            // STRUCT itself is not addressable as a leaf — record its category at the dotted
            // path so the resolver returns null for any predicate landing on it (mirrors the
            // Parquet T1 "lands on group" rule), then recurse into children.
            out.put(dottedPath, TypeDescription.Category.STRUCT);
            List<String> childNames = type.getFieldNames();
            List<TypeDescription> children = type.getChildren();
            for (int i = 0; i < childNames.size(); i++) {
                collectColumnTypes(children.get(i), dottedPath + "." + childNames.get(i), depth + 1, out);
            }
            return;
        }
        out.put(dottedPath, type.getCategory());
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

    /**
     * True iff every value-comparing leaf in {@code expr} references a column whose physical ORC
     * category is compatible with its declared ESQL type. A declared retype (e.g. {@code keyword}
     * over a physical {@code LONG}) reaches the leaf builders, where a STRING {@link PredicateLeaf}
     * over a LONG column <b>mis-prunes</b>: ORC's stripe evaluator stringifies the integer stats for
     * the STRING comparison, so {@code == "42"} is tested lexicographically against stringified
     * integer bounds — {@code "42" > "100"} lexicographically — and the stripe that actually holds
     * {@code 42} is dropped, silently losing rows.
     *
     * <p>Since ORC pushdown is RECHECK ({@code OrcFilterPushdownSupport} → {@code Pushability.RECHECK}),
     * declining such a conjunct only makes the SearchArgument less selective — never changes the
     * answer — and {@code FilterExec} re-applies the real ESQL semantics. Genuine columns
     * ({@code keyword}=STRING, {@code long}=LONG, {@code datetime}=TIMESTAMP/DATE, …) stay compatible,
     * so no legitimate pushdown is lost. IS NULL / IS NOT NULL are type-independent (they consult the
     * column's null stat, not its value bounds), so they never mis-prune and stay pushable. Mirrors
     * the Parquet {@code physicalPrimitiveIs} guard.
     */
    private static boolean allLeavesPhysicallyCompatible(Expression expr, Map<String, TypeDescription.Category> columnTypes) {
        if (expr instanceof EsqlBinaryComparison bc && bc.left() instanceof NamedExpression ne) {
            return leafPhysicallyCompatible(ne.dataType(), ne.name(), columnTypes);
        }
        if (expr instanceof In inExpr && inExpr.value() instanceof NamedExpression ne) {
            return leafPhysicallyCompatible(ne.dataType(), ne.name(), columnTypes);
        }
        if (expr instanceof Range range && range.value() instanceof NamedExpression ne) {
            return leafPhysicallyCompatible(ne.dataType(), ne.name(), columnTypes);
        }
        if (expr instanceof StartsWith sw && sw.singleValueField() instanceof NamedExpression ne) {
            return leafPhysicallyCompatible(ne.dataType(), ne.name(), columnTypes);
        }
        if (expr instanceof IsNull || expr instanceof IsNotNull) {
            return true; // null-pruning is type-independent — never mis-prunes on a retype
        }
        if (expr instanceof And and) {
            return allLeavesPhysicallyCompatible(and.left(), columnTypes) && allLeavesPhysicallyCompatible(and.right(), columnTypes);
        }
        if (expr instanceof Or or) {
            return allLeavesPhysicallyCompatible(or.left(), columnTypes) && allLeavesPhysicallyCompatible(or.right(), columnTypes);
        }
        if (expr instanceof Not not) {
            return allLeavesPhysicallyCompatible(not.field(), columnTypes);
        }
        return true; // an unknown shape canConvert accepted — canConvert already vetted pushability
    }

    /**
     * Whether the file's physical ORC category at {@code columnName} can back a pruning predicate
     * for the declared ESQL {@code dataType} without the stat-stringification mis-prune described on
     * {@link #allLeavesPhysicallyCompatible}. A column absent from this file's schema (an unconvertible
     * leaf, or glob drift) has no stats, so the mis-prune this guard prevents cannot occur — ORC
     * evaluates a leaf on an unknown column as {@code YES_NO} (no pruning). Treat it as compatible so a
     * partial {@code AND} still pushes its convertible, in-schema leaves (the unconvertible side is
     * dropped by {@code canConvert}/{@code buildPredicate}); declining here would drop the whole
     * conjunction. The mis-prune guard only bites an <b>in-schema</b> column whose physical category
     * disagrees with the declared type.
     */
    private static boolean leafPhysicallyCompatible(
        DataType dataType,
        String columnName,
        Map<String, TypeDescription.Category> columnTypes
    ) {
        TypeDescription.Category cat = columnTypes.get(columnName);
        if (cat == null) {
            return true;
        }
        return switch (dataType) {
            case BOOLEAN -> cat == TypeDescription.Category.BOOLEAN;
            case INTEGER, LONG -> cat == TypeDescription.Category.BYTE
                || cat == TypeDescription.Category.SHORT
                || cat == TypeDescription.Category.INT
                || cat == TypeDescription.Category.LONG;
            case DOUBLE -> cat == TypeDescription.Category.FLOAT
                || cat == TypeDescription.Category.DOUBLE
                || cat == TypeDescription.Category.DECIMAL;
            case KEYWORD, TEXT -> cat == TypeDescription.Category.STRING
                || cat == TypeDescription.Category.CHAR
                || cat == TypeDescription.Category.VARCHAR;
            case DATETIME -> cat == TypeDescription.Category.TIMESTAMP
                || cat == TypeDescription.Category.TIMESTAMP_INSTANT
                || cat == TypeDescription.Category.DATE;
            default -> false;
        };
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

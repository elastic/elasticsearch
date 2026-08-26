/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Format-agnostic utility for adapting pushed filter expressions to a specific file's column set.
 * <p>
 * In UNION_BY_NAME scenarios, different files may have different columns. When a pushed filter
 * references a column absent from a file, the predicate must be simplified before re-translation
 * to the format-specific filter (Parquet {@code FilterPredicate}, ORC {@code SearchArgument}).
 * <p>
 * Adaptation rules for missing columns (SQL three-valued logic):
 * <ul>
 *   <li>Value comparison on missing column → UNKNOWN → FALSE in WHERE → remove conjunct</li>
 *   <li>{@code IS NULL} on missing column → TRUE (all rows match) → remove conjunct (always true)</li>
 *   <li>{@code IS NOT NULL} on missing column → FALSE → remove conjunct (entire AND is false)</li>
 *   <li>{@code AND(A, B)}: if either is null (FALSE) → null; if either is TRUE → other side</li>
 *   <li>{@code OR(A, B)}: if either is TRUE → TRUE; if one is null → other side</li>
 *   <li>{@code NOT(child)}: if child is null → TRUE; if child is TRUE → null</li>
 * </ul>
 * <p>
 * RECHECK semantics guarantee correctness: the adapted filter only affects I/O optimization
 * (row-group/stripe skipping), never row-level correctness.
 */
public final class FilterAdaptation {

    private FilterAdaptation() {}

    /**
     * Adapts pushed filter conjuncts for a specific file's column set.
     * Removes or simplifies predicates on columns absent from the file.
     *
     * @param filterConjuncts AND-separated pushed expressions (same contract as
     *        {@link org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport#pushFilters} input)
     * @param fileColumnNames names of columns present in this file's schema
     * @return adapted conjuncts; empty list means no pushdown for this file
     */
    public static List<Expression> adaptFilterForFile(List<Expression> filterConjuncts, Set<String> fileColumnNames) {
        return adaptFilterForFile(filterConjuncts, fileColumnNames, Map.of());
    }

    /**
     * Adapts pushed filter conjuncts for a specific file's column set and types.
     * Handles both missing columns and type-widened columns.
     * <p>
     * When a column exists in the file with a narrower type than the unified schema (e.g., INTEGER
     * in file vs LONG in unified), filter literals are downcast to the file's native type for
     * optimal row-group/stripe skipping. Literals that overflow the narrower range are statically
     * resolved based on the comparison operator.
     *
     * @param filterConjuncts AND-separated pushed expressions
     * @param fileColumnNames names of columns present in this file's schema
     * @param fileColumnTypes file-local types for columns that differ from the unified schema;
     *        columns not in this map are assumed to match the unified type
     * @return adapted conjuncts; empty list means no pushdown for this file
     */
    public static List<Expression> adaptFilterForFile(
        List<Expression> filterConjuncts,
        Set<String> fileColumnNames,
        Map<String, DataType> fileColumnTypes
    ) {
        List<Expression> adapted = new ArrayList<>(filterConjuncts.size());
        for (Expression conjunct : filterConjuncts) {
            Expression result = adaptExpression(conjunct, fileColumnNames, fileColumnTypes);
            if (result == null) {
                return List.of();
            }
            if (result != TRUE_SENTINEL) {
                adapted.add(result);
            }
        }
        return adapted;
    }

    /**
     * Sentinel value indicating an expression that evaluates to TRUE for all rows.
     * Used internally to distinguish TRUE (remove from AND) from null (FALSE, remove entire AND).
     */
    private static final Expression TRUE_SENTINEL = new Literal(Source.EMPTY, Boolean.TRUE, DataType.BOOLEAN);

    /**
     * Recursively adapts a single expression for the file's column set and types.
     * <p>
     * Uses conservative pruning semantics (not strict SQL three-valued logic):
     * a missing column yields "no constraint" (TRUE_SENTINEL) or "unsatisfiable" (null)
     * depending on the predicate. Under RECHECK, correctness is guaranteed by the
     * retained FilterExec; this only affects I/O-level skipping.
     * <p>
     * Unrecognized expression types (functions, casts, etc.) are returned unchanged.
     * This is safe because {@code pushFilters} will reject or ignore any sub-expression
     * it cannot translate, and RECHECK ensures row-level correctness regardless.
     *
     * @return the adapted expression, {@link #TRUE_SENTINEL} if always true, or {@code null} if always false/unknown
     */
    private static Expression adaptExpression(Expression expr, Set<String> fileColumnNames, Map<String, DataType> fileColumnTypes) {
        if (expr instanceof BinaryComparison bc) {
            return adaptComparison(bc, fileColumnNames, fileColumnTypes);
        }
        if (expr instanceof In in) {
            String colName = extractColumnName(in.value());
            if (colName != null && fileColumnNames.contains(colName) == false) {
                return null; // UNKNOWN → FALSE
            }
            return expr;
        }
        if (expr instanceof IsNull isNull) {
            String colName = extractColumnName(isNull.field());
            if (colName != null && fileColumnNames.contains(colName) == false) {
                return TRUE_SENTINEL; // missing column is always null → IS NULL is TRUE
            }
            return expr;
        }
        if (expr instanceof IsNotNull isNotNull) {
            String colName = extractColumnName(isNotNull.field());
            if (colName != null && fileColumnNames.contains(colName) == false) {
                return null; // missing column is always null → IS NOT NULL is FALSE
            }
            return expr;
        }
        if (expr instanceof And and) {
            Expression left = adaptExpression(and.left(), fileColumnNames, fileColumnTypes);
            Expression right = adaptExpression(and.right(), fileColumnNames, fileColumnTypes);
            if (left == null || right == null) {
                return null;
            }
            if (left == TRUE_SENTINEL) {
                return right;
            }
            if (right == TRUE_SENTINEL) {
                return left;
            }
            if (left == and.left() && right == and.right()) {
                return expr;
            }
            return new And(and.source(), left, right);
        }
        if (expr instanceof Or or) {
            Expression left = adaptExpression(or.left(), fileColumnNames, fileColumnTypes);
            Expression right = adaptExpression(or.right(), fileColumnNames, fileColumnTypes);
            if (left == TRUE_SENTINEL || right == TRUE_SENTINEL) {
                return TRUE_SENTINEL;
            }
            if (left == null && right == null) {
                return null;
            }
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            if (left == or.left() && right == or.right()) {
                return expr;
            }
            return new Or(or.source(), left, right);
        }
        if (expr instanceof Not not) {
            Expression child = adaptExpression(not.field(), fileColumnNames, fileColumnTypes);
            if (child == null) {
                return TRUE_SENTINEL; // NOT(unsatisfiable) → no constraint on this file
            }
            if (child == TRUE_SENTINEL) {
                return null; // NOT(always-true) → unsatisfiable
            }
            if (child == not.field()) {
                return expr;
            }
            return new Not(not.source(), child);
        }
        return expr;
    }

    /**
     * Adapts a binary comparison for missing columns and type narrowing.
     * Returns null if the column is missing, TRUE_SENTINEL/null for overflow resolution,
     * or a rebuilt expression with the narrower type literal if the value fits.
     */
    private static Expression adaptComparison(BinaryComparison bc, Set<String> fileColumnNames, Map<String, DataType> fileColumnTypes) {
        String leftCol = extractColumnName(bc.left());
        String rightCol = extractColumnName(bc.right());
        if (leftCol != null && fileColumnNames.contains(leftCol) == false) {
            return null;
        }
        if (rightCol != null && fileColumnNames.contains(rightCol) == false) {
            return null;
        }
        if (fileColumnTypes.isEmpty()) {
            return bc;
        }
        // Type adaptation: column on left, literal on right (standard form from pushdown)
        if (leftCol != null && bc.right().foldable()) {
            DataType fileType = fileColumnTypes.get(leftCol);
            if (fileType != null) {
                return adaptComparisonType(bc, leftCol, fileType);
            }
        }
        return bc;
    }

    /**
     * Adapts a comparison when the file column has a narrower type than the unified schema.
     * Tries to downcast the literal; resolves statically on overflow/underflow.
     */
    private static Expression adaptComparisonType(BinaryComparison bc, String columnName, DataType fileType) {
        Object literalValue = bc.right() instanceof Literal lit ? lit.value() : null;
        if (literalValue == null) {
            return bc;
        }
        if (fileType == DataType.INTEGER && literalValue instanceof Number num) {
            return adaptLongToInt(bc, columnName, num.longValue());
        }
        // Other narrowing pairs (LONG→INTEGER for DOUBLE unified, DATETIME→DATE_NANOS) are not
        // common in filter pushdown and are left unchanged (safe under RECHECK).
        return bc;
    }

    /**
     * Adapts a LONG comparison to INTEGER when the file column is INT32.
     * If the literal fits in int range, rebuilds with INTEGER types.
     * If it overflows, statically resolves based on the comparison operator.
     */
    private static Expression adaptLongToInt(BinaryComparison bc, String columnName, long value) {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return rebuildComparison(bc, columnName, DataType.INTEGER, (int) value);
        }
        return resolveOverflow(bc, value > Integer.MAX_VALUE);
    }

    /**
     * Rebuilds a comparison with a new column type and downcast literal value.
     */
    private static Expression rebuildComparison(BinaryComparison bc, String columnName, DataType newType, Object newValue) {
        NamedExpression origCol = (NamedExpression) bc.left();
        Expression newCol = new FieldAttribute(
            origCol.source(),
            columnName,
            new EsField(columnName, newType, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Literal newLiteral = new Literal(bc.right().source(), newValue, newType);
        return switch (bc) {
            case Equals ignored -> new Equals(bc.source(), newCol, newLiteral);
            case NotEquals ignored -> new NotEquals(bc.source(), newCol, newLiteral);
            case GreaterThan ignored -> new GreaterThan(bc.source(), newCol, newLiteral, null);
            case GreaterThanOrEqual ignored -> new GreaterThanOrEqual(bc.source(), newCol, newLiteral, null);
            case LessThan ignored -> new LessThan(bc.source(), newCol, newLiteral, null);
            case LessThanOrEqual ignored -> new LessThanOrEqual(bc.source(), newCol, newLiteral, null);
            default -> bc;
        };
    }

    /**
     * Statically resolves a comparison when the literal overflows the file's narrower type.
     *
     * @param overflowHigh true if the literal is above the max value, false if below the min value
     * @return TRUE_SENTINEL if the comparison is always true for all file values, null if always false
     */
    private static Expression resolveOverflow(BinaryComparison bc, boolean overflowHigh) {
        // When literal overflows high (e.g., 3_000_000_000L vs INT32 max 2_147_483_647):
        // col > 3B → always FALSE (no int can be > 3B)
        // col >= 3B → always FALSE
        // col < 3B → always TRUE (all ints are < 3B)
        // col <= 3B → always TRUE
        // col = 3B → always FALSE
        // col != 3B → always TRUE
        // When literal underflows low (e.g., -3_000_000_000L vs INT32 min -2_147_483_648):
        // col > -3B → always TRUE
        // col >= -3B → always TRUE
        // col < -3B → always FALSE
        // col <= -3B → always FALSE
        // col = -3B → always FALSE
        // col != -3B → always TRUE
        return switch (bc) {
            case Equals ignored -> null;
            case NotEquals ignored -> TRUE_SENTINEL;
            case GreaterThan ignored -> overflowHigh ? null : TRUE_SENTINEL;
            case GreaterThanOrEqual ignored -> overflowHigh ? null : TRUE_SENTINEL;
            case LessThan ignored -> overflowHigh ? TRUE_SENTINEL : null;
            case LessThanOrEqual ignored -> overflowHigh ? TRUE_SENTINEL : null;
            default -> bc;
        };
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof NamedExpression ne) {
            return ne.name();
        }
        return null;
    }
}

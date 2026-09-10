/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.AllowedGeneratorFailureException;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.SubqueryGenerator;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.fieldCanBeUsed;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.needsQuoting;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.quote;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomNumericField;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomStringField;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.COMMONLY_SUPPORTED_TYPES;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;

/** Generates random boolean expressions used in filters, EVAL fields, and per-aggregate WHERE clauses. */
public final class BooleanExpressionGenerator {

    /** Supported boolean expression shapes around a generated IN subquery. */
    enum InSubqueryVariant {
        BARE,
        CASE_CONDITION,
        CASE_TRUE_VALUE,
        CASE_ELSE_VALUE,
        COALESCE,
        IS_NULL,
        IS_NOT_NULL,
        EQUALS,
        NOT_EQUALS
    }

    private BooleanExpressionGenerator() {}

    /**
     * Generates an IS NULL / IS NOT NULL expression.
     * May randomly use unmapped field names - especially useful for testing IS NULL on unmapped fields.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String isNullExpression(List<Column> columns, boolean allowUnmapped) {
        if (allowUnmapped && shouldAddUnmappedFieldWithProbabilityIncrease(3)) {
            String unmapped = randomUnmappedFieldName();
            return unmapped + (randomBoolean() ? " IS NULL" : " IS NOT NULL");
        }
        String field = randomName(columns);
        if (field == null) {
            return null;
        }
        return field + (randomBoolean() ? " IS NULL" : " IS NOT NULL");
    }

    /**
     * Generates an IN expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String inExpression(List<Column> columns, boolean allowUnmapped) {
        String numericField = fieldOrUnmapped(randomNumericField(columns), allowUnmapped);
        if (numericField != null && randomBoolean()) {
            int val1 = randomIntBetween(0, 100);
            int val2 = randomIntBetween(0, 100);
            int val3 = randomIntBetween(0, 100);
            return numericField + " IN (" + val1 + ", " + val2 + ", " + val3 + ")";
        }
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField != null) {
            return stringField + " IN (\"a\", \"b\", \"c\")";
        }
        return null;
    }

    /**
     * Generates an {@code IN} subquery expression: {@code outerField IN (subquery | KEEP innerCol)}. Builds a validated inner pipeline
     * via {@link SubqueryGenerator}, then narrows it to a single column whose type matches a field in the outer schema. Returns
     * {@code null} if no type-compatible (outer, inner) column pair exists, or if inner subquery generation fails with an allowed error.
     */
    public static String inSubqueryExpression(
        List<Column> outerColumns,
        CommandGenerator.QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        SubqueryGenerator.SubqueryResult subquery;
        try {
            subquery = SubqueryGenerator.build(context, schema, executor);
        } catch (AllowedGeneratorFailureException e) {
            return null;
        }
        List<Column> innerColumns = subquery.outputSchema();
        if (innerColumns == null || innerColumns.isEmpty()) {
            return null;
        }
        record Pair(String outerField, String innerCol) {}
        List<Pair> candidates = new ArrayList<>();
        for (Column outer : outerColumns) {
            if (fieldCanBeUsed(outer) == false) {
                continue;
            }
            if (COMMONLY_SUPPORTED_TYPES.contains(outer.type()) == false) {
                continue;
            }
            for (Column inner : innerColumns) {
                if (outer.type().equals(inner.type())) {
                    String outerName = needsQuoting(outer.name()) ? quote(outer.name()) : outer.name();
                    String innerName = needsQuoting(inner.name()) ? quote(inner.name()) : inner.name();
                    candidates.add(new Pair(outerName, innerName));
                }
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }
        Pair chosen = randomFrom(candidates);
        // Strip the outer parens from the subquery text to get the raw pipeline text
        String innerQueryText = subquery.queryText().substring(1, subquery.queryText().length() - 1);
        // Narrow to a single compatible column by appending a KEEP
        String narrowedText = innerQueryText + " | KEEP " + chosen.innerCol();
        // Probe the narrowed pipeline before embedding it: SubqueryGenerator probed the inner pipeline
        // command-by-command, but never probed the KEEP itself. Probing here catches "Unknown column"
        // errors that can arise when LIMIT BY or other schema-changing commands produce a schema that
        // does not include the chosen column at analysis time inside an IN subquery.
        QueryExecuted probe = executor.execute(narrowedText, context.subqueryDepth());
        if (probe.exception() != null || probe.outputSchema() == null || probe.outputSchema().isEmpty()) {
            return null;
        }
        return chosen.outerField() + (randomBoolean() ? " IN (" : " NOT IN (") + narrowedText + ")";
    }

    /**
     * Randomly places a generated IN subquery in one of the boolean expression shapes supported by filters, EVAL, and per-aggregate WHERE
     * clauses. Bare predicates remain more common so the generative suite continues to exercise the SemiJoin/AntiJoin path heavily.
     */
    public static String randomlyWrapInSubqueryExpression(String expression) {
        InSubqueryVariant variant = switch (randomIntBetween(0, 11)) {
            case 0, 1, 2, 3 -> InSubqueryVariant.BARE;
            case 4 -> InSubqueryVariant.CASE_CONDITION;
            case 5 -> InSubqueryVariant.CASE_TRUE_VALUE;
            case 6 -> InSubqueryVariant.CASE_ELSE_VALUE;
            case 7 -> InSubqueryVariant.COALESCE;
            case 8 -> InSubqueryVariant.IS_NULL;
            case 9 -> InSubqueryVariant.IS_NOT_NULL;
            case 10 -> InSubqueryVariant.EQUALS;
            default -> InSubqueryVariant.NOT_EQUALS;
        };
        return wrapInSubqueryExpression(expression, variant);
    }

    /**
     * Wraps an IN subquery in a specific supported boolean expression shape. Package-private so
     * fixture tests can verify every shape deterministically while production generation remains random.
     */
    static String wrapInSubqueryExpression(String expression, InSubqueryVariant variant) {
        return switch (variant) {
            case BARE -> expression;
            case CASE_CONDITION -> "case(" + expression + ", true, false)";
            case CASE_TRUE_VALUE -> "case(true, " + expression + ", false)";
            case CASE_ELSE_VALUE -> "case(false, true, " + expression + ")";
            case COALESCE -> "coalesce(" + expression + ", false)";
            case IS_NULL -> "(" + expression + ") IS NULL";
            case IS_NOT_NULL -> "(" + expression + ") IS NOT NULL";
            case EQUALS -> "(" + expression + ") == true";
            case NOT_EQUALS -> "(" + expression + ") != false";
        };
    }

    /**
     * Generates a LIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String likeExpression(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String pattern = randomFrom("*", "a*", "*b", "*test*", "???");
        return stringField + " LIKE \"" + pattern + "\"";
    }

    /**
     * Generates an RLIKE expression.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String rlikeExpression(List<Column> columns, boolean allowUnmapped) {
        String stringField = fieldOrUnmapped(randomStringField(columns), allowUnmapped);
        if (stringField == null) {
            return null;
        }
        String pattern = randomFrom(".*", "a.*", ".*b", ".*test.*", ".{3}");
        return stringField + " RLIKE \"" + pattern + "\"";
    }
}

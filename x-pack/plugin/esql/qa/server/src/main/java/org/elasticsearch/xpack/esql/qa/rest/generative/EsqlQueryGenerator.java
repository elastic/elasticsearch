/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.CommandGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.DissectGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.DropGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.EnrichGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.GrokGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.KeepGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.LimitGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.LookupJoinGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.RenameGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.SortGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.pipe.WhereGenerator;
import org.elasticsearch.xpack.esql.qa.rest.generative.command.source.FromGenerator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

public class EsqlQueryGenerator {

    public record Column(String name, String type) {}

    public record QueryExecuted(String query, int depth, List<Column> outputSchema, List<List<Object>> result, Exception exception) {}

    /**
     * These are commands that are at the beginning of the query, eg. FROM
     */
    static List<CommandGenerator> SOURCE_COMMANDS = List.of(FromGenerator.INSTANCE);

    /**
     * These are downstream commands, ie. that cannot appear as the first command in a query
     */
    static List<CommandGenerator> PIPE_COMMANDS = List.of(
        DissectGenerator.INSTANCE,
        DropGenerator.INSTANCE,
        EnrichGenerator.INSTANCE,
        EvalGenerator.INSTANCE,
        GrokGenerator.INSTANCE,
        KeepGenerator.INSTANCE,
        LimitGenerator.INSTANCE,
        LookupJoinGenerator.INSTANCE,
        MvExpandGenerator.INSTANCE,
        RenameGenerator.INSTANCE,
        SortGenerator.INSTANCE,
        StatsGenerator.INSTANCE,
        WhereGenerator.INSTANCE
    );

    public static CommandGenerator sourceCommand() {
        return randomFrom(SOURCE_COMMANDS);
    }

    public static CommandGenerator randomPipeCommandGenerator() {
        return randomFrom(PIPE_COMMANDS);
    }

    public static String booleanExpression(List<Column> previousOutput) {
        // TODO LIKE, RLIKE, functions etc.
        return switch (randomIntBetween(0, 3)) {
            case 0 -> {
                String field = randomNumericField(previousOutput);
                if (field == null) {
                    yield null;
                }
                yield field + " " + mathCompareOperator() + " 50";
            }
            case 1 -> "true";
            default -> "false";
        };
    }

    public static String mathCompareOperator() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> "==";
            case 1 -> ">";
            case 2 -> ">=";
            case 3 -> "<";
            case 4 -> "<=";
            default -> "!=";
        };
    }

    public static List<CsvTestsDataLoader.EnrichConfig> policiesOnKeyword(List<CsvTestsDataLoader.EnrichConfig> policies) {
        // TODO make it smarter and extend it to other types
        return policies.stream().filter(x -> Set.of("languages_policy").contains(x.policyName())).toList();
    }

    public static String randomName(List<Column> previousOutput) {
        String result = randomRawName(previousOutput);
        if (result == null) {
            return null;
        }
        if (randomBoolean() && result.contains("*") == false) {
            result = "`" + result + "`";
        }
        return result;
    }

    /**
     * Returns a field name from a list of columns.
     * Could be null if none of the fields can be considered
     */
    public static String randomRawName(List<Column> previousOutput) {
        var list = previousOutput.stream().filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (list.isEmpty()) {
            return null;
        }
        String result = randomFrom(list).name();
        return result;
    }

    /**
     * Returns a field that can be used for grouping.
     * Can return null
     */
    public static String randomGroupableName(List<Column> previousOutput) {
        var candidates = previousOutput.stream().filter(EsqlQueryGenerator::groupable).filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (candidates.isEmpty()) {
            return null;
        }
        return randomFrom(candidates).name();
    }

    public static boolean groupable(Column col) {
        return col.type.equals("keyword")
            || col.type.equals("text")
            || col.type.equals("long")
            || col.type.equals("integer")
            || col.type.equals("ip")
            || col.type.equals("version");
    }

    /**
     * returns a field that can be sorted.
     * Null if no fields are sortable.
     */
    public static String randomSortableName(List<Column> previousOutput) {
        var candidates = previousOutput.stream().filter(EsqlQueryGenerator::sortable).filter(EsqlQueryGenerator::fieldCanBeUsed).toList();
        if (candidates.isEmpty()) {
            return null;
        }
        return randomFrom(candidates).name();
    }

    public static boolean sortable(Column col) {
        return col.type.equals("keyword")
            || col.type.equals("text")
            || col.type.equals("long")
            || col.type.equals("integer")
            || col.type.equals("ip")
            || col.type.equals("version");
    }

    public static String agg(List<Column> previousOutput) {
        String name = randomNumericOrDateField(previousOutput);
        if (name != null && randomBoolean()) {
            // numerics only
            return switch (randomIntBetween(0, 1)) {
                case 0 -> "max(" + name + ")";
                default -> "min(" + name + ")";
                // TODO more numerics
            };
        }
        // all types
        name = randomName(previousOutput);
        if (name == null) {
            return "count(*)";
        }
        return switch (randomIntBetween(0, 2)) {
            case 0 -> "count(*)";
            case 1 -> "count(" + name + ")";
            default -> "count_distinct(" + name + ")";
        };
    }

    public static String randomNumericOrDateField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("long", "integer", "double", "date"));
    }

    public static String randomNumericField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("long", "integer", "double"));
    }

    public static String randomStringField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("text", "keyword"));
    }

    public static String randomKeywordField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("keyword"));
    }

    public static String randomName(List<Column> cols, Set<String> allowedTypes) {
        List<String> items = cols.stream().filter(x -> allowedTypes.contains(x.type())).map(Column::name).collect(Collectors.toList());
        if (items.size() == 0) {
            return null;
        }
        return items.get(randomIntBetween(0, items.size() - 1));
    }

    public static String expression(List<Column> previousOutput) {
        // TODO improve!!!
        return constantExpression();
    }

    public static String indexPattern(String indexName) {
        return randomBoolean() ? indexName : indexName.substring(0, randomIntBetween(0, indexName.length())) + "*";
    }

    public static String row() {
        StringBuilder cmd = new StringBuilder("row ");
        int nFields = randomIntBetween(1, 10);
        for (int i = 0; i < nFields; i++) {
            String name = randomIdentifier();
            String expression = constantExpression();
            if (i > 0) {
                cmd.append(",");
            }
            cmd.append(" ");
            cmd.append(name);
            cmd.append(" = ");
            cmd.append(expression);
        }
        return cmd.toString();
    }

    public static String constantExpression() {
        // TODO not only simple values, but also foldable expressions
        return switch (randomIntBetween(0, 4)) {
            case 0 -> "" + randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
            case 1 -> "" + randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            case 2 -> "\"" + randomAlphaOfLength(randomIntBetween(0, 20)) + "\"";
            case 3 -> "" + randomBoolean();
            default -> "null";
        };

    }

    public static String randomIdentifier() {
        // Let's create identifiers that are long enough to avoid collisions with reserved keywords.
        // There could be a smarter way (introspection on the lexer class?), but probably it's not worth the effort
        return randomAlphaOfLength(randomIntBetween(8, 12));
    }

    public static boolean fieldCanBeUsed(Column field) {
        return (
        // https://github.com/elastic/elasticsearch/issues/121741
        field.name().equals("<all-fields-projected>")
            // this is a known pathological case, no need to test it for now
            || field.name().equals("<no-fields>")) == false;
    }

    public static String unquote(String colName) {
        if (colName.startsWith("`") && colName.endsWith("`")) {
            return colName.substring(1, colName.length() - 1);
        }
        return colName;
    }

}

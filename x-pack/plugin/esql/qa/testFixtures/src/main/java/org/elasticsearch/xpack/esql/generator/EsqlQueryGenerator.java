/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.ChangePointGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DissectGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DropGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EnrichGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.ForkGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.GrokGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LimitGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LookupJoinGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.RenameGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.SortGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.WhereGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.SimpleFromGenerator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

public class EsqlQueryGenerator {

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_ORIGINAL_TYPES = "original_types";

    /**
     * These are commands that are at the beginning of the query, eg. FROM
     */
    public static List<CommandGenerator> SOURCE_COMMANDS = List.of(FromGenerator.INSTANCE);

    public static List<CommandGenerator> SIMPLIFIED_SOURCE_COMMANDS = List.of(SimpleFromGenerator.INSTANCE);

    /**
     * These are downstream commands, ie. that cannot appear as the first command in a query
     */
    public static List<CommandGenerator> PIPE_COMMANDS = List.of(
        ChangePointGenerator.INSTANCE,
        DissectGenerator.INSTANCE,
        DropGenerator.INSTANCE,
        EnrichGenerator.INSTANCE,
        EvalGenerator.INSTANCE,
        ForkGenerator.INSTANCE,
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

    /**
     * Same as PIPE_COMMANDS but without the more complex commands (Fork, Enrich, Join).
     * This is needed in CSV tests, that don't support the full ES capabilities
     */
    public static List<CommandGenerator> SIMPLIFIED_PIPE_COMMANDS = List.of(
        ChangePointGenerator.INSTANCE,
        DissectGenerator.INSTANCE,
        DropGenerator.INSTANCE,
        EvalGenerator.INSTANCE,
        GrokGenerator.INSTANCE,
        KeepGenerator.INSTANCE,
        LimitGenerator.INSTANCE,
        MvExpandGenerator.INSTANCE,
        RenameGenerator.INSTANCE,
        SortGenerator.INSTANCE,
        StatsGenerator.INSTANCE,
        WhereGenerator.INSTANCE
    );

    public static CommandGenerator sourceCommand() {
        return randomFrom(SOURCE_COMMANDS);
    }

    public static CommandGenerator simplifiedSourceCommand() {
        return randomFrom(SIMPLIFIED_SOURCE_COMMANDS);
    }

    public interface Executor {
        void run(CommandGenerator generator, CommandGenerator.CommandDescription current);

        List<CommandGenerator.CommandDescription> previousCommands();

        boolean continueExecuting();

        List<Column> currentSchema();

    }

    public static void generatePipeline(
        final int depth,
        CommandGenerator commandGenerator,
        List<CommandGenerator> pipelineGenerators,
        final CommandGenerator.QuerySchema schema,
        Executor executor,
        QueryExecutor queryExecutor
    ) {
        CommandGenerator.CommandDescription desc = commandGenerator.generate(List.of(), List.of(), schema, queryExecutor);
        executor.run(commandGenerator, desc);
        if (executor.continueExecuting() == false) {
            return;
        }

        for (int j = 0; j < depth; j++) {
            if (executor.currentSchema().isEmpty()) {
                break;
            }
            commandGenerator = randomFrom(pipelineGenerators);
            desc = commandGenerator.generate(executor.previousCommands(), executor.currentSchema(), schema, queryExecutor);
            if (desc == CommandGenerator.EMPTY_DESCRIPTION) {
                continue;
            }

            executor.run(commandGenerator, desc);
            if (executor.continueExecuting() == false) {
                break;
            }
        }
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
        return col.type().equals("keyword")
            || col.type().equals("text")
            || col.type().equals("long")
            || col.type().equals("integer")
            || col.type().equals("ip")
            || col.type().equals("version");
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
        return col.type().equals("keyword")
            || col.type().equals("text")
            || col.type().equals("long")
            || col.type().equals("integer")
            || col.type().equals("ip")
            || col.type().equals("version");
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
        name = randomBoolean() ? randomStringField(previousOutput) : randomNumericOrDateField(previousOutput);
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

    public static String randomDateField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("date"));
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

    /**
     * returns a random identifier or one of the existing names
     */
    public static String randomAttributeOrIdentifier(List<Column> previousOutput) {
        String name;
        if (randomBoolean()) {
            name = EsqlQueryGenerator.randomIdentifier();
        } else {
            name = EsqlQueryGenerator.randomName(previousOutput);
            if (name == null) {
                name = EsqlQueryGenerator.randomIdentifier();
            }
        }
        return name;
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
            || field.name().equals("<no-fields>")
            // no dense vectors for now, they are not supported in most commands
            || field.type().contains("vector")
            || field.originalTypes().stream().anyMatch(x -> x.contains("vector"))) == false;
    }

    public static String unquote(String colName) {
        if (colName.startsWith("`") && colName.endsWith("`")) {
            return colName.substring(1, colName.length() - 1);
        }
        return colName;
    }

}

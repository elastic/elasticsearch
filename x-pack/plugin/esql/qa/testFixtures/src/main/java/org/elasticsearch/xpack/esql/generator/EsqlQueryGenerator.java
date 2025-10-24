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
import org.elasticsearch.xpack.esql.generator.command.pipe.DropAllGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DropGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EnrichGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.ForkGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.GrokGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.InlineStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LimitGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LookupJoinGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.RenameGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.SortGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.TimeSeriesStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.WhereGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.TimeSeriesGenerator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    static List<CommandGenerator> SOURCE_COMMANDS = List.of(FromGenerator.INSTANCE);

    /**
     * Commands at the beginning of queries that begin queries on time series indices, eg. TS
     */
    static List<CommandGenerator> TIME_SERIES_SOURCE_COMMANDS = List.of(TimeSeriesGenerator.INSTANCE);

    /**
     * These are downstream commands, ie. that cannot appear as the first command in a query
     */
    static List<CommandGenerator> PIPE_COMMANDS = List.of(
        ChangePointGenerator.INSTANCE,
        DissectGenerator.INSTANCE,
        DropGenerator.INSTANCE,
        DropAllGenerator.INSTANCE,
        EnrichGenerator.INSTANCE,
        EvalGenerator.INSTANCE,
        ForkGenerator.INSTANCE,
        GrokGenerator.INSTANCE,
        KeepGenerator.INSTANCE,
        InlineStatsGenerator.INSTANCE,
        LimitGenerator.INSTANCE,
        LookupJoinGenerator.INSTANCE,
        MvExpandGenerator.INSTANCE,
        RenameGenerator.INSTANCE,
        // awaits fix for https://github.com/elastic/elasticsearch/issues/135336
        // SampleGenerator.INSTANCE,
        SortGenerator.INSTANCE,
        StatsGenerator.INSTANCE,
        WhereGenerator.INSTANCE
    );

    static List<CommandGenerator> TIME_SERIES_PIPE_COMMANDS = Stream.concat(
        PIPE_COMMANDS.stream(),
        Stream.of(TimeSeriesStatsGenerator.INSTANCE)
    ).toList();

    public static CommandGenerator sourceCommand() {
        return randomFrom(SOURCE_COMMANDS);
    }

    public static CommandGenerator timeSeriesSourceCommand() {
        return randomFrom(TIME_SERIES_SOURCE_COMMANDS);
    }

    public static CommandGenerator randomPipeCommandGenerator() {
        return randomFrom(PIPE_COMMANDS);
    }

    public static CommandGenerator randomMetricsPipeCommandGenerator() {
        return randomFrom(TIME_SERIES_PIPE_COMMANDS);
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
        final CommandGenerator.QuerySchema schema,
        Executor executor,
        boolean isTimeSeries,
        QueryExecutor queryExecutor
    ) {
        boolean canGenerateTimeSeries = isTimeSeries;
        CommandGenerator.CommandDescription desc = commandGenerator.generate(List.of(), List.of(), schema, queryExecutor);
        executor.run(commandGenerator, desc);
        if (executor.continueExecuting() == false) {
            return;
        }

        for (int j = 0; j < depth; j++) {
            if (executor.currentSchema().isEmpty()) {
                break;
            }
            commandGenerator = isTimeSeries && canGenerateTimeSeries ? randomMetricsPipeCommandGenerator() : randomPipeCommandGenerator();
            if (isTimeSeries) {
                if (commandGenerator.equals(ForkGenerator.INSTANCE)) {
                    // don't fork with TS command until this is resolved: https://github.com/elastic/elasticsearch/issues/136927
                    continue;
                }
                if (commandGenerator.equals(TimeSeriesStatsGenerator.INSTANCE) || commandGenerator.equals(StatsGenerator.INSTANCE)) {
                    if (canGenerateTimeSeries == false) {
                        // Don't generate multiple stats commands in a single query for TS
                        continue;
                    }
                    canGenerateTimeSeries = false;
                } else if (commandGenerator.equals(RenameGenerator.INSTANCE)) {
                    // don't allow stats after a rename until this is resolved: https://github.com/elastic/elasticsearch/issues/134994
                    canGenerateTimeSeries = false;
                }
            }

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

    public static String metricsAgg(List<Column> previousOutput) {
        String outerCommand = randomFrom("min", "max", "sum", "count", "avg");
        String innerCommand = switch (randomIntBetween(0, 3)) {
            case 0 -> {
                // input can be numerics + aggregate_metric_double
                String numericPlusAggMetricFieldName = randomMetricsNumericField(previousOutput);
                if (numericPlusAggMetricFieldName == null) {
                    yield null;
                }
                yield switch ((randomIntBetween(0, 6))) {
                    case 0 -> "max_over_time(" + numericPlusAggMetricFieldName + ")";
                    case 1 -> "min_over_time(" + numericPlusAggMetricFieldName + ")";
                    case 2 -> "sum_over_time(" + numericPlusAggMetricFieldName + ")";
                    case 3 -> {
                        if (outerCommand.equals("sum") || outerCommand.equals("avg")) {
                            yield null;
                        }
                        yield "present_over_time(" + numericPlusAggMetricFieldName + ")";
                    }
                    case 4 -> {
                        if (outerCommand.equals("sum") || outerCommand.equals("avg")) {
                            yield null;
                        }
                        yield "absent_over_time(" + numericPlusAggMetricFieldName + ")";
                    }
                    case 5 -> "count_over_time(" + numericPlusAggMetricFieldName + ")";
                    default -> "avg_over_time(" + numericPlusAggMetricFieldName + ")";
                };
            }
            case 1 -> {
                // input can be a counter
                String counterField = randomCounterField(previousOutput);
                if (counterField == null) {
                    yield null;
                }
                yield switch ((randomIntBetween(0, 2))) {
                    case 0 -> "rate(" + counterField + ")";
                    case 1 -> "first_over_time(" + counterField + ")";
                    default -> "last_over_time(" + counterField + ")";
                };
            }
            case 2 -> {
                // numerics except aggregate_metric_double
                // TODO: add to case 0 when support for aggregate_metric_double is added to these functions
                String numericFieldName = randomNumericField(previousOutput);
                if (numericFieldName == null) {
                    yield null;
                }
                if (previousOutput.stream()
                    .noneMatch(
                        column -> column.name().equals("@timestamp")
                            && (column.type().equals("date_nanos") || column.type().equals("datetime"))
                    )) {
                    // first_over_time and last_over_time require @timestamp to be available and be either datetime or date_nanos
                    yield null;
                }
                yield (randomBoolean() ? "first_over_time(" : "last_over_time(") + numericFieldName + ")";
            }
            default -> {
                // TODO: add other types that count_over_time supports
                String otherFieldName = randomBoolean() ? randomStringField(previousOutput) : randomNumericOrDateField(previousOutput);
                if (otherFieldName == null) {
                    yield null;
                }
                if (randomBoolean()) {
                    yield "count_over_time(" + otherFieldName + ")";
                } else {
                    yield "count_distinct_over_time(" + otherFieldName + ")";
                    // TODO: replace with the below
                    // yield "count_distinct_over_time(" + otherFieldName + (randomBoolean() ? ", " + randomNonNegativeInt() : "") + ")";
                }
            }
        };
        if (innerCommand == null) {
            // TODO: figure out a default that maybe makes more sense than using a timestamp field
            innerCommand = "count_over_time(" + randomDateField(previousOutput) + ")";
        }
        return outerCommand + "(" + innerCommand + ")";
    }

    public static String agg(List<Column> previousOutput) {
        String name = randomNumericField(previousOutput);
        // complex with numerics
        if (name != null && randomBoolean()) {
            int ops = randomIntBetween(1, 3);
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < ops; i++) {
                if (i > 0) {
                    result.append(" + ");
                }
                String agg = switch (randomIntBetween(0, 5)) {
                    case 0 -> "max(" + name + ")";
                    case 1 -> "min(" + name + ")";
                    case 2 -> "avg(" + name + ")";
                    case 3 -> "median(" + name + ")";
                    case 4 -> "sum(" + name + ")";
                    default -> "count(" + name + ")";
                    // TODO more numerics
                };
                result.append(agg);
            }
            return result.toString();
        }
        // all types
        name = randomBoolean() ? randomStringField(previousOutput) : randomNumericOrDateField(previousOutput);
        if (name == null) {
            return "count(*)";
        }
        if (randomBoolean()) {
            String exp = expression(previousOutput, false);
            name = exp == null ? name : exp;
        }
        return switch (randomIntBetween(0, 5)) {
            case 0 -> "count(*)";
            case 1 -> "count(" + name + ")";
            case 2 -> "absent(" + name + ")";
            case 3 -> "present(" + name + ")";
            case 4 -> "values(" + name + ")";
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

    public static String randomMetricsNumericField(List<Column> previousOutput) {
        Set<String> allowedTypes = Set.of("double", "long", "unsigned_long", "integer", "aggregate_metric_double");
        List<String> items = previousOutput.stream()
            .filter(
                x -> allowedTypes.contains(x.type())
                    || (x.type().equals("unsupported") && canBeCastedToAggregateMetricDouble(x.originalTypes()))
            )
            .map(Column::name)
            .toList();
        if (items.isEmpty()) {
            return null;
        }
        return items.get(randomIntBetween(0, items.size() - 1));
    }

    public static String randomCounterField(List<Column> previousOutput) {
        return randomName(previousOutput, Set.of("counter_long", "counter_double", "counter_integer"));
    }

    private static boolean canBeCastedToAggregateMetricDouble(List<String> types) {
        return types.contains("aggregate_metric_double")
            && Set.of("double", "long", "unsigned_long", "integer", "aggregate_metric_double").containsAll(types);
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

    /**
     * @param previousOutput columns that can be used in the expression
     * @param allowConstants if set to true, this will never return a constant expression.
     *                       If no expression can be generated, it will return null
     * @return an expression or null
     */
    public static String expression(List<Column> previousOutput, boolean allowConstants) {
        if (randomBoolean() && allowConstants) {
            return constantExpression();
        }
        if (randomBoolean()) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < randomIntBetween(1, 3); i++) {
                String field = randomNumericField(previousOutput);
                if (field == null) {
                    return allowConstants ? constantExpression() : null;
                }
                if (i > 0) {
                    result.append(" + ");
                }
                result.append(field);
            }
            return result.toString();
        }
        if (randomBoolean()) {
            String field = randomKeywordField(previousOutput);
            if (field == null) {
                return allowConstants ? constantExpression() : null;
            }
            return switch (randomIntBetween(0, 3)) {
                case 0 -> "substring(" + field + ", 1, 3)";
                case 1 -> "to_lower(" + field + ")";
                case 2 -> "to_upper(" + field + ")";
                default -> "length(" + field + ")";
            };
        }
        if (randomBoolean() || allowConstants == false) {
            String field = randomStringField(previousOutput);
            if (field == null || randomBoolean()) {
                field = randomNumericOrDateField(previousOutput);
            }
            return field;
        }
        return allowConstants ? constantExpression() : null;
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

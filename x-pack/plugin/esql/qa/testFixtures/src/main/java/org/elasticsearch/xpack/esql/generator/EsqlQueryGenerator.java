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
import org.elasticsearch.xpack.esql.generator.command.pipe.SampleGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.SortGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.TimeSeriesStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.UriPartsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.WhereGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.PromQLGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.TimeSeriesGenerator;
import org.elasticsearch.xpack.esql.parser.ParserUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.areUnmappedFieldsAllowed;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.binaryMathFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.caseFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.cidrMatchFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.clampFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.coalesceFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.concatFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.conversionFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.dateDiffFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.dateFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.greatestLeastFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.inExpression;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.ipPrefixFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.isNullExpression;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.likeExpression;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.mathFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.mvFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.mvSliceZipFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.rlikeExpression;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.splitFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.stringFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.stringToBoolFunction;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.stringToIntFunction;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.randomUnmappedFieldName;

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
     * Commands at the beginning of PromQL queries, eg. PROMQL
     */
    static List<CommandGenerator> PROMQL_SOURCE_COMMANDS = List.of(PromQLGenerator.INSTANCE);

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
        SampleGenerator.INSTANCE,
        SortGenerator.INSTANCE,
        StatsGenerator.INSTANCE,
        UriPartsGenerator.INSTANCE,
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

        default void clearCommandHistory() {
            throw new IllegalArgumentException("Clearing command history is not allowed");
        }
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
        CommandGenerator.CommandDescription desc;

        if (commandGenerator instanceof PromQLGenerator promQLGenerator) {
            // do a dummy query to get available fields first
            // TODO: modify when METRICS_INFO available https://github.com/elastic/elasticsearch/issues/141413
            String index = promQLGenerator.generateIndices(schema);
            var fromDesc = new CommandGenerator.CommandDescription("from", FromGenerator.INSTANCE, "FROM " + index, Map.of());
            executor.run(FromGenerator.INSTANCE, fromDesc);
            executor.clearCommandHistory();
            desc = promQLGenerator.generateWithIndices(List.of(), executor.currentSchema(), schema, queryExecutor, index);
            canGenerateTimeSeries = false;
        } else {
            desc = commandGenerator.generate(List.of(), List.of(), schema, queryExecutor);
        }
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

    /**
     * Generates a boolean expression.
     * @deprecated Use {@link #booleanExpression(List, List)} instead to properly handle unmapped fields
     */
    @Deprecated
    public static String booleanExpression(List<Column> previousOutput) {
        return booleanExpression(previousOutput, null);
    }

    /**
     * Generates a boolean expression.
     * @param previousOutput the columns available in the current schema
     * @param previousCommands the list of commands executed so far (used to determine if unmapped fields are allowed)
     */
    public static String booleanExpression(List<Column> previousOutput, List<CommandGenerator.CommandDescription> previousCommands) {
        boolean allowUnmapped = areUnmappedFieldsAllowed(previousCommands);
        return switch (randomIntBetween(0, 11)) {
            case 0, 1, 2 -> {
                String field = randomNumericField(previousOutput);
                if (field == null) {
                    yield null;
                }
                yield field + " " + mathCompareOperator() + " " + randomIntBetween(-100, 100);
            }
            case 3 -> "true";
            case 4 -> "false";
            case 5 -> isNullExpression(previousOutput, allowUnmapped); // IS NULL / IS NOT NULL
            case 6 -> stringToBoolFunction(previousOutput, allowUnmapped); // String comparison functions: starts_with, ends_with, contains
            case 7 -> inExpression(previousOutput, allowUnmapped);
            case 8 -> likeExpression(previousOutput, allowUnmapped);
            case 9 -> rlikeExpression(previousOutput, allowUnmapped);
            case 10 -> cidrMatchFunction(previousOutput, allowUnmapped);
            default -> {
                // Numeric comparison on function result
                String funcExpr = stringToIntFunction(previousOutput, allowUnmapped);
                yield funcExpr == null ? null : funcExpr + " " + mathCompareOperator() + " " + randomIntBetween(0, 20);
            }
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
        // If the raw name needs quoting (contains special characters), we must quote it
        if (needsQuoting(result)) {
            return quote(result);
        }
        if (randomBoolean() && result.contains("*") == false) {
            return quote(result);
        }
        return result;
    }

    public static boolean needsQuoting(String rawName) {
        return rawName.contains("`") || rawName.contains("-");
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
        String result = randomFrom(candidates).name();
        if (needsQuoting(result)) {
            return quote(result);
        }
        return result;
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
        String result = randomFrom(candidates).name();
        if (needsQuoting(result)) {
            return quote(result);
        }
        return result;
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

    /**
     * @deprecated Use {@link #agg(List, List)} instead to properly handle unmapped fields
     */
    @Deprecated
    public static String agg(List<Column> previousOutput) {
        return agg(previousOutput, null);
    }

    public static String agg(List<Column> previousOutput, List<CommandGenerator.CommandDescription> previousCommands) {
        boolean allowUnmapped = areUnmappedFieldsAllowed(previousCommands);
        var unmappedFieldName = randomUnmappedFieldName();
        // Only use unmapped field if allowed and it doesn't exist in the schema
        var canUseUnmappedFieldName = allowUnmapped && previousOutput.stream().noneMatch(x -> x.name().equals(unmappedFieldName));
        String name = canUseUnmappedFieldName && randomBoolean() ? unmappedFieldName : randomNumericField(previousOutput);
        // complex with numerics
        if (name != null && randomBoolean()) {
            int ops = randomIntBetween(1, 3);
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < ops; i++) {
                if (i > 0) {
                    result.append(" + ");
                }
                String agg = switch (randomIntBetween(0, 11)) {
                    case 0 -> "max(" + name + ")";
                    case 1 -> "min(" + name + ")";
                    case 2 -> "avg(" + name + ")";
                    case 3 -> "median(" + name + ")";
                    case 4 -> "sum(" + name + ")";
                    case 5 -> "count(" + name + ")";
                    case 6 -> "percentile(" + name + ", " + randomIntBetween(1, 99) + ")";
                    case 7 -> "std_dev(" + name + ")";
                    case 8 -> "variance(" + name + ")";
                    case 9 -> "median_absolute_deviation(" + name + ")";
                    case 10 -> "weighted_avg(" + name + ", " + randomIntBetween(1, 10) + ")";
                    default -> "count_distinct(" + name + ")";
                };
                result.append(agg);
            }
            return result.toString();
        }
        // all types
        name = randomBoolean() ? randomStringField(previousOutput) : randomNumericOrDateField(previousOutput);
        var unmappedFieldName2 = randomUnmappedFieldName();
        // Only use unmapped field if allowed and it doesn't exist in the schema
        canUseUnmappedFieldName = allowUnmapped && previousOutput.stream().noneMatch(x -> x.name().equals(unmappedFieldName2));
        name = randomBoolean() && canUseUnmappedFieldName ? unmappedFieldName2 : name;
        if (name == null) {
            return "count(*)";
        }
        if (randomBoolean()) {
            String exp = expression(previousOutput, false, previousCommands);
            name = exp == null ? name : exp;
        }
        // For type-constrained agg functions (top, sample, first), use a type-safe field/expression
        // instead of the arbitrary 'name' which may have an incompatible type (e.g. date_range from coalesce)
        final String anyName = name;
        return switch (randomIntBetween(0, 9)) {
            case 0 -> "count(*)";
            case 1 -> "count(" + anyName + ")";
            case 2 -> "absent(" + anyName + ")";
            case 3 -> "present(" + anyName + ")";
            case 4 -> "values(" + anyName + ")";
            case 5 -> "count_distinct(" + anyName + ")";
            case 6, 7 -> {
                // top() accepts: boolean, double, integer, long, date, ip, keyword, text
                Set<String> topTypes = Set.of("boolean", "double", "integer", "long", "date", "datetime", "ip", "keyword", "text");
                String topField = FunctionGenerator.typeSafeExpression(previousOutput, topTypes, allowUnmapped);
                if (topField == null) topField = anyName;
                String order = randomIntBetween(0, 1) == 0 ? "asc" : "desc";
                yield "top(" + topField + ", " + randomIntBetween(1, 5) + ", \"" + order + "\")";
            }
            case 8 -> {
                // sample() - use a commonly supported field to avoid type issues
                String sampleField = randomName(previousOutput, FunctionGenerator.COMMONLY_SUPPORTED_TYPES);
                if (sampleField == null) sampleField = anyName;
                yield "sample(" + sampleField + ", " + randomIntBetween(1, 10) + ")";
            }
            default -> "first(" + anyName + ", " + randomDateField(previousOutput) + ")";
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
        String result = items.get(randomIntBetween(0, items.size() - 1));
        if (needsQuoting(result)) {
            return quote(result);
        }
        return result;
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
        String result = items.get(randomIntBetween(0, items.size() - 1));
        if (needsQuoting(result)) {
            return quote(result);
        }
        return result;
    }

    /**
     * @param previousOutput columns that can be used in the expression
     * @param allowConstants if set to true, this will never return a constant expression.
     *                       If no expression can be generated, it will return null
     * @return an expression or null
     * @deprecated Use {@link #expression(List, boolean, List)} instead to properly handle unmapped fields
     */
    @Deprecated
    public static String expression(List<Column> previousOutput, boolean allowConstants) {
        return expression(previousOutput, allowConstants, null);
    }

    /**
     * @param previousOutput columns that can be used in the expression
     * @param allowConstants if set to true, this will never return a constant expression.
     *                       If no expression can be generated, it will return null
     * @param previousCommands the list of commands executed so far (used to determine if unmapped fields are allowed)
     * @return an expression or null
     */
    public static String expression(
        List<Column> previousOutput,
        boolean allowConstants,
        List<CommandGenerator.CommandDescription> previousCommands
    ) {
        if (randomBoolean() && allowConstants) {
            return constantExpression();
        }
        // Try to generate a function expression with high probability
        if (randomIntBetween(0, 10) < 7) {
            String funcExpr = functionExpression(previousOutput, previousCommands);
            if (funcExpr != null) {
                return funcExpr;
            }
        }
        // Arithmetic expression with fields
        if (randomBoolean()) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < randomIntBetween(1, 3); i++) {
                String field = randomNumericField(previousOutput);
                if (field == null) {
                    return allowConstants ? constantExpression() : null;
                }
                if (i > 0) {
                    result.append(randomFrom(" + ", " - ", " * "));
                }
                result.append(field);
            }
            return result.toString();
        }
        // Field reference or constant
        if (randomBoolean() || allowConstants == false) {
            String field = randomStringField(previousOutput);
            if (field == null || randomBoolean()) {
                field = randomNumericOrDateField(previousOutput);
            }
            return field;
        }
        return allowConstants ? constantExpression() : null;
    }

    /**
     * Generates a random function expression.
     * @param previousOutput the columns available in the current schema
     * @param previousCommands the list of commands executed so far (used to determine if unmapped fields are allowed)
     */
    public static String functionExpression(List<Column> previousOutput, List<CommandGenerator.CommandDescription> previousCommands) {
        boolean allowUnmapped = areUnmappedFieldsAllowed(previousCommands);
        return switch (randomIntBetween(0, 18)) {
            case 0, 1 -> mathFunction(previousOutput, allowUnmapped);
            case 2 -> binaryMathFunction(previousOutput, allowUnmapped);
            case 3, 4 -> stringFunction(previousOutput, allowUnmapped);
            case 5 -> stringToIntFunction(previousOutput, allowUnmapped);
            case 6 -> dateFunction(previousOutput, allowUnmapped);
            case 7 -> conversionFunction(previousOutput, allowUnmapped);
            case 8 -> caseFunction(previousOutput, allowUnmapped);
            case 9 -> coalesceFunction(previousOutput, allowUnmapped);
            case 10, 11 -> mvFunction(previousOutput, allowUnmapped);
            case 12 -> concatFunction(previousOutput, allowUnmapped);
            case 13 -> greatestLeastFunction(previousOutput, allowUnmapped);
            case 14 -> mvSliceZipFunction(previousOutput, allowUnmapped);
            case 15 -> splitFunction(previousOutput, allowUnmapped);
            case 16 -> clampFunction(previousOutput, allowUnmapped);
            case 17 -> dateDiffFunction(previousOutput, allowUnmapped);
            default -> ipPrefixFunction(previousOutput, allowUnmapped);
        };
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

    public static String quote(String rawName) {
        return ParserUtils.quoteIdString(rawName);
    }

    public static String unquote(String colName) {
        if (colName.length() >= 2 && colName.startsWith("`") && colName.endsWith("`")) {
            return ParserUtils.unquoteIdString(colName);
        }
        return colName;
    }

}

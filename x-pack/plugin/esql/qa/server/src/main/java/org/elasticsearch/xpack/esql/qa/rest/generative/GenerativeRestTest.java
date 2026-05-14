/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.LookupIdxColumn;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.DissectGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EnrichGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.EvalGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.GrokGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.InlineStatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.LookupJoinGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.MvExpandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.RegisteredDomainGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.RenameGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.StatsGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.UriPartsGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.ENRICH_POLICIES;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.availableDatasetsForEs;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.loadDataSetIntoEs;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.COLUMN_NAME;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.COLUMN_ORIGINAL_TYPES;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.COLUMN_TYPE;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.unquote;
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.UNMAPPED_FIELD_NAMES;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.SET_UNMAPPED_FIELDS_PREFIX;

public abstract class GenerativeRestTest extends ESRestTestCase implements QueryExecutor {

    @Rule(order = Integer.MIN_VALUE)
    public ProfileLogger profileLogger = new ProfileLogger();

    public static final int ITERATIONS = 100;
    public static final int MAX_DEPTH = 20;

    public static final Set<String> ALLOWED_ERRORS = Set.of(
        "Reference \\[.*\\] is ambiguous",
        "Cannot use field \\[.*\\] due to ambiguities",
        "cannot sort on .*",
        "argument of \\[count.*\\] must",
        "Cannot use field \\[.*\\] with unsupported type \\[.*\\]",
        "Unbounded SORT not supported yet",
        "MV_EXPAND .* cannot yet have an unbounded SORT .* before it",
        "The field names are too complex to process", // field_caps problem
        "must be \\[any type except counter types\\]", // TODO refine the generation of count()
        "INLINE STATS cannot be used after an explicit or implicit LIMIT command",
        // Full-text functions and `:` operator are not allowed after FORK
        "(?:(?:\\[(?:KQL|QSTR|MATCH|MatchPhrase)] function)|(?:\\[:\\] operator)) cannot be used after FORK",
        "sub-plan execution results too large",  // INLINE STATS limitations
        // this comes from mapping-all-types.json and it gets occasionally picked up by full text functions
        "Inference endpoint not found \\[foo_inference_id\\]",
        // full-text functions are not allowed to match on fields that come from lookup indices
        "cannot operate on \\[.*\\], supplied by an index \\[.*\\] in non-STANDARD mode \\[lookup\\]",
        "Can only use fuzzy queries on keyword and text fields - not on \\[.*\\] which is of type \\[.*\\]",
        // full-text function receiving a non-boolean value for a boolean type field
        "Can't parse boolean value \\[.*\\], expected \\[true\\] or \\[false\\]",
        // full-text function trying to parse text as date field and failing
        "failed to parse date field \\[.*\\] with format",
        // full-text function trying to parse a non-IP string
        "is not an IP string literal",
        // a values(<that field>) agg could more than 100,000 values into a single multi-valued field, and a subsequent
        // inline stats … by <that field> hits the hard limit Block.MAX_LOOKUP = 100_000 in the compute layer
        // throwing IllegalArgumentException via PackedValuesBlockHash
        // see https://github.com/elastic/elasticsearch/issues/145694
        "Found a single entry with .* entries",

        // Awaiting fixes for query failure
        "Unknown column \\[<all-fields-projected>\\]", // https://github.com/elastic/elasticsearch/issues/121741,
        // https://github.com/elastic/elasticsearch/issues/125866
        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references",
        "The incoming YAML document exceeds the limit:", // still to investigate, but it seems to be specific to the test framework
        "Data too large", // Circuit breaker exceptions eg. https://github.com/elastic/elasticsearch/issues/130072
        "long overflow", // https://github.com/elastic/elasticsearch/issues/99575
        // "optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/138231
        // https://github.com/elastic/elasticsearch/issues/142537 for null arguments in clamp() function
        "'field' must not be null in clamp\\(\\)", // clamp/clamp_min/clamp_max reject NULL field from unmapped fields
        "must be \\[boolean, date, ip, string or numeric except unsigned_long or counter types\\]", // type mismatch in top() arguments
        "Does not support yet aggregations over constants", // https://github.com/elastic/elasticsearch/issues/118292
        "Field \\[.*\\] of type \\[.*\\] does not support match.* queries",
        // https://github.com/elastic/elasticsearch/issues/145570
        "function cannot operate on \\[.*\\], which is not a field from an index mapping",
        "\\[:\\] operator cannot operate on \\[.*\\], which is not a field from an index mapping",

        // Awaiting fixes for correctness
        "Expecting at most \\[.*\\] columns, got \\[.*\\]", // https://github.com/elastic/elasticsearch/issues/129561

        // TS-command tests (acceptable errors)
        "time-series.*the first aggregation.*is not allowed",
        "count_star .* can't be used with TS command",
        "time_series aggregate.* can only be used with the TS command",
        "implicit time-series aggregation function",
        "INLINE STATS .* can only be used after STATS when used with TS command",
        "cannot group by a metric field .* in a time-series aggregation",
        "@timestamp field of type date or date_nanos",
        "which was either not present in the source index, or has been dropped or renamed",
        "second argument of .* must be \\[date_nanos or datetime\\], found value \\[@timestamp\\] type \\[.*\\]",
        "expected named expression for grouping; got ",
        "Time-series aggregations require direct use of @timestamp which was not found. If @timestamp was renamed in EVAL, "
            + "use the original @timestamp field instead.", // https://github.com/elastic/elasticsearch/pull/141196

        // _doc field unexpectedly appearing in output after FORK
        "Output has changed from \\[.*\\] to \\[.*_doc.*\\]", // https://github.com/elastic/elasticsearch/issues/146856

        // TopNOperator type mismatch in ValueExtractor
        "Expected \\[.*\\] but was \\[.*\\].*ValueExtractor" // https://github.com/elastic/elasticsearch/issues/146850
    );

    /**
     * FORK converts UnsupportedAttribute to a plain ReferenceAttribute(UNSUPPORTED), stripping the
     * Unresolvable marker. Functions then reject the argument with a generic "type [unsupported]" error
     * instead of the proper "Cannot use field [X] with unsupported type [Y]" message.
     * <a href="https://github.com/elastic/elasticsearch/issues/147603">github issue</a>
     */
    private static final Pattern UNSUPPORTED_TYPE_AFTER_FORK_PATTERN = Pattern.compile(
        ".*argument of \\[.*] must be \\[.*], found value \\[.*] type \\[unsupported].*",
        Pattern.DOTALL
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    /**
     * Matches "Unknown column [X]" errors, optionally followed by ", did you mean [Y]?".
     * This error is expected when an unmapped field is used after a schema-fixing command (KEEP, DROP, STATS)
     * that included a different unmapped field but not this one, making the second one legitimately unknown.
     * We only allow this error when the unknown column is an unmapped field name, and if a suggestion is present,
     * the suggested column must also be an unmapped field name.
     */
    private static final Pattern UNKNOWN_COLUMN_WITH_SUGGESTION_PATTERN = Pattern.compile(
        ".*Unknown column \\[([^]]+)], did you mean \\[([^]]+)]\\?.*",
        Pattern.DOTALL
    );
    private static final Pattern UNKNOWN_COLUMN_PATTERN = Pattern.compile(".*Unknown column \\[([^]]+)].*", Pattern.DOTALL);

    /**
     * Matches "... argument of [X] must be [Y], found value [Z] type [T]" errors.
     * This happens when an unmapped field ends up with a different data type that doesn't match the one of the function's argument(s).
     * The unmapped field name may appear either in the function expression(group 1) or in the found-value position (group 2).
     */
    private static final Pattern ANY_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*argument of \\[([^]]+)] must be \\[.*], found value \\[([^]]+)] type \\[.*].*",
        Pattern.DOTALL
    );
    /**
     * Matches type mismatch errors where a function argument has one of the special types that most scalar functions reject.
     * For example: "must be [any type except counter types, dense_vector, ...], found value [...] type [counter_long]"
     */
    private static final Pattern SCALAR_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*found value \\[[^]]+] type \\[(counter_long|counter_double|counter_integer"
            + "|aggregate_metric_double|dense_vector|tdigest|histogram|exponential_histogram|date_range)].*",
        Pattern.DOTALL
    );

    private static final Set<String> UNMAPPED_NAMES = Set.of(UNMAPPED_FIELD_NAMES);

    @Before
    public void setup() throws IOException {
        if (indexExists(CSV_DATASET.keySet().iterator().next()) == false) {
            loadDataSetIntoEs(client(), true, supportsSourceFieldMapping(), false);
        }
    }

    protected abstract boolean supportsSourceFieldMapping();

    protected boolean requiresTimeSeries() {
        return false;
    }

    @AfterClass
    public static void wipeTestData() throws IOException {
        try {
            adminClient().performRequest(new Request("DELETE", "/*"));
        } catch (ResponseException e) {
            // 404 here just means we had no indexes
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
        }
    }

    public void test() throws IOException {
        List<String> indices = availableIndices();
        List<LookupIdx> lookupIndices = lookupIndices();
        Collection<CsvTestsDataLoader.EnrichConfig> policies = ENRICH_POLICIES.values();
        CommandGenerator.QuerySchema mappingInfo = new CommandGenerator.QuerySchema(indices, lookupIndices, policies);

        for (int i = 0; i < ITERATIONS; i++) {
            var exec = new EsqlQueryGenerator.Executor() {
                @Override
                public void run(CommandGenerator generator, CommandGenerator.CommandDescription current) {
                    final String command = current.commandString();

                    QueryExecuted result = previousResult == null
                        ? execute(command, 0)
                        : execute(previousResult.query() + command, previousResult.depth());

                    final boolean hasException = result.exception() != null;
                    if (hasException
                        || checkPipelineResults(previousCommands, generator, current, previousResult, result, currentSchema)
                            .success() == false) {
                        if (hasException) {
                            List<CommandGenerator.CommandDescription> commands = new ArrayList<>(previousCommands.size() + 1);
                            commands.addAll(previousCommands);
                            commands.add(current);
                            checkPipelineException(result, commands, currentSchema);
                        }
                        continueExecuting = false;
                        currentSchema = List.of();
                    } else {
                        continueExecuting = true;
                        currentSchema = updateIndexMapped(result.outputSchema(), currentSchema, current);
                    }

                    previousCommands.add(current);
                    previousResult = result;
                }

                @Override
                public List<CommandGenerator.CommandDescription> previousCommands() {
                    return previousCommands;
                }

                @Override
                public boolean continueExecuting() {
                    return continueExecuting;
                }

                @Override
                public List<Column> currentSchema() {
                    return currentSchema;
                }

                @Override
                public void clearCommandHistory() {
                    previousCommands = new ArrayList<>();
                    previousResult = null;
                }

                boolean continueExecuting;
                List<Column> currentSchema;
                List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
                QueryExecuted previousResult;
            };
            try {
                EsqlQueryGenerator.generatePipeline(MAX_DEPTH, sourceCommand(), mappingInfo, exec, requiresTimeSeries(), this);
            } catch (Exception e) {
                // query failures are AssertionErrors, if we get here it's an unexpected exception in the query generation
                if (isAllowedError(e.getMessage()) == false) {
                    StringBuilder message = new StringBuilder();
                    message.append("Generative tests, error generating new command \n");
                    message.append("Previous query: \n");
                    message.append(exec.previousResult.query());
                    fail(e, message.toString());
                }
            }
        }
    }

    /**
     * Strips {@code _approximation_} metadata columns from a {@link QueryExecuted}.
     */
    private static QueryExecuted stripApproximationColumns(QueryExecuted qe) {
        if (qe == null || qe.outputSchema() == null) {
            return qe;
        }
        List<Integer> keepIndices = new ArrayList<>();
        for (int i = 0; i < qe.outputSchema().size(); i++) {
            if (qe.outputSchema().get(i).name().startsWith(ApproximationPlan.CONFIDENCE_INTERVAL_COLUMN_PREFIX) == false
                && qe.outputSchema().get(i).name().startsWith(ApproximationPlan.CERTIFIED_COLUMN_PREFIX) == false) {
                keepIndices.add(i);
            }
        }
        if (keepIndices.size() == qe.outputSchema().size()) {
            return qe;
        }
        List<Column> schema = keepIndices.stream().map(qe.outputSchema()::get).toList();
        List<List<Object>> rows = qe.result() == null
            ? null
            : qe.result().stream().map(row -> keepIndices.stream().map(row::get).toList()).toList();
        return new QueryExecuted(qe.query(), qe.depth(), schema, rows, qe.exception());
    }

    protected CommandGenerator sourceCommand() {
        return EsqlQueryGenerator.sourceCommand();
    }

    protected CommandGenerator.ValidationResult checkPipelineResults(
        List<CommandGenerator.CommandDescription> previousCommands,
        CommandGenerator commandGenerator,
        CommandGenerator.CommandDescription commandDescription,
        QueryExecuted previousResult,
        QueryExecuted result,
        List<Column> currentSchema
    ) {
        return checkResults(previousCommands, commandGenerator, commandDescription, previousResult, result, currentSchema);
    }

    protected void checkPipelineException(
        QueryExecuted query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        checkException(query, previousCommands, currentSchema);
    }

    private record FailureContext(
        String errorMessage,
        String normalizedErrorMessage,
        String query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        FailureContext(
            String errorMessage,
            String query,
            List<CommandGenerator.CommandDescription> previousCommands,
            List<Column> currentSchema
        ) {
            this(
                errorMessage,
                errorMessage == null ? null : normalizeErrorMessage(errorMessage),
                query,
                previousCommands == null ? List.of() : previousCommands,
                currentSchema == null ? List.of() : currentSchema
            );
        }
    }

    private static final AllowedFailureRule[] ALLOWED_FAILURE_RULES = {
        ctx -> matchesAllowedErrorPatterns(ctx.normalizedErrorMessage),
        ctx -> isUnmappedFieldError(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isScalarTypeMismatchError(ctx.normalizedErrorMessage),
        ctx -> isFieldFullTextError(ctx.normalizedErrorMessage, ctx.query, ctx.previousCommands, ctx.currentSchema),
        ctx -> isFullTextAfterWhereBugs(ctx.normalizedErrorMessage),
        ctx -> isLenientFalseFailedToCreateFullTextQueryError(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isTsOutputChangedError(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isUnsupportedTypeAfterForkError(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isForkWithSortBranchBug(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isForkTopNIndexOutOfBoundsBug(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isForkOptimizedIncorrectlyBug(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isRenameMvExpandOrderByBug(ctx.normalizedErrorMessage, ctx.query),
        ctx -> isLimitByMvExpandBug(ctx.normalizedErrorMessage, ctx.query), };

    private static boolean isAllowedFailure(FailureContext ctx) {
        if (ctx == null || ctx.errorMessage == null) {
            return false;
        }
        for (AllowedFailureRule rule : ALLOWED_FAILURE_RULES) {
            if (rule.matches(ctx)) {
                return true;
            }
        }
        return false;
    }

    protected static CommandGenerator.ValidationResult checkResults(
        List<CommandGenerator.CommandDescription> previousCommands,
        CommandGenerator commandGenerator,
        CommandGenerator.CommandDescription commandDescription,
        QueryExecuted previousResult,
        QueryExecuted result,
        List<Column> currentSchema
    ) {
        CommandGenerator.ValidationResult outputValidation = commandGenerator.validateOutput(
            previousCommands,
            commandDescription,
            previousResult == null ? null : previousResult.outputSchema(),
            previousResult == null ? null : previousResult.result(),
            result.outputSchema(),
            result.result()
        );
        failOnUnexpectedValidationError(outputValidation, result, previousCommands, currentSchema);
        return outputValidation;
    }

    protected static void failOnUnexpectedValidationError(
        CommandGenerator.ValidationResult outputValidation,
        QueryExecuted result,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        if (outputValidation.success() == false) {
            if (isAllowedFailure(new FailureContext(outputValidation.errorMessage(), result.query(), previousCommands, currentSchema))) {
                return;
            }
            fail("query: " + result.query() + "\nerror: " + outputValidation.errorMessage());
        }
    }

    protected void checkException(
        QueryExecuted query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        if (isAllowedFailure(new FailureContext(query.exception().getMessage(), query.query(), previousCommands, currentSchema))) {
            return;
        }
        fail("query: " + query.query() + "\nexception: " + query.exception().getMessage());
    }

    /**
     * Long lines in exceptions can be split across several lines. When a newline is inserted, the end of the current line and the beginning
     * of the new line are marked with a backslash {@code \}; the new line will also have whitespace before the backslash for aligning.
     */
    private static final Pattern ERROR_MESSAGE_LINE_BREAK = Pattern.compile("\\\\\r?\n\\s*\\\\");

    private static String normalizeErrorMessage(String errorMessage) {
        return ERROR_MESSAGE_LINE_BREAK.matcher(errorMessage).replaceAll("");
    }

    protected static boolean isAllowedError(String errorMessage) {
        if (errorMessage == null) {
            return false;
        }
        return matchesAllowedErrorPatterns(normalizeErrorMessage(errorMessage));
    }

    private static boolean matchesAllowedErrorPatterns(String normalizedErrorMessage) {
        for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
            if (allowedError.matcher(normalizedErrorMessage).matches()) {
                return true;
            }
        }
        return false;
    }

    protected static boolean isAllowedError(String errorMessage, Pattern allowedPattern) {
        return allowedPattern.matcher(normalizeErrorMessage(errorMessage)).matches();
    }

    /**
     * Checks if the error is a known unmapped field error. This covers:
     * <ul>
     *   <li>"Unknown column [X], did you mean [Y]?" - both X and Y must be unmapped field names</li>
     *   <li>"Unknown column [X]" (no suggestion) - X must be an unmapped field name</li>
     *   <li>"first argument of [X] is [null] so second argument must also be [null] but was [Y]" -
     *       the expression X must contain an unmapped field name (https://github.com/elastic/elasticsearch/issues/142115)</li>
     *
     * </ul>
     */
    private static boolean isUnmappedFieldError(String errorMessage, String query) {
        return isUnmappedFieldPrefixError(errorMessage, query, SET_UNMAPPED_FIELDS_PREFIX);
    }

    protected static boolean isUnmappedFieldPrefixError(String errorMessage, String query, String prefix) {
        if (query.startsWith(prefix) == false) {
            return false;
        }
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);

        Matcher matcher = UNKNOWN_COLUMN_WITH_SUGGESTION_PATTERN.matcher(errorWithoutLineBreaks);
        if (matcher.matches()) {
            String unknownColumn = matcher.group(1);
            String suggestedColumn = matcher.group(2);
            return UNMAPPED_NAMES.contains(unknownColumn) && UNMAPPED_NAMES.contains(suggestedColumn);
        }

        matcher = UNKNOWN_COLUMN_PATTERN.matcher(errorWithoutLineBreaks);
        if (matcher.matches()) {
            String unknownColumn = matcher.group(1);
            return UNMAPPED_NAMES.contains(unknownColumn);
        }

        matcher = ANY_TYPE_MISMATCH_PATTERN.matcher(errorWithoutLineBreaks);
        if (matcher.matches()) {
            String functionExpression = matcher.group(1);
            String foundValue = matcher.group(2);
            return UNMAPPED_NAMES.stream().anyMatch(name -> functionExpression.contains(name) || foundValue.contains(name));
        }

        return false;
    }

    /**
     * Checks if the error is a type mismatch where a function received a field of a special type that most scalar
     * functions don't support (counter types, aggregate_metric_double, dense_vector, tdigest, histogram, etc.).
     * These errors are acceptable since the generative tests may compose function calls with fields of these types.
     */
    private static boolean isScalarTypeMismatchError(String errorMessage) {
        return SCALAR_TYPE_MISMATCH_PATTERN.matcher(errorMessage).matches();
    }

    private static final Pattern NOT_A_FIELD_FROM_INDEX_PATTERN = Pattern.compile(
        ".*cannot operate on \\[([^]]+)\\], which is not a field from an index mapping.*",
        Pattern.DOTALL
    );

    /**
     * Captures fields created by GROK patterns, e.g. {@code %{WORD:foo}} or {@code %{NUMBER:bar:int}}.
     */
    private static final Pattern GROK_GENERATED_FIELD_PATTERN = Pattern.compile("%\\{[^:}]+:([^}:]+)(?::[^}]+)?}");
    /**
     * Captures fields created by DISSECT patterns, e.g. {@code %{foo}}. Ignores skip fields like {@code %{?}} or {@code %{?skip}}.
     */
    private static final Pattern DISSECT_GENERATED_FIELD_PATTERN = Pattern.compile("%\\{([^}]+)}");

    /**
     * Captures both the source and target of a RENAME clause, e.g. {@code old_field AS new_field}.
     * Group 1 is the source (possibly back-tick quoted), group 2 is the target.
     */
    private static final Pattern RENAME_PAIR_PATTERN = Pattern.compile("\\s*(`[^`]+`|[^,\\s]+)\\s+[Aa][Ss]\\s+(`[^`]+`|[^,\\s]+)\\s*");

    /**
     * Matches {@code | rename X as Y} segments embedded in a LOOKUP JOIN command string.
     * {@link LookupJoinGenerator} prepends rename commands to align the left-side key columns
     * with the lookup index key names; these renames are part of a single {@link CommandGenerator.CommandDescription}
     * and must be accounted for when propagating {@link Column#indexMapped()} flags.
     */
    private static final Pattern EMBEDDED_RENAME_PATTERN = Pattern.compile(
        "(?i)\\|\\s*rename\\s+(`[^`]+`|[^\\s|]+)\\s+as\\s+(`[^`]+`|[^\\s|]+)"
    );

    /**
     * Propagates the {@link Column#indexMapped()} flag through the pipeline after a command executes.
     * <p>
     * The REST API does not expose attribute-type information, so this method infers it from:
     * <ul>
     *   <li>The previous schema (columns carry their {@code indexMapped} status from earlier commands)</li>
     *   <li>The command description (command name and context — e.g. EVAL stores {@code NEW_COLUMNS})</li>
     * </ul>
     * Columns that are explicitly created by the command are marked {@code indexMapped=false};
     * columns that survive unchanged from the previous schema inherit their previous status.
     */
    static List<Column> updateIndexMapped(
        List<Column> newSchema,
        List<Column> previousSchema,
        CommandGenerator.CommandDescription command
    ) {
        if (newSchema == null || newSchema.isEmpty()) {
            return newSchema;
        }
        if (previousSchema == null || previousSchema.isEmpty()) {
            return newSchema;
        }

        String commandName = command.commandName();
        if (commandName == null) {
            return newSchema;
        }
        commandName = commandName.toLowerCase(Locale.ROOT);

        Map<String, Boolean> prevMapped = new HashMap<>();
        for (Column col : previousSchema) {
            prevMapped.put(col.name(), col.indexMapped());
        }

        Set<String> createdColumns = new HashSet<>();

        switch (commandName) {
            case EvalGenerator.EVAL -> {
                Object newCols = command.context().get(EvalGenerator.NEW_COLUMNS);
                if (newCols instanceof List<?> list) {
                    list.forEach(name -> createdColumns.add((String) name));
                }
            }
            case GrokGenerator.GROK -> {
                Matcher gm = GROK_GENERATED_FIELD_PATTERN.matcher(command.commandString());
                while (gm.find()) {
                    createdColumns.add(unquote(gm.group(1)));
                }
            }
            case DissectGenerator.DISSECT -> {
                Matcher dm = DISSECT_GENERATED_FIELD_PATTERN.matcher(command.commandString());
                while (dm.find()) {
                    String generated = dm.group(1);
                    if (generated.startsWith("?") == false) {
                        createdColumns.add(unquote(generated));
                    }
                }
            }
            case MvExpandGenerator.MV_EXPAND -> {
                String expanded = command.commandString().replaceFirst("(?i)^\\s*\\|\\s*mv_expand\\s+", "").trim();
                // Not truly a newly created column, but we need to override the indexMapped flag so that full-text functions don't use it.
                // https://github.com/elastic/elasticsearch/issues/142713
                createdColumns.add(unquote(expanded));
            }
            case StatsGenerator.STATS, InlineStatsGenerator.INLINE_STATS -> {
                return newSchema.stream().map(col -> new Column(col.name(), col.type(), col.originalTypes(), false)).toList();
            }
            case RenameGenerator.RENAME -> {
                return handleRenameIndexMapped(newSchema, prevMapped, command.commandString());
            }
            case RegisteredDomainGenerator.REGISTERED_DOMAIN -> {
                String prefix = (String) command.context().get("prefix");
                if (prefix != null) {
                    for (String subField : List.of("domain", "registered_domain", "top_level_domain", "subdomain")) {
                        createdColumns.add(prefix + "." + subField);
                    }
                }
            }
            case UriPartsGenerator.URI_PARTS -> {
                String prefix = (String) command.context().get("prefix");
                if (prefix != null) {
                    for (Column col : newSchema) {
                        if (col.name().startsWith(prefix + ".")) {
                            createdColumns.add(col.name());
                        }
                    }
                }
            }
            case EnrichGenerator.ENRICH -> {
                // Enrich fields can shadow existing index columns, so we use the policy's declared enrich_fields
                // from the context to ensure they are marked as non-index-mapped even when names collide.
                Object enrichFieldsObj = command.context().get(EnrichGenerator.ENRICH_FIELDS);
                if (enrichFieldsObj instanceof List<?> enrichFieldsList) {
                    enrichFieldsList.forEach(name -> createdColumns.add((String) name));
                }
            }
            case LookupJoinGenerator.LOOKUP_JOIN -> {
                // LookupJoinGenerator embeds RENAME commands before the actual LOOKUP JOIN to align
                // left-side key columns with lookup index key names. Process these renames so that
                // fields renamed from non-index-mapped sources correctly inherit indexMapped=false
                // instead of picking up the old indexMapped status of a same-named existing field.
                Matcher rm = EMBEDDED_RENAME_PATTERN.matcher(command.commandString());
                while (rm.find()) {
                    String oldName = unquote(rm.group(1).trim());
                    String newName = unquote(rm.group(2).trim());
                    boolean wasMapped = prevMapped.getOrDefault(oldName, false);
                    prevMapped.remove(oldName);
                    prevMapped.put(newName, wasMapped);
                }
            }
            default -> {
                // For commands that don't create named columns (KEEP, DROP, SORT, LIMIT, WHERE, etc.),
                // any column not in previous is from the command (e.g. LOOKUP_JOIN, CHANGE_POINT)
            }
        }

        return applyIndexMapped(newSchema, createdColumns, prevMapped);
    }

    private static List<Column> applyIndexMapped(List<Column> schema, Set<String> createdColumns, Map<String, Boolean> prevMapped) {
        return schema.stream().map(col -> {
            if (createdColumns.contains(col.name())) {
                return new Column(col.name(), col.type(), col.originalTypes(), false);
            }
            Boolean prev = prevMapped.get(col.name());
            if (prev != null) {
                return new Column(col.name(), col.type(), col.originalTypes(), prev);
            }
            return new Column(col.name(), col.type(), col.originalTypes(), false);
        }).toList();
    }

    private static List<Column> handleRenameIndexMapped(List<Column> newSchema, Map<String, Boolean> prevMapped, String commandString) {
        Map<String, Boolean> mapped = new HashMap<>(prevMapped);
        String body = commandString.replaceFirst("(?i)^\\s*\\|\\s*rename\\s+", "");
        for (String pair : body.split(",")) {
            Matcher m = RENAME_PAIR_PATTERN.matcher(pair);
            if (m.matches()) {
                String oldName = unquote(m.group(1).trim());
                String newName = unquote(m.group(2).trim());
                boolean wasMapped = mapped.getOrDefault(oldName, false);
                mapped.remove(oldName);
                mapped.put(newName, wasMapped);
            }
        }
        return newSchema.stream().map(col -> {
            Boolean isMapped = mapped.get(col.name());
            return new Column(col.name(), col.type(), col.originalTypes(), isMapped != null && isMapped);
        }).toList();
    }

    /**
     * Checks if the error is a full-text function/operator rejecting a field that is not from an index mapping.
     * Uses the {@link Column#indexMapped()} flag from the current schema when available; falls back to
     * command-history heuristics otherwise.
     */
    static boolean isFieldFullTextError(
        String errorMessage,
        String query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        Matcher m = NOT_A_FIELD_FROM_INDEX_PATTERN.matcher(errorMessage);
        if (m.matches() == false) {
            return false;
        }
        String fieldName = unquote(m.group(1));

        if (currentSchema != null && currentSchema.isEmpty() == false) {
            for (Column col : currentSchema) {
                if (col.name().equals(fieldName)) {
                    return col.indexMapped() == false;
                }
            }
            // Field not found in schema — likely a pipeline artifact; allow the error
            return true;
        }
        return false;
    }

    private static final Pattern FULL_TEXT_AFTER_SAMPLE_PATTERN = Pattern.compile(
        ".*\\[(KQL|QSTR)] function cannot be used after SAMPLE.*",
        Pattern.DOTALL
    );

    private static final Pattern FULL_TEXT_AFTER_WHERE_PATTERN = Pattern.compile(
        ".*(?:(?:\\[(?:KQL|QSTR|MATCH|MatchPhrase)] function)|(?:\\[:\\] operator)) cannot be used after \\(?(?i:WHERE).*",
        Pattern.DOTALL
    );

    /**
     * See https://github.com/elastic/elasticsearch/issues/142705
     * See https://github.com/elastic/elasticsearch/issues/142710
     */
    static boolean isFullTextAfterWhereBugs(String errorMessage) {
        return FULL_TEXT_AFTER_WHERE_PATTERN.matcher(errorMessage).matches();
    }

    private static final Pattern MATCH_LENIENT_FALSE_PATTERN = Pattern.compile(
        "(?i)\\bmatch\\s*\\([^)]*\\{[^}]*[\"']lenient[\"']\\s*:\\s*false[^}]*}[^)]*\\)"
    );
    private static final Pattern QSTR_LENIENT_FALSE_PATTERN = Pattern.compile(
        "(?i)\\bqstr\\s*\\([^)]*\\{[^}]*[\"']lenient[\"']\\s*:\\s*false[^}]*}[^)]*\\)"
    );

    /**
     * Work around a query-building failure in full-text functions when options include {@code {"lenient": false}}.
     */
    static boolean isLenientFalseFailedToCreateFullTextQueryError(String errorMessage, String query) {
        if (errorMessage.contains("failed to create query: For input string") == false) {
            return false;
        }
        return MATCH_LENIENT_FALSE_PATTERN.matcher(query).find() || QSTR_LENIENT_FALSE_PATTERN.matcher(query).find();
    }

    private static final Pattern TS_OUTPUT_CHANGED_PATTERN = Pattern.compile(
        ".*Output has changed from \\[.*\\] to \\[.*\\].*",
        Pattern.DOTALL
    );

    // https://github.com/elastic/elasticsearch/issues/134794
    static boolean isTsOutputChangedError(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (TS_OUTPUT_CHANGED_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        String trimmed = query.trim().toUpperCase(Locale.ROOT);
        return trimmed.startsWith("TS ");
    }

    /**
     * Matches the FORK pipeline command (a {@code |} followed by the FORK keyword)
     */
    private static final Pattern FORK_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*FORK\\b");

    // https://github.com/elastic/elasticsearch/issues/147603
    static boolean isUnsupportedTypeAfterForkError(String errorMessage, String query) {
        if (query == null || UNSUPPORTED_TYPE_AFTER_FORK_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return FORK_COMMAND_PATTERN.matcher(query).find();
    }

    private static final Pattern OPTIMIZED_INCORRECTLY_ORDERBY_PATTERN = Pattern.compile(
        ".*Plan \\[OrderBy\\[.*optimized incorrectly due to missing references.*",
        Pattern.DOTALL
    );

    private static final Pattern COMPUTE_BLOCK_CLASS_CAST_PATTERN = Pattern.compile(
        ".*class org\\.elasticsearch\\.compute\\.data\\.\\w+Block cannot be cast"
            + " to class org\\.elasticsearch\\.compute\\.data\\.\\w+Block.*",
        Pattern.DOTALL
    );

    private static final Pattern FORK_WITH_SORT_BRANCH_PATTERN = Pattern.compile(
        "(?is)\\bFORK\\b.*\\bSORT\\b.*\\|\\s*WHERE\\s+_fork\\s*=="
    );

    /**
     * FORK with an in-branch SORT on a field that becomes unused after the FORK currently
     * triggers two distinct bugs depending on the surrounding pipeline:
     * <ul>
     *   <li>{@code Plan [OrderBy[...]] optimized incorrectly due to missing references [...]}
     *       — see <a href="https://github.com/elastic/elasticsearch/issues/148382">#148382</a></li>
     *   <li>{@code ClassCastException} between compute {@code Block} subclasses (e.g.
     *       {@code BytesRefArrayBlock} cast to {@code IntBlock}) downstream of the FORK
     *       — see <a href="https://github.com/elastic/elasticsearch/issues/148386">#148386</a></li>
     * </ul>
     */
    static boolean isForkWithSortBranchBug(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (OPTIMIZED_INCORRECTLY_ORDERBY_PATTERN.matcher(errorMessage).matches() == false
            && COMPUTE_BLOCK_CLASS_CAST_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return FORK_WITH_SORT_BRANCH_PATTERN.matcher(query).find();
    }

    private static final Pattern ARRAY_INDEX_OUT_OF_BOUNDS_PATTERN = Pattern.compile(
        ".*array_index_out_of_bounds_exception.*Index \\d+ out of bounds for length \\d+.*",
        Pattern.DOTALL
    );

    /**
     * See https://github.com/elastic/elasticsearch/issues/148475
     */
    static boolean isForkTopNIndexOutOfBoundsBug(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (ARRAY_INDEX_OUT_OF_BOUNDS_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return FORK_COMMAND_PATTERN.matcher(query).find();
    }

    private static final Pattern OPTIMIZED_INCORRECTLY_PATTERN = Pattern.compile(
        ".*optimized incorrectly due to missing references.*",
        Pattern.DOTALL
    );

    /**
     * See https://github.com/elastic/elasticsearch/issues/138231
     */
    static boolean isForkOptimizedIncorrectlyBug(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (OPTIMIZED_INCORRECTLY_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return FORK_COMMAND_PATTERN.matcher(query).find();
    }

    private static final Pattern RENAME_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*RENAME\\b");
    private static final Pattern MV_EXPAND_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*MV_EXPAND\\b");
    private static final Pattern FUNCTION_GENERATING_COMMAND_PATTERN = Pattern.compile(
        "(?i)\\|\\s*(?:REGISTERED_DOMAIN|URI_PARTS|USER_AGENT)\\b"
    );

    /**
     * See https://github.com/elastic/elasticsearch/issues/148500
     */
    static boolean isRenameMvExpandOrderByBug(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (OPTIMIZED_INCORRECTLY_ORDERBY_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return RENAME_COMMAND_PATTERN.matcher(query).find()
            && MV_EXPAND_COMMAND_PATTERN.matcher(query).find()
            && FUNCTION_GENERATING_COMMAND_PATTERN.matcher(query).find();
    }

    private static final Pattern OPTIMIZED_INCORRECTLY_LIMITBY_PATTERN = Pattern.compile(
        ".*Plan \\[(?:LimitBy|TopNBy)\\[.*optimized incorrectly due to missing references.*",
        Pattern.DOTALL
    );

    private static final Pattern LIMIT_BY_COMMAND_PATTERN = Pattern.compile("(?i)\\|\\s*LIMIT\\s+\\S+\\s+BY\\b");

    /**
     * See https://github.com/elastic/elasticsearch/issues/148513
     * <p>
     * The same root cause manifests as either {@code LimitBy[...]} (no upstream SORT) or {@code TopNBy[...]}
     * (when an upstream SORT gets combined with the LIMIT BY into a TopNBy).
     */
    static boolean isLimitByMvExpandBug(String errorMessage, String query) {
        if (errorMessage == null || query == null) {
            return false;
        }
        if (OPTIMIZED_INCORRECTLY_LIMITBY_PATTERN.matcher(errorMessage).matches() == false) {
            return false;
        }
        return MV_EXPAND_COMMAND_PATTERN.matcher(query).find() && LIMIT_BY_COMMAND_PATTERN.matcher(query).find();
    }

    @Override
    @SuppressWarnings("unchecked")
    public QueryExecuted execute(String query, int depth) {
        QueryExecuted result;
        try {
            Map<String, Object> json = RestEsqlTestCase.runEsql(
                new RestEsqlTestCase.RequestObjectBuilder().query(query).build(),
                new AssertWarnings.AllowedRegexes(List.of(Pattern.compile(".*"))),// we don't care about warnings
                profileLogger,
                RestEsqlTestCase.Mode.SYNC
            );
            List<Column> outputSchema = outputSchema(json);
            List<List<Object>> values = (List<List<Object>>) json.get("values");
            result = new QueryExecuted(query, depth, outputSchema, values, null);
        } catch (Exception e) {
            result = new QueryExecuted(query, depth, null, null, e);
        } catch (AssertionError ae) {
            // this is for ensureNoWarnings()
            result = new QueryExecuted(query, depth, null, null, new RuntimeException(ae.getMessage()));
        }
        // Strip the artificial query approximation columns that are added after query execution and trail all
        // other columns. These columns confuse follow-up command generation (that try to reference them) and
        // result validation (that expect columns added after them).
        if (result.query() != null && FromGenerator.hasApproximationSettings(result.query())) {
            result = stripApproximationColumns(result);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<Column> outputSchema(Map<String, Object> a) {
        List<Map<String, ?>> cols = (List<Map<String, ?>>) a.get("columns");
        if (cols == null) {
            return null;
        }
        return cols.stream().map(x -> new Column((String) x.get(COLUMN_NAME), (String) x.get(COLUMN_TYPE), originalTypes(x))).toList();
    }

    @SuppressWarnings("unchecked")
    private static List<String> originalTypes(Map<String, ?> x) {
        List<String> originalTypes = (List<String>) x.get(COLUMN_ORIGINAL_TYPES);
        if (originalTypes == null) {
            return List.of();
        }
        return originalTypes;
    }

    private List<String> availableIndices() throws IOException {
        return availableDatasetsForEs(true, supportsSourceFieldMapping(), false, requiresTimeSeries(), cap -> false).stream()
            .filter(x -> x.inferenceEndpoints().isEmpty())
            .map(x -> x.indexName())
            .toList();
    }

    private List<LookupIdx> lookupIndices() {
        List<LookupIdx> result = new ArrayList<>();
        // we don't have key info from the dataset loader, let's hardcode it for now
        result.add(new LookupIdx("languages_lookup", List.of(new LookupIdxColumn("language_code", "integer"))));
        result.add(new LookupIdx("message_types_lookup", List.of(new LookupIdxColumn("message", "keyword"))));
        List<LookupIdxColumn> multiColumnJoinableLookupKeys = List.of(
            new LookupIdxColumn("id_int", "integer"),
            new LookupIdxColumn("name_str", "keyword"),
            new LookupIdxColumn("is_active_bool", "boolean"),
            new LookupIdxColumn("ip_addr", "ip"),
            new LookupIdxColumn("other1", "keyword"),
            new LookupIdxColumn("other2", "integer")
        );
        result.add(new LookupIdx("multi_column_joinable_lookup", multiColumnJoinableLookupKeys));
        return result;
    }

    private interface AllowedFailureRule {
        boolean matches(FailureContext ctx);
    }
}

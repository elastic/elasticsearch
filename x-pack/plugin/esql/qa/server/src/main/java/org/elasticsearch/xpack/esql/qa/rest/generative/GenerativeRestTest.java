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
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.LookupIdx;
import org.elasticsearch.xpack.esql.generator.LookupIdxColumn;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;
import org.elasticsearch.xpack.esql.qa.rest.ProfileLogger;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.UNMAPPED_FIELD_NAMES;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.SET_UNMAPPED_FIELDS_PREFIX;
import static org.elasticsearch.xpack.esql.generator.command.source.FromGenerator.isFromSource;

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
        "The field names are too complex to process", // field_caps problem
        "must be \\[any type except counter types\\]", // TODO refine the generation of count()
        "INLINE STATS cannot be used after an explicit or implicit LIMIT command",
        "sub-plan execution results too large",  // INLINE STATS limitations

        // Awaiting fixes for query failure
        "Unknown column \\[<all-fields-projected>\\]", // https://github.com/elastic/elasticsearch/issues/121741,
        // https://github.com/elastic/elasticsearch/issues/125866
        "Plan \\[ProjectExec\\[\\[<no-fields>.* optimized incorrectly due to missing references",
        "The incoming YAML document exceeds the limit:", // still to investigate, but it seems to be specific to the test framework
        "Data too large", // Circuit breaker exceptions eg. https://github.com/elastic/elasticsearch/issues/130072
        "long overflow", // https://github.com/elastic/elasticsearch/issues/135759
        "can't find input for", // https://github.com/elastic/elasticsearch/issues/136596
        "optimized incorrectly due to missing references", // https://github.com/elastic/elasticsearch/issues/138231
        "'field' must not be null in clamp\\(\\)", // clamp/clamp_min/clamp_max reject NULL field from unmapped fields
        "must be \\[boolean, date, ip, string or numeric except unsigned_long or counter types\\]", // type mismatch in top() arguments
        "unsupported logical plan node \\[Join\\]", // https://github.com/elastic/elasticsearch/issues/141978
        "Unsupported right plan for lookup join \\[Eval\\]", // https://github.com/elastic/elasticsearch/issues/141870
        "Does not support yet aggregations over constants", // https://github.com/elastic/elasticsearch/issues/118292
        "illegal data type \\[datetime\\]", // https://github.com/elastic/elasticsearch/issues/142137
        "Expected to replace a single StubRelation in the plan, but none found", // https://github.com/elastic/elasticsearch/issues/142219
        "blocks is empty", // https://github.com/elastic/elasticsearch/issues/142473
        "Overflow to represent absolute value of .*.MIN_VALUE", // https://github.com/elastic/elasticsearch/issues/142642
        "found value \\[.*\\] type \\[unsupported\\]", // https://github.com/elastic/elasticsearch/issues/142761
        "illegal query_string option \\[boost\\]", // https://github.com/elastic/elasticsearch/issues/142758
        "change point value \\[.*\\] must be numeric", // https://github.com/elastic/elasticsearch/issues/142858
        // https://github.com/elastic/elasticsearch/issues/142860
        "(Grok|Dissect) only supports KEYWORD or TEXT values, found expression \\[.*\\] type \\[NULL\\]",
        // https://github.com/elastic/elasticsearch/issues/142543
        "Column \\[.*\\] has conflicting data types in FORK branches: \\[NULL\\] and \\[.*\\]",
        "Column \\[.*\\] has conflicting data types in FORK branches: \\[.*\\] and \\[NULL\\]",
        "illegal match option \\[zero_terms_query\\]", // https://github.com/elastic/elasticsearch/issues/143070
        "Field \\[.*\\] of type \\[.*\\] does not support match.* queries",
        "Input for URI_PARTS must be of type [string] but is [null]", // https://github.com/elastic/elasticsearch/issues/143145
        "JOIN left field \\[.*\\] of type \\[NULL\\] is incompatible with right", // https://github.com/elastic/elasticsearch/issues/141827
        // https://github.com/elastic/elasticsearch/issues/141827
        "JOIN left field \\[.*\\] of type \\[.*\\] is incompatible with right field \\[.*\\] of type \\[NULL\\]",

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
            + "use the original @timestamp field instead.", // https://github.com/elastic/elasticsearch/issues/140607

        // Ts-command errors awaiting fixes
        "Output has changed from \\[.*\\] to \\[.*\\]" // https://github.com/elastic/elasticsearch/issues/134794
    );

    public static final Set<Pattern> ALLOWED_ERROR_PATTERNS = ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

    private static final Pattern FULL_TEXT_AFTER_SORT_PATTERN = Pattern.compile(
        ".*\\[(KQL|QSTR)] function cannot be used after SORT.*",
        Pattern.DOTALL
    );
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
     * Matches "first argument of [X] is [null] so second argument must also be [null] but was [Y]" errors.
     * This happens when an unmapped field (which resolves to DataType.NULL) is used in a binary operation
     * with a non-null typed field. See https://github.com/elastic/elasticsearch/issues/142115
     */
    private static final Pattern NULL_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*first argument of \\[([^]]+)] is \\[null] so second argument must also be \\[null] but was \\[.*].*",
        Pattern.DOTALL
    );
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
    /**
     * Matches FIRST(...) or LAST(...) function calls where the second argument is the literal {@code null}.
     * See https://github.com/elastic/elasticsearch/issues/142180#issuecomment-3913054718
     */
    private static final Pattern FIRST_LAST_NULL_ARG_PATTERN = Pattern.compile("(?i)\\b(?:first|last)\\s*\\(.+?,\\s*null\\s*\\)");
    /**
     * Matches FIRST(...) or LAST(...) function calls and captures both arguments.
     * Used to detect when the same field is passed as both the search and sort parameters.
     * See https://github.com/elastic/elasticsearch/issues/142180
     */
    private static final Pattern FIRST_LAST_CALL_PATTERN = Pattern.compile(
        "(?i)\\b(?:first|last)\\s*\\(\\s*([^,()]+?)\\s*,\\s*([^,()]+?)\\s*\\)"
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

                    final QueryExecuted result = previousResult == null
                        ? execute(command, 0)
                        : execute(previousResult.query() + command, previousResult.depth());

                    final boolean hasException = result.exception() != null;
                    if (hasException || checkResults(previousCommands, generator, current, previousResult, result).success() == false) {
                        if (hasException) {
                            List<CommandGenerator.CommandDescription> commands = new ArrayList<>(previousCommands.size() + 1);
                            commands.addAll(previousCommands);
                            commands.add(current);
                            checkException(result, commands);
                        }
                        continueExecuting = false;
                        currentSchema = List.of();
                    } else {
                        continueExecuting = true;
                        currentSchema = result.outputSchema();
                    }
                    if (previousCommands.isEmpty() && continueExecuting && isFromSource(current)) {
                        current.context()
                            .put(
                                FromGenerator.INDEX_FIELD_NAMES,
                                currentSchema.stream().map(Column::name).collect(java.util.stream.Collectors.toSet())
                            );
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
                boolean knownError = false;
                for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
                    if (isAllowedError(e.getMessage(), allowedError)) {
                        knownError = true;
                        break;
                    }
                }
                if (knownError == false) {
                    StringBuilder message = new StringBuilder();
                    message.append("Generative tests, error generating new command \n");
                    message.append("Previous query: \n");
                    message.append(exec.previousResult.query());
                    fail(e, message.toString());
                }
            }
        }
    }

    protected CommandGenerator sourceCommand() {
        return EsqlQueryGenerator.sourceCommand();
    }

    private record FailureContext(String errorMessage, String query, List<CommandGenerator.CommandDescription> previousCommands) {
        FailureContext {
            previousCommands = previousCommands == null ? List.of() : previousCommands;
        }
    }

    private static final AllowedFailureRule[] ALLOWED_FAILURE_RULES = { ctx -> {
        for (Pattern allowedError : ALLOWED_ERROR_PATTERNS) {
            if (isAllowedError(ctx.errorMessage, allowedError)) {
                return true;
            }
        }
        return false;
    },
        ctx -> isUnmappedFieldError(ctx.errorMessage, ctx.query),
        ctx -> isScalarTypeMismatchError(ctx.errorMessage),
        ctx -> isFirstLastSameFieldError(ctx.errorMessage, ctx.query),
        ctx -> isForkOptimizationBugWithUnmappedFields(ctx.errorMessage, ctx.query),
        ctx -> isFieldFullTextError(ctx.errorMessage, ctx.query, ctx.previousCommands),
        ctx -> isFullTextAfterSampleBug(ctx.errorMessage, ctx.query),
        ctx -> isFullTextAfterWhereBugs(ctx.errorMessage),
        ctx -> isLenientFalseFailedToCreateFullTextQueryError(ctx.errorMessage, ctx.query), };

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
        QueryExecuted result
    ) {
        CommandGenerator.ValidationResult outputValidation = commandGenerator.validateOutput(
            previousCommands,
            commandDescription,
            previousResult == null ? null : previousResult.outputSchema(),
            previousResult == null ? null : previousResult.result(),
            result.outputSchema(),
            result.result()
        );
        if (outputValidation.success() == false) {
            if (isAllowedFailure(new FailureContext(outputValidation.errorMessage(), result.query(), previousCommands))) {
                return outputValidation;
            }
            fail("query: " + result.query() + "\nerror: " + outputValidation.errorMessage());
        }
        return outputValidation;
    }

    protected void checkException(QueryExecuted query, List<CommandGenerator.CommandDescription> previousCommands) {
        if (isAllowedFailure(new FailureContext(query.exception().getMessage(), query.query(), previousCommands))) {
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

    private static boolean isAllowedError(String errorMessage, Pattern allowedPattern) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        return allowedPattern.matcher(errorWithoutLineBreaks).matches();
    }

    /**
     * Checks if the error is a known unmapped field error. This covers:
     * <ul>
     *   <li>"[KQL|QSTR] function cannot be used after SORT" (https://github.com/elastic/elasticsearch/issues/142959)</li>
     *   <li>"Rule execution limit [100] reached" - can happen with complex plans involving "nullify" unmapped fields
     *       (https://github.com/elastic/elasticsearch/issues/142390)</li>
     *   <li>"Unknown column [X], did you mean [Y]?" - both X and Y must be unmapped field names</li>
     *   <li>"Unknown column [X]" (no suggestion) - X must be an unmapped field name</li>
     *   <li>"first argument of [X] is [null] so second argument must also be [null] but was [Y]" -
     *       the expression X must contain an unmapped field name (https://github.com/elastic/elasticsearch/issues/142115)</li>
     *
     * </ul>
     */
    private static boolean isUnmappedFieldError(String errorMessage, String query) {
        if (query.startsWith(SET_UNMAPPED_FIELDS_PREFIX) == false) {
            return false;
        }
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        if (errorWithoutLineBreaks.contains("Rule execution limit [100] reached")) {
            return true;
        }

        Matcher matcher = FULL_TEXT_AFTER_SORT_PATTERN.matcher(errorWithoutLineBreaks);
        if (matcher.matches()) {
            return true;
        }

        matcher = UNKNOWN_COLUMN_WITH_SUGGESTION_PATTERN.matcher(errorWithoutLineBreaks);
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

        matcher = NULL_TYPE_MISMATCH_PATTERN.matcher(errorWithoutLineBreaks);
        if (matcher.matches()) {
            String expression = matcher.group(1);
            return UNMAPPED_NAMES.stream().anyMatch(expression::contains);
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
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        return SCALAR_TYPE_MISMATCH_PATTERN.matcher(errorWithoutLineBreaks).matches();
    }

    /**
     * Checks if the error is an {@code ArrayIndexOutOfBoundsException} caused by calling FIRST or LAST with problematic arguments.
     * See https://github.com/elastic/elasticsearch/issues/142180
     */
    private static boolean isFirstLastSameFieldError(String errorMessage, String query) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        if (errorWithoutLineBreaks.contains("out of bounds for length") == false) {
            return false;
        }
        if (FIRST_LAST_NULL_ARG_PATTERN.matcher(query).find()) {
            return true;
        }
        Matcher matcher = FIRST_LAST_CALL_PATTERN.matcher(query);
        while (matcher.find()) {
            if (matcher.group(1).equals(matcher.group(2))) {
                return true;
            }
        }
        return false;
    }

    private static final Pattern FORK_OPTIMIZED_INCORRECTLY_PATTERN = Pattern.compile(
        ".*Plan \\[.*\\] optimized incorrectly due to missing references \\[_fork.*",
        Pattern.DOTALL
    );

    /**
     * When {@code SET unmapped_fields="nullify"} is used, the _fork reference can go missing during plan optimization.
     * https://github.com/elastic/elasticsearch/issues/142762
     */
    static boolean isForkOptimizationBugWithUnmappedFields(String errorMessage, String query) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        return query.startsWith(SET_UNMAPPED_FIELDS_PREFIX) && FORK_OPTIMIZED_INCORRECTLY_PATTERN.matcher(errorWithoutLineBreaks).matches();
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

    private static final Pattern MV_EXPAND_FIELD_PATTERN = Pattern.compile("(?i)\\|\\s*mv_expand\\s+`?([^`|\\s]+)`?");

    private static final Pattern RENAME_NEW_FIELD_PATTERN = Pattern.compile("(?i)\\bas\\s+(`[^`]+`|[^,|\\s]+)");

    /**
     * Checks if the error is a full-text function/operator rejecting a field that is not a FieldAttribute from an index mapping. It covers:
     * <ul>
     *   <li>Fields added by an ENRICH command (enrich fields)</li>
     *   <li>Fields expanded by MV_EXPAND (the expanded fields)</li>
     *   <li>Fields created by GROK or DISSECT (the "extracted" fields)</li>
     *   <li>Fields renamed via RENAME</li>
     * </ul>
     * The error is allowed only when the offending field can be traced back to one of these commands.
     */
    static boolean isFieldFullTextError(String errorMessage, String query, List<CommandGenerator.CommandDescription> previousCommands) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        Matcher m = NOT_A_FIELD_FROM_INDEX_PATTERN.matcher(errorWithoutLineBreaks);
        if (m.matches() == false) {
            return false;
        }
        String lowerQuery = query.toLowerCase(java.util.Locale.ROOT);
        if (lowerQuery.contains("| enrich ") || lowerQuery.startsWith("enrich ")) {
            return true;
        }
        // see https://github.com/elastic/elasticsearch/issues/142713
        String fieldName = EsqlQueryGenerator.unquote(m.group(1));
        Matcher mvMatcher = MV_EXPAND_FIELD_PATTERN.matcher(query);
        while (mvMatcher.find()) {
            if (EsqlQueryGenerator.unquote(mvMatcher.group(1)).equals(fieldName)) {
                return true;
            }
        }
        for (var previous : previousCommands) {
            String name = previous.commandName();
            if (name == null) {
                continue;
            }
            name = name.toLowerCase(java.util.Locale.ROOT);
            if ("grok".equals(name)) {
                Matcher gm = GROK_GENERATED_FIELD_PATTERN.matcher(previous.commandString());
                while (gm.find()) {
                    if (EsqlQueryGenerator.unquote(gm.group(1)).equals(fieldName)) {
                        return true;
                    }
                }
            } else if ("dissect".equals(name)) {
                Matcher dm = DISSECT_GENERATED_FIELD_PATTERN.matcher(previous.commandString());
                while (dm.find()) {
                    String generated = dm.group(1);
                    if (generated.startsWith("?")) {
                        continue;
                    }
                    if (EsqlQueryGenerator.unquote(generated).equals(fieldName)) {
                        return true;
                    }
                }
            } else if ("rename".equals(name)) {
                Matcher rm = RENAME_NEW_FIELD_PATTERN.matcher(previous.commandString());
                while (rm.find()) {
                    if (EsqlQueryGenerator.unquote(rm.group(1).trim()).equals(fieldName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * SAMPLE should not block QSTR/KQL when it appears after the WHERE containing them, but currently it does.
     * See https://github.com/elastic/elasticsearch/issues/142694
     */
    static boolean isFullTextAfterSampleBug(String errorMessage, String query) {
        return FULL_TEXT_AFTER_SAMPLE_PATTERN.matcher(normalizeErrorMessage(errorMessage)).matches()
            && query.toLowerCase(java.util.Locale.ROOT).contains("| sample");
    }

    private static final Pattern FULL_TEXT_AFTER_SAMPLE_PATTERN = Pattern.compile(
        ".*\\[(KQL|QSTR)] function cannot be used after SAMPLE.*",
        Pattern.DOTALL
    );

    /**
     * See https://github.com/elastic/elasticsearch/issues/142705
     * See https://github.com/elastic/elasticsearch/issues/142710
     */
    static boolean isFullTextAfterWhereBugs(String errorMessage) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        return FULL_TEXT_AFTER_WHERE_PATTERN.matcher(errorWithoutLineBreaks).matches();
    }

    private static final Pattern FULL_TEXT_AFTER_WHERE_PATTERN = Pattern.compile(
        ".*(?:(?:\\[(?:KQL|QSTR|MATCH|MultiMatch)] function)|(?:\\[:\\] operator)) cannot be used after \\(?WHERE.*",
        Pattern.DOTALL
    );

    /**
     * Work around a query-building failure in full-text functions when options include {@code {"lenient": false}}.
     */
    static boolean isLenientFalseFailedToCreateFullTextQueryError(String errorMessage, String query) {
        String errorWithoutLineBreaks = normalizeErrorMessage(errorMessage);
        if (errorWithoutLineBreaks.contains("failed to create query: For input string") == false) {
            return false;
        }
        return MULTI_MATCH_LENIENT_FALSE_PATTERN.matcher(query).find()
            || MATCH_LENIENT_FALSE_PATTERN.matcher(query).find()
            || QSTR_LENIENT_FALSE_PATTERN.matcher(query).find();
    }

    private static final Pattern MULTI_MATCH_LENIENT_FALSE_PATTERN = Pattern.compile(
        "(?i)\\bmulti_match\\s*\\([^)]*\\{[^}]*[\"']lenient[\"']\\s*:\\s*false[^}]*}[^)]*\\)"
    );
    private static final Pattern MATCH_LENIENT_FALSE_PATTERN = Pattern.compile(
        "(?i)\\bmatch\\s*\\([^)]*\\{[^}]*[\"']lenient[\"']\\s*:\\s*false[^}]*}[^)]*\\)"
    );
    private static final Pattern QSTR_LENIENT_FALSE_PATTERN = Pattern.compile(
        "(?i)\\bqstr\\s*\\([^)]*\\{[^}]*[\"']lenient[\"']\\s*:\\s*false[^}]*}[^)]*\\)"
    );

    @Override
    @SuppressWarnings("unchecked")
    public QueryExecuted execute(String query, int depth) {
        try {
            Map<String, Object> json = RestEsqlTestCase.runEsql(
                new RestEsqlTestCase.RequestObjectBuilder().query(query).build(),
                new AssertWarnings.AllowedRegexes(List.of(Pattern.compile(".*"))),// we don't care about warnings
                profileLogger,
                RestEsqlTestCase.Mode.SYNC
            );
            List<Column> outputSchema = outputSchema(json);
            List<List<Object>> values = (List<List<Object>>) json.get("values");
            return new QueryExecuted(query, depth, outputSchema, values, null);
        } catch (Exception e) {
            return new QueryExecuted(query, depth, null, null, e);
        } catch (AssertionError ae) {
            // this is for ensureNoWarnings()
            return new QueryExecuted(query, depth, null, null, new RuntimeException(ae.getMessage()));
        }

    }

    @SuppressWarnings("unchecked")
    private static List<Column> outputSchema(Map<String, Object> a) {
        List<Map<String, ?>> cols = (List<Map<String, ?>>) a.get("columns");
        if (cols == null) {
            return null;
        }
        return cols.stream()
            .map(x -> new Column((String) x.get(COLUMN_NAME), (String) x.get(COLUMN_TYPE), originalTypes(x)))
            .collect(Collectors.toList());
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

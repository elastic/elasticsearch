/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.QueryExecuted;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromLoadGenerator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generative REST tests with {@code SET unmapped_fields="load"} on the source command. Concrete IT classes
 * extend this base and supply cluster wiring like {@link GenerativeRestTest}.
 */
public abstract class GenerativeUnmappedLoadRestTest extends GenerativeRestTest {

    public static final Set<String> LOAD_ALLOWED_ERRORS = Set.of(
        "missing references \\[.*\\]",
        "column \\[.*\\] already resolved",
        "cannot use \\[SET unmapped_fields\\] with FORK",
        "Unknown column \\[.*\\]",
        "first argument of \\[.*\\] is \\[null\\]",
        "type \\[null\\] .* not supported",
        "Rule execution limit \\[100\\] reached",
        "requires the \\[@timestamp\\] field"
    );

    public static final Set<Pattern> LOAD_ALLOWED_ERROR_PATTERNS;

    static {
        Set<Pattern> patterns = new HashSet<>();
        for (String x : LOAD_ALLOWED_ERRORS) {
            patterns.add(Pattern.compile(".*" + x + ".*", Pattern.DOTALL));
        }
        LOAD_ALLOWED_ERROR_PATTERNS = Set.copyOf(patterns);
    }

    private static final Pattern FULL_TEXT_AFTER_SORT_PATTERN = Pattern.compile(
        ".*\\[(KQL|QSTR)] function cannot be used after SORT.*",
        Pattern.DOTALL
    );

    private static final Pattern UNKNOWN_COLUMN_WITH_SUGGESTION_PATTERN = Pattern.compile(
        ".*Unknown column \\[([^]]+)], did you mean \\[([^]]+)]\\?.*",
        Pattern.DOTALL
    );

    private static final Pattern UNKNOWN_COLUMN_PATTERN = Pattern.compile(".*Unknown column \\[([^]]+)].*", Pattern.DOTALL);

    private static final Pattern NULL_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*first argument of \\[([^]]+)] is \\[null] so second argument must also be \\[null] but was \\[.*].*",
        Pattern.DOTALL
    );

    private static final Pattern ANY_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*argument of \\[([^]]+)] must be \\[.*], found value \\[([^]]+)] type \\[.*].*",
        Pattern.DOTALL
    );

    private static final Set<String> UNMAPPED_NAMES = Set.of(KeepGenerator.UNMAPPED_FIELD_NAMES);

    @Override
    protected CommandGenerator sourceCommand() {
        return FromLoadGenerator.INSTANCE;
    }

    @Override
    protected void checkPipelineException(
        QueryExecuted query,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        String errorMessage = query.exception().getMessage();
        for (Pattern p : LOAD_ALLOWED_ERROR_PATTERNS) {
            if (isAllowedError(errorMessage, p)) {
                return;
            }
        }
        String normalized = normalizeErrorMessage(errorMessage);
        if (query.query().startsWith(FromLoadGenerator.SET_LOAD_PREFIX) && isUnmappedFieldLoadModeError(normalized, query.query())) {
            return;
        }
        super.checkPipelineException(query, previousCommands, currentSchema);
    }

    @Override
    protected CommandGenerator.ValidationResult checkPipelineResults(
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
        if (outputValidation.success()) {
            return outputValidation;
        }
        String errorMsg = outputValidation.errorMessage();
        for (Pattern p : LOAD_ALLOWED_ERROR_PATTERNS) {
            if (isAllowedError(errorMsg, p)) {
                return outputValidation;
            }
        }
        String normalized = normalizeErrorMessage(errorMsg);
        if (result.query().startsWith(FromLoadGenerator.SET_LOAD_PREFIX) && isUnmappedFieldLoadModeError(normalized, result.query())) {
            return outputValidation;
        }
        return GenerativeRestTest.checkResults(
            previousCommands,
            commandGenerator,
            commandDescription,
            previousResult,
            result,
            currentSchema
        );
    }

    private static boolean isUnmappedFieldLoadModeError(String errorMessage, String query) {
        if (query.startsWith(FromLoadGenerator.SET_LOAD_PREFIX) == false) {
            return false;
        }
        if (errorMessage.contains("Rule execution limit [100] reached")) {
            return true;
        }

        Matcher matcher = FULL_TEXT_AFTER_SORT_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            return true;
        }

        matcher = UNKNOWN_COLUMN_WITH_SUGGESTION_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            String unknownColumn = matcher.group(1);
            String suggestedColumn = matcher.group(2);
            return UNMAPPED_NAMES.contains(unknownColumn) && UNMAPPED_NAMES.contains(suggestedColumn);
        }

        matcher = UNKNOWN_COLUMN_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            String unknownColumn = matcher.group(1);
            return UNMAPPED_NAMES.contains(unknownColumn);
        }

        matcher = NULL_TYPE_MISMATCH_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            String expression = matcher.group(1);
            for (String name : UNMAPPED_NAMES) {
                if (expression.contains(name)) {
                    return true;
                }
            }
            return false;
        }

        matcher = ANY_TYPE_MISMATCH_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            String functionExpression = matcher.group(1);
            String foundValue = matcher.group(2);
            for (String name : UNMAPPED_NAMES) {
                if (functionExpression.contains(name) || foundValue.contains(name)) {
                    return true;
                }
            }
        }

        return false;
    }
}

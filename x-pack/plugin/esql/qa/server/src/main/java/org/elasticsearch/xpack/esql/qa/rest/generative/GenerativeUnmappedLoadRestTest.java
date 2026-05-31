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
import org.elasticsearch.xpack.esql.generator.command.source.FromLoadGenerator;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.generator.command.pipe.KeepGenerator.UNMAPPED_FIELD_NAMES;

/**
 * Generative REST tests that always prepend {@code SET unmapped_fields="load"} to queries.
 * Lives in a separate class hierarchy from {@link GenerativeRestTest} so that CI muting
 * of these tests does not affect the main generative test suite.
 */
public abstract class GenerativeUnmappedLoadRestTest extends GenerativeRestTest {

    private static final Set<String> UNMAPPED_NAMES = Set.of(UNMAPPED_FIELD_NAMES);

    /**
     * Matches "argument of [X] is [keyword] so ... argument must also be [keyword] but was [Y]" errors.
     * This happens when an unmapped field that is force-loaded as keyword is used in a binary operation
     * with a non-keyword typed value (e.g. {@code unmapped_field > 31}).
     */
    private static final Pattern KEYWORD_TYPE_MISMATCH_PATTERN = Pattern.compile(
        ".*argument of \\[([^]]+)] is \\[keyword] so .* argument must also be \\[keyword] but was \\[.*].*",
        Pattern.DOTALL
    );

    protected static final Set<String> LOAD_ALLOWED_ERRORS = Set.of(
        // https://github.com/elastic/elasticsearch/issues/141995, https://github.com/elastic/elasticsearch/issues/141990
        "missing references \\[.*\\]",
        // https://github.com/elastic/elasticsearch/issues/142026
        "column \\[.*\\] already resolved",
        // https://github.com/elastic/elasticsearch/issues/142033, https://github.com/elastic/elasticsearch/issues/142026
        "is not supported with unmapped_fields",
        "does not support full-text search function",
        "type \\[null\\] .* not supported",
        // https://github.com/elastic/elasticsearch/issues/145555
        "Multiple index patterns should be disabled with unmapped fields",
        // https://github.com/elastic/elasticsearch/issues/146036
        "argument of \\[.*\\] must be \\[unsupported\\], found value",
        "LOOKUP JOIN is not supported with unmapped_fields=\\\"load\\\"",
        // https://github.com/elastic/elasticsearch/issues/146074
        "Input for REGISTERED_DOMAIN must be of type \\[string\\] but is \\[unsupported]\\",
        "argument of \\[.*\\] is [keyword] so .* argument must also be [keyword] but was \\[.*\\]",
        "FORK is not supported with unmapped_fields=\\\"load\\\""
    );

    protected static final Set<Pattern> LOAD_ALLOWED_ERROR_PATTERNS = LOAD_ALLOWED_ERRORS.stream()
        .map(x -> ".*" + x + ".*")
        .map(x -> Pattern.compile(x, Pattern.DOTALL))
        .collect(Collectors.toSet());

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
        if (isUnmappedFieldPrefixError(errorMessage, query.query(), FromLoadGenerator.SET_LOAD_PREFIX)) {
            return;
        }
        if (isKeywordTypeMismatchForLoadedField(errorMessage)) {
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
        if (isUnmappedFieldPrefixError(errorMsg, result.query(), FromLoadGenerator.SET_LOAD_PREFIX)) {
            return outputValidation;
        }
        if (isKeywordTypeMismatchForLoadedField(errorMsg)) {
            return outputValidation;
        }
        failOnUnexpectedValidationError(outputValidation, result, previousCommands, currentSchema);
        return outputValidation;
    }

    private static boolean isKeywordTypeMismatchForLoadedField(String errorMessage) {
        Matcher matcher = KEYWORD_TYPE_MISMATCH_PATTERN.matcher(errorMessage);
        if (matcher.matches()) {
            String expression = matcher.group(1);
            return UNMAPPED_NAMES.stream().anyMatch(expression::contains);
        }
        return false;
    }
}

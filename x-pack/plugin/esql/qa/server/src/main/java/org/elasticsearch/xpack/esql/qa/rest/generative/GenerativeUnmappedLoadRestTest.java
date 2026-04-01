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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Generative REST tests that always prepend {@code SET unmapped_fields="load"} to queries.
 * Lives in a separate class hierarchy from {@link GenerativeRestTest} so that CI muting
 * of these tests does not affect the main generative test suite.
 *
 * <p>Load-specific allowed errors track known limitations:
 * <ul>
 *   <li>{@code missing references} — optimizer bugs with partial mappings
 *       (<a href="https://github.com/elastic/elasticsearch/issues/141995">#141995</a>,
 *        <a href="https://github.com/elastic/elasticsearch/issues/141990">#141990</a>)</li>
 *   <li>{@code column already resolved} — LOOKUP JOIN issues
 *       (<a href="https://github.com/elastic/elasticsearch/issues/142026">#142026</a>)</li>
 *   <li>{@code cannot use [SET unmapped_fields] with FORK} — branching commands
 *       (<a href="https://github.com/elastic/elasticsearch/issues/142033">#142033</a>)</li>
 *   <li>{@code type [null]} — null type propagation from loaded-as-null fields</li>
 * </ul>
 */
public abstract class GenerativeUnmappedLoadRestTest extends GenerativeRestTest {

    protected static final Set<String> LOAD_ALLOWED_ERRORS = Set.of(
        // https://github.com/elastic/elasticsearch/issues/141995, https://github.com/elastic/elasticsearch/issues/141990
        "missing references \\[.*\\]",
        // https://github.com/elastic/elasticsearch/issues/142026
        "column \\[.*\\] already resolved",
        // https://github.com/elastic/elasticsearch/issues/142033
        "cannot use \\[SET unmapped_fields\\] with FORK",
        "type \\[null\\] .* not supported"
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
        return super.checkPipelineResults(previousCommands, commandGenerator, commandDescription, previousResult, result, currentSchema);
    }
}

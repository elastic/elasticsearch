/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.GenerativeFeature;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.SubqueryGenerator;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.FunctionGenerator.shouldAddUnmappedFieldWithProbabilityIncrease;

public class FromGenerator implements CommandGenerator {

    public static final FromGenerator INSTANCE = new FromGenerator();

    /**
     * Context key used to indicate whether SET unmapped_fields="nullify" was included in the FROM command.
     * When true, unmapped field names can be used in downstream commands and functions.
     */
    public static final String UNMAPPED_FIELDS_ENABLED = "unmappedFieldsEnabled";

    public static final String HAS_SUBQUERY = "hasSubquery";

    public static final String SET_UNMAPPED_FIELDS_PREFIX = "SET unmapped_fields=\"nullify\";";

    public static final String SET_APPROXIMATION_PREFIX = "SET approximation=";

    protected static final double QUERY_APPROXIMATION_SETTING_PROBABILITY = 0.1;

    /**
     * Returns {@code true} if the given command is a FROM source command.
     * Used to gate full-text function generation which are only valid when the query originates from a FROM command (not TS or PROMQL).
     */
    public static boolean isFromSource(CommandDescription command) {
        return command != null && "from".equals(command.commandName());
    }

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        // SET prefixes are only legal at the top level of a query — never emit them inside a subquery.
        boolean useUnmappedFields = context.isWithinASubquery() == false && shouldAddUnmappedFieldWithProbabilityIncrease(3);
        StringBuilder result = new StringBuilder();
        if (useUnmappedFields) {
            result.append(SET_UNMAPPED_FIELDS_PREFIX);
        }
        boolean setQueryApproximation = context.isWithinASubquery() == false
            && EsqlCapabilities.Cap.APPROXIMATION_V7.isEnabled()
            && randomDouble() < QUERY_APPROXIMATION_SETTING_PROBABILITY;
        if (setQueryApproximation) {
            result.append(randomQueryApproximationSettings());
        }
        boolean hasSubquery = appendFromCommand(result, schema, executor, context);
        String query = result.toString();
        Map<String, Object> commandContext = new HashMap<>();
        commandContext.put(UNMAPPED_FIELDS_ENABLED, useUnmappedFields);
        commandContext.put(HAS_SUBQUERY, hasSubquery);
        return new CommandDescription("from", this, query, commandContext);
    }

    @Override
    public ValidationResult validateOutput(
        List<CommandDescription> previousCommands,
        CommandDescription commandDescription,
        List<Column> previousColumns,
        List<List<Object>> previousOutput,
        List<Column> columns,
        List<List<Object>> output
    ) {
        return VALIDATION_OK;
    }

    protected static final double SUBQUERY_PROBABILITY = 0.15;

    /**
     * Appends the {@code from <sources>} portion of the command and returns whether the resulting plan
     * tree contains a subquery. Callers should propagate this into the {@link CommandDescription} context
     * map under {@link #HAS_SUBQUERY} so downstream commands can react.
     */
    protected static boolean appendFromCommand(
        StringBuilder result,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        result.append("from ");
        int items = randomIntBetween(1, 3);
        List<String> availableIndices = schema.baseIndices();
        boolean hasSubquery = false;
        for (int i = 0; i < items; i++) {
            if (i > 0) {
                result.append(",");
            }
            // No nested subqueries: ESQL rejects UnionAll under UnionAll ("Nested subqueries are not supported").
            // Subqueries are opt-in via GenerativeFeature.SUBQUERIES so the main generative suite can remain
            // subquery-free; only GenerativeSubqueryRestTest enables them.
            if (context.isFeatureEnabled(GenerativeFeature.SUBQUERIES)
                && context.isWithinASubquery() == false
                && randomDouble() < SUBQUERY_PROBABILITY) {
                SubqueryGenerator.SubqueryResult sub = SubqueryGenerator.build(context, schema, executor);
                if (sub != null) {
                    result.append(sub.queryText());
                    hasSubquery = true;
                    continue;
                }
                // Fall through to a plain index pattern if subquery generation failed.
            }
            String pattern = EsqlQueryGenerator.indexPattern(availableIndices.get(randomIntBetween(0, availableIndices.size() - 1)));
            result.append(pattern);
        }
        return hasSubquery;
    }

    protected String randomQueryApproximationSettings() {
        StringBuilder settings = new StringBuilder();
        settings.append(SET_APPROXIMATION_PREFIX);
        double x = randomDouble();
        if (x < 0.1) {
            settings.append("null");
        } else if (x < 0.2) {
            settings.append("false");
        } else if (x < 0.3) {
            settings.append("true");
        } else {
            settings.append("{");
            boolean needsSeparator = false;
            if (randomBoolean()) {
                settings.append("\"rows\":");
                settings.append(randomIntBetween(10000, 100000));
                needsSeparator = true;
            }
            if (randomBoolean()) {
                if (needsSeparator) {
                    settings.append(",");
                }
                settings.append("\"confidence_level\":");
                settings.append(randomDoubleBetween(0.5, 0.95, true));
            }
            settings.append("}");
        }
        settings.append(";");
        return settings.toString();
    }

    public static boolean hasApproximationSettings(String query) {
        return query.contains(SET_APPROXIMATION_PREFIX);
    }
}

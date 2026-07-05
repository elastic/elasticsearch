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

    /**
     * Set to {@code true} when the FROM produces a {@code UnionAll}, either from an embedded subquery
     * or from a wildcard that matches both a view and regular indices. FORK must not be generated when
     * this flag is set.
     */
    public static final String HAS_UNION_ALL = "hasUnionAll";

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
        // SET prefixes are only legal at the top level of a query, never inside a subquery.
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
        Map<String, Object> commandContext = new HashMap<>();
        commandContext.put(UNMAPPED_FIELDS_ENABLED, useUnmappedFields);
        appendFromCommand(result, schema, executor, context, commandContext);
        String query = result.toString();
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
     * Appends the {@code FROM <sources>} portion of the command and sets {@link #HAS_UNION_ALL} in
     * {@code commandContext}. Subqueries and mixed-view wildcards are kept mutually exclusive, because
     * nesting them triggers a planner error (https://github.com/elastic/elasticsearch/issues/149396).
     */
    protected static void appendFromCommand(
        StringBuilder result,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context,
        Map<String, Object> commandContext
    ) {
        result.append("from ");
        int items = randomIntBetween(1, 3);
        List<String> availableIndices = schema.baseIndices();
        boolean canHaveSubquery = context.isFeatureEnabled(GenerativeFeature.SUBQUERIES) && context.isWithinASubquery() == false;
        boolean hasSubquery = false;
        boolean hasViewInFrom = false;
        for (int i = 0; i < items; i++) {
            if (i > 0) {
                result.append(",");
            }
            if (canHaveSubquery && hasViewInFrom == false && randomDouble() < SUBQUERY_PROBABILITY) {
                result.append(SubqueryGenerator.build(context, schema, executor).queryText());
                hasSubquery = true;
            } else {
                String idxName = availableIndices.get(randomIntBetween(0, availableIndices.size() - 1));
                String pattern = EsqlQueryGenerator.indexPattern(idxName);
                if (pattern.endsWith("*")) {
                    String prefix = pattern.substring(0, pattern.length() - 1);
                    // A wildcard hitting both a view and regular indices creates a ViewUnionAll nested
                    // inside any outer UnionAll, which the planner rejects (see issue #149396).
                    boolean hitsView = schema.viewNames().stream().anyMatch(v -> v.startsWith(prefix) && v.equals(idxName) == false);
                    if (hitsView) {
                        if (hasSubquery) {
                            pattern = idxName;
                        } else {
                            hasViewInFrom = true;
                        }
                    }
                }
                result.append(pattern);
            }
        }
        commandContext.put(HAS_UNION_ALL, hasSubquery || hasViewInFrom);
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

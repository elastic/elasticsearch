/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class FromLoadGenerator extends FromGenerator {

    public static final FromLoadGenerator INSTANCE = new FromLoadGenerator();

    public static final String SET_LOAD_PREFIX = "SET unmapped_fields=\"load\";";

    private static final double QUERY_APPROXIMATION_SETTING_PROBABILITY = 0.1;

    private FromLoadGenerator() {}

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        StringBuilder result = new StringBuilder();
        result.append(SET_LOAD_PREFIX);
        boolean setQueryApproximation = randomDouble() < QUERY_APPROXIMATION_SETTING_PROBABILITY;
        if (setQueryApproximation) {
            result.append(randomQueryApproximationSettings());
        }
        result.append("from ");
        int items = randomIntBetween(1, 3);
        List<String> availableIndices = schema.baseIndices();
        for (int i = 0; i < items; i++) {
            String pattern = EsqlQueryGenerator.indexPattern(availableIndices.get(randomIntBetween(0, availableIndices.size() - 1)));
            if (i > 0) {
                result.append(",");
            }
            result.append(pattern);
        }
        String query = result.toString();
        Map<String, Object> context = new HashMap<>();
        context.put(UNMAPPED_FIELDS_ENABLED, Boolean.TRUE);
        return new CommandDescription("from", this, query, context);
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

    private static String randomQueryApproximationSettings() {
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
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomDouble;

/**
 * Source command generator that always prepends {@code SET unmapped_fields="load";} to queries.
 */
public class FromLoadGenerator extends FromGenerator {

    public static final FromLoadGenerator INSTANCE = new FromLoadGenerator();

    public static final String SET_LOAD_PREFIX = "SET unmapped_fields=\"load\";";

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
        if (randomDouble() < QUERY_APPROXIMATION_SETTING_PROBABILITY) {
            result.append(randomQueryApproximationSettings());
        }
        appendFromClause(result, schema);
        Map<String, Object> context = new HashMap<>();
        context.put(UNMAPPED_FIELDS_ENABLED, Boolean.TRUE);
        return new CommandDescription("from", this, result.toString(), context);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.source;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.test.ESTestCase.randomDouble;

/**
 * Source command generator that prepends a {@code SET unmapped_fields="load"|"load_all";} prefix, forcing unmapped fields to load from
 * {@code _source}. {@code load_all} is snapshot-only, so {@link #LOAD_ALL_INSTANCE} degrades to a plain FROM when its capability is off.
 */
public class FromLoadGenerator extends FromGenerator {

    public static final String SET_LOAD_PREFIX = "SET unmapped_fields=\"load\";";
    public static final String SET_LOAD_ALL_PREFIX = "SET unmapped_fields=\"load_all\";";

    /** Both loading-mode prefixes, so the allowed-failure checks recognize either without drifting apart. */
    public static final Set<String> LOAD_PREFIXES = Set.of(SET_LOAD_PREFIX, SET_LOAD_ALL_PREFIX);

    public static final FromLoadGenerator INSTANCE = new FromLoadGenerator(SET_LOAD_PREFIX, () -> true);
    public static final FromLoadGenerator LOAD_ALL_INSTANCE = new FromLoadGenerator(
        SET_LOAD_ALL_PREFIX,
        EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL::isEnabled
    );

    private final String setPrefix;
    private final BooleanSupplier enabled;

    private FromLoadGenerator(String setPrefix, BooleanSupplier enabled) {
        this.setPrefix = setPrefix;
        this.enabled = enabled;
    }

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        if (enabled.getAsBoolean() == false) {
            return super.generate(previousCommands, previousOutput, schema, executor, context);
        }
        StringBuilder result = new StringBuilder();
        result.append(setPrefix);
        if (randomDouble() < QUERY_APPROXIMATION_SETTING_PROBABILITY) {
            result.append(randomQueryApproximationSettings());
        }
        Map<String, Object> commandContext = new HashMap<>();
        commandContext.put(UNMAPPED_FIELDS_ENABLED, Boolean.TRUE);
        appendFromCommand(result, schema, executor, context, commandContext);
        return new CommandDescription("from", this, result.toString(), commandContext);
    }
}

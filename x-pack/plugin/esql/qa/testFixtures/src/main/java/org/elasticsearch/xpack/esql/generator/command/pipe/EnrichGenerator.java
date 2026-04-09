/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.test.ESTestCase.randomFrom;

public class EnrichGenerator implements CommandGenerator {

    public static final String ENRICH = "enrich";
    public static final String ENRICH_FIELDS = "enrichFields";
    public static final CommandGenerator INSTANCE = new EnrichGenerator();

    private static final Pattern ENRICH_FIELDS_PATTERN = Pattern.compile("\"enrich_fields\"\\s*:\\s*\\[([^\\]]*)]");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String field = EsqlQueryGenerator.randomKeywordField(previousOutput);
        if (field == null || schema.enrichPolicies().isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        // TODO add WITH
        CsvTestsDataLoader.EnrichConfig policy = randomFrom(EsqlQueryGenerator.policiesOnKeyword(schema.enrichPolicies()));
        String cmdString = " | enrich " + policy.policyName() + " on " + field;
        List<String> enrichFields = parseEnrichFields(policy.loadPolicy());
        return new CommandDescription(ENRICH, this, cmdString, Map.of(ENRICH_FIELDS, enrichFields));
    }

    static List<String> parseEnrichFields(String policyJson) {
        Matcher m = ENRICH_FIELDS_PATTERN.matcher(policyJson);
        if (m.find()) {
            List<String> fields = new ArrayList<>();
            for (String part : m.group(1).split(",")) {
                String trimmed = part.trim();
                if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                    trimmed = trimmed.substring(1, trimmed.length() - 1);
                }
                if (trimmed.isEmpty() == false) {
                    fields.add(trimmed);
                }
            }
            return fields;
        }
        return List.of();
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
        if (commandDescription == EMPTY_DESCRIPTION) {
            return VALIDATION_OK;
        }

        if (previousColumns.size() > columns.size()) {
            return new ValidationResult(false, "Expecting at least [" + previousColumns.size() + "] columns, got [" + columns.size() + "]");
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}

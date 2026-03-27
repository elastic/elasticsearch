/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.command.RegisteredDomainFunctionBridge;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;

public class RegisteredDomainGenerator implements CommandGenerator {

    public static final CommandGenerator INSTANCE = new RegisteredDomainGenerator();

    public static final String REGISTERED_DOMAIN = "registered_domain";

    private static final String PREFIX = "prefix";

    private static final LinkedHashMap<String, String> REGISTERED_DOMAIN_OUTPUT_FIELDS;
    static {
        LinkedHashMap<String, Class<?>> outputFields = RegisteredDomainFunctionBridge.getAllOutputFields();
        REGISTERED_DOMAIN_OUTPUT_FIELDS = new LinkedHashMap<>(outputFields.size());
        for (Map.Entry<String, Class<?>> e : outputFields.entrySet()) {
            REGISTERED_DOMAIN_OUTPUT_FIELDS.putLast(e.getKey(), Objects.requireNonNull(DataType.fromJavaType(e.getValue())).typeName());
        }
    }

    private static final String[] LITERAL_FQDNS = new String[] {
        "www.example.co.uk",
        "elastic.co",
        "content-autofill.googleapis.com",
        "www.books.amazon.co.uk",
        "sub.example.com",
        "example.com" };

    private static final Set<String> HOSTNAME_LIKE_FIELD_NAMES = Set.of("domain", "host", "hostname", "fqdn");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor
    ) {
        String inputExpression = pickFqdnInput(previousOutput);
        if (inputExpression == null) {
            return EMPTY_DESCRIPTION;
        }
        String prefixRaw = EsqlQueryGenerator.randomIdentifier();
        String prefixForCmd = EsqlQueryGenerator.needsQuoting(prefixRaw) ? EsqlQueryGenerator.quote(prefixRaw) : prefixRaw;
        String cmdString = " | registered_domain " + prefixForCmd + " = " + inputExpression;
        return new CommandDescription(REGISTERED_DOMAIN, this, cmdString, Map.of(PREFIX, prefixRaw));
    }

    private static String pickFqdnInput(List<Column> previousOutput) {
        if (randomBoolean()) {
            return "\"" + randomFrom(LITERAL_FQDNS) + "\"";
        }
        return hostnameLikeFieldOrRandomString(previousOutput);
    }

    private static String hostnameLikeFieldOrRandomString(List<Column> previousOutput) {
        List<Column> stringColumns = previousOutput.stream()
            .filter(c -> "keyword".equals(c.type()) || "text".equals(c.type()))
            .filter(EsqlQueryGenerator::fieldCanBeUsed)
            .toList();
        if (stringColumns.isEmpty()) {
            return null;
        }
        for (Column c : stringColumns) {
            String name = c.name();
            if (HOSTNAME_LIKE_FIELD_NAMES.contains(EsqlQueryGenerator.unquote(name))) {
                return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
            }
        }
        Column chosen = randomFrom(stringColumns);
        String name = chosen.name();
        return EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name;
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

        String prefix = (String) commandDescription.context().get(PREFIX);
        if (prefix == null) {
            return new ValidationResult(false, "Missing prefix in command context");
        }

        int expectedColumns = REGISTERED_DOMAIN_OUTPUT_FIELDS.size();
        int expectedTotal = previousColumns.size() + expectedColumns;
        if (columns.size() != expectedTotal) {
            return new ValidationResult(
                false,
                "Expecting ["
                    + expectedTotal
                    + "] columns ("
                    + previousColumns.size()
                    + " previous + "
                    + expectedColumns
                    + " REGISTERED_DOMAIN), got ["
                    + columns.size()
                    + "]"
            );
        }

        var it = columns.iterator();
        int pos = 0;

        for (Column prev : previousColumns) {
            if (it.hasNext() == false) {
                return new ValidationResult(false, "Missing previous column [" + prev.name() + "] in output");
            }
            Column actual = it.next();
            pos++;
            if (actual.name().equals(prev.name()) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected column [" + prev.name() + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(prev.type()) == false) {
                return new ValidationResult(
                    false,
                    "Column [" + prev.name() + "] type changed from [" + prev.type() + "] to [" + actual.type() + "]"
                );
            }
        }

        for (Map.Entry<String, String> e : REGISTERED_DOMAIN_OUTPUT_FIELDS.entrySet()) {
            if (it.hasNext() == false) {
                return new ValidationResult(
                    false,
                    "Missing REGISTERED_DOMAIN column [" + prefix + "." + e.getKey() + "] (expected type [" + e.getValue() + "])"
                );
            }
            Column actual = it.next();
            pos++;
            String expectedName = prefix + "." + e.getKey();
            String expectedType = e.getValue();
            if (actual.name().equals(expectedName) == false) {
                return new ValidationResult(
                    false,
                    "At position " + pos + ": expected REGISTERED_DOMAIN column [" + expectedName + "], got [" + actual.name() + "]"
                );
            }
            if (actual.type().equals(expectedType) == false) {
                return new ValidationResult(
                    false,
                    "REGISTERED_DOMAIN column [" + expectedName + "] expected type [" + expectedType + "], got [" + actual.type() + "]"
                );
            }
        }

        return CommandGenerator.expectSameRowCount(previousCommands, previousOutput, output);
    }
}

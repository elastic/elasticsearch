/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.GenerationContext;
import org.elasticsearch.xpack.esql.generator.QueryExecutor;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;

/**
 * Generates {@code FILLNULL} commands in two shapes:
 * <ul>
 *   <li>all-fields: {@code | FILLNULL [WITH <value>]}</li>
 *   <li>targeted: {@code | FILLNULL [WITH <value>] field1, field2, ...}</li>
 * </ul>
 * When a fill value is combined with explicit targets, the value type is chosen to be compatible with the
 * selected fields, since the verifier rejects an incompatible targeted fill (that is correct behavior, not a bug).
 * Filled columns become reference attributes; {@code GenerativeRestTest#updateIndexMapped} consumes the
 * {@link #ALL_FIELDS} / {@link #FILLED_FIELDS} context entries to clear their {@code indexMapped} flag.
 */
public class FillNullGenerator implements CommandGenerator {

    public static final String FILL_NULL = "fillnull";
    public static final String ALL_FIELDS = "all_fields";
    public static final String FILLED_FIELDS = "filled_fields";

    public static final CommandGenerator INSTANCE = new FillNullGenerator();

    private static final Set<String> NUMERIC_TYPES = Set.of("integer", "long", "double");
    private static final Set<String> STRING_TYPES = Set.of("keyword", "text");
    private static final Set<String> BOOLEAN_TYPES = Set.of("boolean");

    @Override
    public CommandDescription generate(
        List<CommandDescription> previousCommands,
        List<Column> previousOutput,
        QuerySchema schema,
        QueryExecutor executor,
        GenerationContext context
    ) {
        if (EsqlCapabilities.Cap.FILLNULL.isEnabled() == false) {
            return EMPTY_DESCRIPTION;
        }

        if (randomBoolean()) {
            StringBuilder cmd = new StringBuilder(" | fillnull");
            if (randomBoolean()) {
                cmd.append(" with ").append(randomFillValue());
            }
            return new CommandDescription(FILL_NULL, this, cmd.toString(), Map.of(ALL_FIELDS, Boolean.TRUE));
        }

        String fillValue = null;
        List<Column> candidates = null;
        if (randomBoolean()) {
            // Targeted FILLNULL WITH <value>: restrict targets to a type family compatible with the value.
            Set<String> family = randomFrom(NUMERIC_TYPES, STRING_TYPES, BOOLEAN_TYPES);
            candidates = previousOutput.stream().filter(EsqlQueryGenerator::fieldCanBeUsed).filter(c -> family.contains(c.type())).toList();
            if (candidates.isEmpty() == false) {
                fillValue = randomFillValueOfFamily(family);
            }
        }
        if (candidates == null || candidates.isEmpty()) {
            // Targeted FILLNULL with type-appropriate defaults: any usable field except unsupported (which cannot resolve).
            fillValue = null;
            candidates = previousOutput.stream()
                .filter(EsqlQueryGenerator::fieldCanBeUsed)
                .filter(c -> c.type().equals("unsupported") == false)
                .toList();
        }
        if (candidates.isEmpty()) {
            return EMPTY_DESCRIPTION;
        }

        List<String> rawNames = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        int count = randomIntBetween(1, Math.min(3, candidates.size()));
        for (int i = 0; i < count * 2 && seen.size() < count; i++) {
            seen.add(randomFrom(candidates).name());
        }
        StringBuilder cmd = new StringBuilder(" | fillnull");
        if (fillValue != null) {
            cmd.append(" with ").append(fillValue);
        }
        cmd.append(" ");
        boolean first = true;
        for (String name : seen) {
            if (first == false) {
                cmd.append(", ");
            }
            first = false;
            cmd.append(EsqlQueryGenerator.needsQuoting(name) ? EsqlQueryGenerator.quote(name) : name);
            rawNames.add(name);
        }
        return new CommandDescription(FILL_NULL, this, cmd.toString(), Map.of(FILLED_FIELDS, rawNames));
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
        return CommandGenerator.expectSameColumns(previousCommands, previousColumns, columns);
    }

    private static String randomFillValue() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> Integer.toString(randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE));
            case 1 -> Long.toString(randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE));
            case 2 -> "\"" + randomAlphaOfLength(randomIntBetween(0, 10)) + "\"";
            case 3 -> Boolean.toString(randomBoolean());
            case 4 -> randomIntBetween(0, 1000) + "." + randomIntBetween(0, 999);
            default -> "null";
        };
    }

    private static String randomFillValueOfFamily(Set<String> family) {
        if (family == STRING_TYPES) {
            return "\"" + randomAlphaOfLength(randomIntBetween(0, 10)) + "\"";
        }
        if (family == BOOLEAN_TYPES) {
            return Boolean.toString(randomBoolean());
        }
        return Integer.toString(randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE));
    }
}

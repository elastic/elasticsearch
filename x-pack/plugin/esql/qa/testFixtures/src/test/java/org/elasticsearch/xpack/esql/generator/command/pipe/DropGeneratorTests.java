/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.generator.command.CommandGenerator.EMPTY_DESCRIPTION;
import static org.hamcrest.Matchers.greaterThan;

public class DropGeneratorTests extends ESTestCase {

    private static final String UNMAPPED_FIELDS = "_unmapped_fields";

    public void testWildcardDoesNotDropTheUnmappedFieldsColumn() {
        List<Column> previousOutput = List.of(
            column("emp_no", "integer"),
            column("first_name", "keyword"),
            column(UNMAPPED_FIELDS, DataType.SOURCE.outputType())
        );
        int wildcards = 0;
        int named = 0;
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(previousOutput);
            if (description == EMPTY_DESCRIPTION) {
                continue;
            }
            for (String projection : projections(description)) {
                if (derivesFrom(projection, UNMAPPED_FIELDS)) {
                    if (projection.contains("*")) {
                        wildcards++;
                    } else {
                        named++;
                    }
                }
            }
            assertEquals(description.commandString(), expectedDropped(previousOutput, description), droppedColumns(description));
        }
        assertThat(wildcards, greaterThan(0));
        assertThat(named, greaterThan(0));
    }

    public void testWildcardDropsAQueryColumnNamedUnmappedFields() {
        List<Column> previousOutput = List.of(
            column("emp_no", "integer"),
            column("first_name", "keyword"),
            column(UNMAPPED_FIELDS, "keyword")
        );
        int wildcards = 0;
        for (int i = 0; i < 1000; i++) {
            CommandDescription description = generate(previousOutput);
            if (description == EMPTY_DESCRIPTION) {
                continue;
            }
            for (String projection : projections(description)) {
                if (projection.contains("*") && derivesFrom(projection, UNMAPPED_FIELDS)) {
                    wildcards++;
                }
            }
            assertEquals(description.commandString(), expectedDropped(previousOutput, description), droppedColumns(description));
        }
        assertThat(wildcards, greaterThan(0));
    }

    private static CommandDescription generate(List<Column> previousOutput) {
        return DropGenerator.INSTANCE.generate(List.of(), previousOutput, null, null, null);
    }

    private static Column column(String name, String type) {
        return new Column(name, type, List.of());
    }

    private static List<String> projections(CommandDescription description) {
        String prefix = " | " + DropGenerator.DROP + " ";
        assertTrue(description.commandString(), description.commandString().startsWith(prefix));
        return Arrays.asList(description.commandString().substring(prefix.length()).split(", "));
    }

    private static Set<String> expectedDropped(List<Column> previousOutput, CommandDescription description) {
        Set<String> expected = new HashSet<>();
        for (String projection : projections(description)) {
            List<Column> sources = previousOutput.stream().filter(c -> derivesFrom(projection, c.name())).toList();
            assertEquals(description.commandString(), 1, sources.size());
            Column source = sources.getFirst();
            boolean unmappedFieldsColumn = source.name().equals(UNMAPPED_FIELDS) && source.type().equals(DataType.SOURCE.outputType());
            if (projection.contains("*") == false || unmappedFieldsColumn == false) {
                expected.add(source.name());
            }
        }
        return expected;
    }

    private static boolean derivesFrom(String projection, String name) {
        if (projection.startsWith("*")) {
            return name.endsWith(projection.substring(1));
        }
        if (projection.endsWith("*")) {
            return name.startsWith(projection.substring(0, projection.length() - 1));
        }
        return EsqlQueryGenerator.unquote(projection).equals(name);
    }

    @SuppressWarnings("unchecked")
    private static Set<String> droppedColumns(CommandDescription description) {
        return (Set<String>) description.context().get(DropGenerator.DROPPED_COLUMNS);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.CommandDescription;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator.ValidationResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.generator.command.CommandGenerator.EMPTY_DESCRIPTION;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Deterministic coverage for {@link FillNullGenerator}: the comparative invariant in {@link FillNullGenerator#validateOutput}
 * (a type-matched targeted fill must leave no null in the filled columns), and the shapes {@link FillNullGenerator#generate}
 * produces. The invariant is asserted on the result alone, so these tests build the result directly rather than running a
 * query, which keeps them independent of the generative framework's non-deterministic row order and truncation.
 */
public class FillNullGeneratorTests extends ESTestCase {

    private static final List<Column> SCHEMA = List.of(column("a", "integer"), column("b", "keyword"));

    public void testValueTargetedFillLeavingANullFailsValidation() {
        CommandDescription command = valueTargeted("a");
        // Column "a" is still null in a row after a FILLNULL that targeted it with a type-matched value: that must fail.
        List<List<Object>> before = List.of(Arrays.asList(null, "x"), Arrays.asList(5, "y"));
        List<List<Object>> after = List.of(Arrays.asList(null, "x"), Arrays.asList(5, "y"));

        ValidationResult result = FillNullGenerator.INSTANCE.validateOutput(List.of(), command, SCHEMA, before, SCHEMA, after);

        assertFalse(result.success());
        assertThat(result.errorMessage(), containsString("left a null in filled column [a]"));
    }

    public void testValueTargetedFillWithoutNullsPassesValidation() {
        CommandDescription command = valueTargeted("a");
        List<List<Object>> before = List.of(Arrays.asList(null, "x"), Arrays.asList(5, "y"));
        List<List<Object>> after = List.of(Arrays.asList(0, "x"), Arrays.asList(5, "y"));

        ValidationResult result = FillNullGenerator.INSTANCE.validateOutput(List.of(), command, SCHEMA, before, SCHEMA, after);

        assertTrue(result.errorMessage(), result.success());
    }

    public void testDefaultTargetedFillIsNotHeldToTheNoNullInvariant() {
        // DEFAULT can leave nulls (types with no default), so a leftover null must not fail validation.
        CommandDescription command = new CommandDescription(
            FillNullGenerator.FILL_NULL,
            FillNullGenerator.INSTANCE,
            " | fillnull DEFAULT ON a",
            Map.of(FillNullGenerator.FILLED_FIELDS, List.of("a"), FillNullGenerator.HAS_FILL_VALUE, false)
        );
        List<List<Object>> before = List.of(Arrays.asList(null, "x"));
        List<List<Object>> after = List.of(Arrays.asList(null, "x"));

        ValidationResult result = FillNullGenerator.INSTANCE.validateOutput(List.of(), command, SCHEMA, before, SCHEMA, after);

        assertTrue(result.errorMessage(), result.success());
    }

    public void testAllFieldsFillIsNotHeldToTheNoNullInvariant() {
        // ON * fills only the compatible columns, so a leftover null must not fail validation.
        CommandDescription command = new CommandDescription(
            FillNullGenerator.FILL_NULL,
            FillNullGenerator.INSTANCE,
            " | fillnull 0 ON *",
            Map.of(FillNullGenerator.ALL_FIELDS, Boolean.TRUE)
        );
        List<List<Object>> before = List.of(Arrays.asList(null, "x"));
        List<List<Object>> after = List.of(Arrays.asList(null, "x"));

        ValidationResult result = FillNullGenerator.INSTANCE.validateOutput(List.of(), command, SCHEMA, before, SCHEMA, after);

        assertTrue(result.errorMessage(), result.success());
    }

    public void testEmptyDescriptionValidatesOk() {
        List<List<Object>> rows = List.of(Arrays.asList(null, "x"));
        assertTrue(FillNullGenerator.INSTANCE.validateOutput(List.of(), EMPTY_DESCRIPTION, SCHEMA, rows, SCHEMA, rows).success());
    }

    public void testGeneratedShapesAreConsistent() {
        // generate() is a no-op unless FILLNULL is enabled (it is snapshot-only until the command is released), so this
        // case only has shapes to assert on a snapshot build. The validateOutput cases above build the command by hand and
        // run everywhere.
        assumeTrue("requires FILLNULL capability", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        List<Column> previousOutput = List.of(
            column("i", "integer"),
            column("k", "keyword"),
            column("b", "boolean"),
            column("d", "datetime"),
            column("ip_addr", "ip"),
            column("v", "version")
        );
        int allFields = 0;
        int valueTargeted = 0;
        int defaultTargeted = 0;
        for (int iter = 0; iter < 2000; iter++) {
            CommandDescription command = FillNullGenerator.INSTANCE.generate(List.of(), previousOutput, null, null, null);
            if (command == EMPTY_DESCRIPTION) {
                continue;
            }
            assertThat(command.commandString(), startsWith(" | fillnull"));
            if (Boolean.TRUE.equals(command.context().get(FillNullGenerator.ALL_FIELDS))) {
                allFields++;
                assertThat(command.commandString(), containsString(" ON *"));
                assertNull(command.context().get(FillNullGenerator.FILLED_FIELDS));
            } else {
                assertThat(command.context().get(FillNullGenerator.FILLED_FIELDS), instanceOf(List.class));
                assertFalse(((List<?>) command.context().get(FillNullGenerator.FILLED_FIELDS)).isEmpty());
                if (Boolean.TRUE.equals(command.context().get(FillNullGenerator.HAS_FILL_VALUE))) {
                    valueTargeted++;
                    // A concrete value, not DEFAULT.
                    assertThat(command.commandString(), not(containsString("DEFAULT")));
                } else {
                    defaultTargeted++;
                    assertThat(command.commandString(), containsString("DEFAULT"));
                }
            }
        }
        assertThat(allFields, greaterThan(0));
        assertThat(valueTargeted, greaterThan(0));
        assertThat(defaultTargeted, greaterThan(0));
    }

    private static CommandDescription valueTargeted(String field) {
        return new CommandDescription(
            FillNullGenerator.FILL_NULL,
            FillNullGenerator.INSTANCE,
            " | fillnull 0 ON " + field,
            Map.of(FillNullGenerator.FILLED_FIELDS, List.of(field), FillNullGenerator.HAS_FILL_VALUE, true)
        );
    }

    private static Column column(String name, String type) {
        return new Column(name, type, List.of());
    }
}

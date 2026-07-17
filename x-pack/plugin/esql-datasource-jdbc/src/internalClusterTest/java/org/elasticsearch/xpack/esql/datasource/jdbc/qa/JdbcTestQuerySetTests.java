/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.ExpectedColumn;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Fixture;
import org.elasticsearch.xpack.esql.datasource.jdbc.qa.JdbcTestQuerySet.Scenario;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Pure unit test (no cluster boot, no database) that guards {@link JdbcTestQuerySet}'s internal consistency, so a
 * typo in the shared matrix fails fast here rather than as a confusing assertion inside a real-database IT.
 * <p>
 * It lives in the {@code internalClusterTest} source set — not {@code test} — because {@link JdbcTestQuerySet} lives
 * in that source set (a {@code test}-source-set class cannot see it), and the {@code internalClusterTest} task runs
 * plain {@link ESTestCase} classes as well as ITs. It boots nothing, so it stays cheap.
 */
public class JdbcTestQuerySetTests extends ESTestCase {

    /** ES|QL {@code outputType()} strings the matrix is allowed to declare, mapped to the Java cell type they yield. */
    private static final Map<String, Class<?>> TYPE_TO_JAVA = Map.of(
        "integer",
        Integer.class,
        "long",
        Long.class,
        "double",
        Double.class,
        "keyword",
        String.class,
        "boolean",
        Boolean.class,
        // DATETIME columns render as an ISO string; the matrix asserts these by type only (no value rows), but map
        // it so a stray temporal cell would still be checked rather than silently accepted.
        "date",
        String.class
    );

    public void testAtLeastTwentyScenarios() {
        assertThat(
            "shared correctness matrix should carry the planned ~20 scenarios",
            JdbcTestQuerySet.scenarios().size(),
            greaterThanOrEqualTo(20)
        );
    }

    public void testQueryIdsAreUnique() {
        Set<String> seen = new HashSet<>();
        for (Scenario scenario : JdbcTestQuerySet.scenarios()) {
            assertTrue("duplicate queryId [" + scenario.queryId() + "]", seen.add(scenario.queryId()));
        }
    }

    public void testNoUnresolvedPlaceholders() {
        String dataset = "sample_dataset";
        for (Scenario scenario : JdbcTestQuerySet.scenarios()) {
            String id = scenario.queryId();
            assertTrue(
                "pattern for [" + id + "] must contain the dataset placeholder",
                scenario.esqlPattern().contains(JdbcTestQuerySet.DATASET_PLACEHOLDER)
            );

            String resolved = scenario.esql(dataset);
            assertThat("resolved ES|QL for [" + id + "] should be non-blank", resolved.isBlank(), equalTo(false));
            assertThat("resolved ES|QL for [" + id + "] should start with FROM", resolved.startsWith("FROM "), equalTo(true));
            assertThat("resolved ES|QL for [" + id + "] should target the dataset", resolved.contains(dataset), equalTo(true));
            // No brace tokens should survive substitution: the placeholder is the only templated part.
            assertThat("resolved ES|QL for [" + id + "] leaked a '{'", resolved.indexOf('{'), equalTo(-1));
            assertThat("resolved ES|QL for [" + id + "] leaked a '}'", resolved.indexOf('}'), equalTo(-1));
        }
    }

    public void testExpectedColumnsAreWellFormed() {
        for (Scenario scenario : JdbcTestQuerySet.scenarios()) {
            String id = scenario.queryId();
            assertThat("[" + id + "] should declare at least one column", scenario.columns().isEmpty(), equalTo(false));
            for (ExpectedColumn column : scenario.columns()) {
                assertThat("[" + id + "] column name blank", column.name().isBlank(), equalTo(false));
                assertTrue(
                    "[" + id + "] column [" + column.name() + "] has unknown type [" + column.type() + "]",
                    TYPE_TO_JAVA.containsKey(column.type())
                );
            }
            assertThat(
                "[" + id + "] column names should be unique",
                new HashSet<>(scenario.columnNames()).size(),
                equalTo(scenario.columns().size())
            );
        }
    }

    public void testRowShapesMatchColumns() {
        for (Scenario scenario : JdbcTestQuerySet.scenarios()) {
            String id = scenario.queryId();
            int width = scenario.columns().size();
            List<List<Object>> rows = scenario.rows();
            for (int r = 0; r < rows.size(); r++) {
                List<Object> row = rows.get(r);
                assertThat("[" + id + "] row " + r + " width", row.size(), equalTo(width));
                for (int c = 0; c < width; c++) {
                    Object cell = row.get(c);
                    if (cell == null) {
                        continue; // SQL NULL is representable for any column type
                    }
                    Class<?> expected = TYPE_TO_JAVA.get(scenario.columns().get(c).type());
                    assertThat(
                        "[" + id + "] row " + r + " col " + c + " (" + scenario.columns().get(c).name() + ") java type",
                        cell,
                        instanceOf(expected)
                    );
                }
            }
        }
    }

    public void testEveryScenarioReferencesALoadableFixtureResource() {
        for (Fixture fixture : Fixture.values()) {
            assertNotNull(
                "fixture resource [" + fixture.resourcePath() + "] not found on the classpath",
                JdbcTestQuerySetTests.class.getResource(fixture.resourcePath())
            );
            assertThat("fixture table name blank for " + fixture, fixture.tableName().isBlank(), equalTo(false));
        }
        for (Scenario scenario : JdbcTestQuerySet.scenarios()) {
            assertNotNull("[" + scenario.queryId() + "] has no fixture", scenario.fixture());
        }
    }

    public void testEveryFixtureIsExercised() {
        for (Fixture fixture : Fixture.values()) {
            if (fixture == Fixture.PUSHDOWN_PARITY) {
                // PUSHDOWN_PARITY carries adversarial data exercised end-to-end by AbstractJdbcPushdownParityIT
                // (pushdown on-vs-off equivalence), not by the shared JdbcTestQuerySet scenario matrix, so it
                // deliberately has no scenariosFor(...) entry.
                continue;
            }
            assertThat(
                "fixture " + fixture + " has no scenario exercising it",
                JdbcTestQuerySet.scenariosFor(fixture).isEmpty(),
                equalTo(false)
            );
        }
    }
}

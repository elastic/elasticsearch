/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A runtime field is defined at the root of the mapping, so one named under a nested field is a field of the parent documents:
 * an ordinary column before and after {@code NESTED_EXPAND}, repeated on each row the command makes of a parent.
 */
public class NestedExpandRuntimeFieldIT extends AbstractEsqlIntegTestCase {

    private static final String RUNTIME_FROM_SOURCE = """
        {
          "runtime": { "items.calc": { "type": "long" } },
          "properties": {
            "id": { "type": "keyword" },
            "items": { "type": "nested", "dynamic": false, "properties": { "qty": { "type": "long" } } }
          }
        }""";

    private static final String CHILD = """
        {
          "properties": {
            "id": { "type": "keyword" },
            "items": { "type": "nested", "properties": { "qty": { "type": "long" }, "calc": { "type": "long" } } }
          }
        }""";

    private static final String DOC = """
        {"id": "a", "items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}""";

    @Before
    public void requireNestedExpand() {
        assumeTrue("NESTED_EXPAND is snapshot-only", EsqlCapabilities.Cap.NESTED_EXPAND.isEnabled());
    }

    private void createIndex(String index, String mapping, boolean synthetic) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", randomIntBetween(1, 2));
        if (synthetic) {
            settings.put("index.mapping.source.mode", "synthetic");
        }
        assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).setMapping(mapping));
        client().prepareIndex(index)
            .setId("1")
            .setSource(DOC, XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private void assertRows(String query, List<List<Object>> expected) {
        try (var response = run(query)) {
            assertThat(query, getValuesList(response), equalTo(expected));
        }
    }

    public void testRuntimeFieldOfStoredSourceIsRepeatedOnEveryChildRow() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        assertRows(
            "FROM rt | NESTED_EXPAND items | KEEP items.qty, items.calc | SORT items.qty",
            List.of(List.of(1L, List.of(3L, 4L)), List.of(2L, List.of(3L, 4L)))
        );
    }

    public void testRuntimeFieldOfSyntheticSourceIsRepeatedOnEveryChildRow() {
        createIndex("rts", RUNTIME_FROM_SOURCE, true);
        assertRows(
            "FROM rts | NESTED_EXPAND items | KEEP items.qty, items.calc | SORT items.qty",
            List.of(List.of(1L, List.of(3L, 4L)), List.of(2L, List.of(3L, 4L)))
        );
    }

    public void testTheSameColumnWithAndWithoutTheCommand() {
        boolean synthetic = randomBoolean();
        createIndex("rt", RUNTIME_FROM_SOURCE, synthetic);
        List<Object> value = List.of(3L, 4L);
        assertRows("FROM rt | KEEP id, items.calc", List.of(List.of("a", value)));
        assertRows("FROM rt | NESTED_EXPAND items | KEEP id, items.calc | LIMIT 1", List.of(List.of("a", value)));
        try (
            var withoutCommand = run("FROM rt | KEEP id, items.calc");
            var withCommand = run("FROM rt | NESTED_EXPAND items | KEEP id, items.calc")
        ) {
            assertThat(withCommand.columns(), equalTo(withoutCommand.columns()));
            assertThat(withoutCommand.columns().get(1).outputType(), equalTo(DataType.LONG.outputType()));
        }
    }

    public void testRuntimeFieldBeforeTheExpansion() {
        createIndex("rts", RUNTIME_FROM_SOURCE, true);
        assertRows(
            "FROM rts | EVAL c = MV_MAX(items.calc) | NESTED_EXPAND items | KEEP id, items.qty, c | SORT items.qty",
            List.of(List.of("a", 1L, 4L), List.of("a", 2L, 4L))
        );
        assertRows("FROM rts | WHERE MV_MAX(items.calc) > 3 | NESTED_EXPAND items | STATS n = COUNT(*)", List.of(List.of(2L)));
        assertRows("FROM rts | WHERE MV_MAX(items.calc) > 4 | NESTED_EXPAND items | STATS n = COUNT(*)", List.of(List.of(0L)));
    }

    public void testRuntimeFieldAfterTheExpansion() {
        createIndex("rt", RUNTIME_FROM_SOURCE, randomBoolean());
        assertRows(
            "FROM rt | NESTED_EXPAND items | EVAL m = MV_MAX(items.calc) | KEEP items.qty, m | SORT items.qty",
            List.of(List.of(1L, 4L), List.of(2L, 4L))
        );
        assertRows("FROM rt | NESTED_EXPAND items | STATS c = COUNT(items.qty), m = MAX(items.calc) BY id", List.of(List.of(2L, 4L, "a")));
    }

    public void testRuntimeFieldOfStoredSourceCanBeTestedForNullAndNotNull() {
        assertNullChecks(false);
    }

    public void testRuntimeFieldOfSyntheticSourceCanBeTestedForNullAndNotNull() {
        assertNullChecks(true);
    }

    private void assertNullChecks(boolean synthetic) {
        createIndex("rt", RUNTIME_FROM_SOURCE, synthetic);
        client().prepareIndex("rt")
            .setId("2")
            .setSource("{\"id\": \"b\"}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertRows("FROM rt | WHERE items.calc IS NOT NULL | KEEP id", List.of(List.of("a")));
        assertRows("FROM rt | WHERE items.calc IS NULL | KEEP id", List.of(List.of("b")));
        assertRows("FROM rt | NESTED_EXPAND items | WHERE items.calc IS NOT NULL | STATS n = COUNT(*)", List.of(List.of(2L)));
        assertRows("FROM rt | NESTED_EXPAND items | WHERE items.calc IS NULL | STATS n = COUNT(*)", List.of(List.of(1L)));
    }

    public void testRuntimeFieldAndAnUndeclaredNameUnderTheCollectionStayParentColumns() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        try (
            var response = run("SET unmapped_fields=\"load\"; FROM rt | NESTED_EXPAND items | KEEP items.qty, items.calc, items.undeclared")
        ) {
            assertThat(
                response.columns().stream().map(c -> c.name()).toList(),
                equalTo(List.of("items.qty", "items.calc", "items.undeclared"))
            );
            assertThat(response.columns().get(1).outputType(), equalTo(DataType.LONG.outputType()));
        }
    }

    public void testRuntimeFieldInOneIndexAndChildInAnotherGivesEachIndexItsOwnValues() {
        createIndex("rt", RUNTIME_FROM_SOURCE, randomBoolean());
        createIndex("mp", CHILD, false);
        assertRows(
            "FROM rt, mp METADATA _index | NESTED_EXPAND items | KEEP _index, items.qty, items.calc | SORT _index, items.qty",
            List.of(List.of("mp", 1L, 3L), List.of("mp", 2L, 4L), List.of("rt", 1L, List.of(3L, 4L)), List.of("rt", 2L, List.of(3L, 4L)))
        );
    }

    public void testTheValueKeptForAMergedNameIsNotAColumn() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        createIndex("mp", CHILD, false);
        try (var response = run("FROM rt, mp | NESTED_EXPAND items")) {
            List<String> names = response.columns().stream().map(c -> c.name()).toList();
            assertThat(names, equalTo(List.of("id", "items.qty", "items.calc")));
        }
    }

    public void testMergedNameCanBeUsedAfterTheExpansion() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        createIndex("mp", CHILD, false);
        assertRows(
            "FROM rt, mp METADATA _index | NESTED_EXPAND items | WHERE MV_MAX(items.calc) == 4 "
                + "| STATS n = COUNT(*) BY _index | SORT _index",
            List.of(List.of(1L, "mp"), List.of(2L, "rt"))
        );
    }

    public void testRuntimeFieldInOneIndexAndChildOfAnotherTypeInAnotherIsAnError() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        createIndex("kw", CHILD.replace("\"calc\": { \"type\": \"long\" }", "\"calc\": { \"type\": \"keyword\" }"), false);
        VerificationException e = expectThrows(VerificationException.class, () -> run("FROM rt, kw | NESTED_EXPAND items").close());
        assertThat(
            e.getMessage(),
            containsString(
                "NESTED_EXPAND cannot read [items.calc]: it is a runtime field of type [long] in some indices and a field of "
                    + "nested [items] of type [keyword] in others"
            )
        );
    }

    public void testRuntimeFieldInOneIndexAndChildInAnotherIsTheOrdinaryColumnOfTheFirstWithoutTheCommand() {
        createIndex("rt", RUNTIME_FROM_SOURCE, false);
        createIndex("mp", CHILD, false);
        assertRows(
            "FROM rt, mp METADATA _index | KEEP _index, items.calc | SORT _index",
            List.of(Arrays.asList("mp", null), List.of("rt", List.of(3L, 4L)))
        );
    }

    public void testChildWithoutARuntimeFieldOfTheSameNameStillExpands() {
        createIndex("mp", CHILD, false);
        assertRows(
            "FROM mp | NESTED_EXPAND items | KEEP items.qty, items.calc | SORT items.qty",
            List.of(List.of(1L, 3L), List.of(2L, 4L))
        );
    }
}

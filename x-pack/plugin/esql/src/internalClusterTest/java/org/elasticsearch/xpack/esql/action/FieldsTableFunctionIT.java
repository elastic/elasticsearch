/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code FROM FIELDS("logs-*")} against indices that really disagree.
 * <p>
 * Real queries rather than plan assertions, because the question the function answers is only answerable as a query: the conflict
 * detail it reports exists today solely inside the message of an error, where it cannot be filtered, grouped or counted.
 * <p>
 * Mappings are declared explicitly throughout. Dynamic mapping makes a string field {@code text}, which would conflict with a
 * {@code keyword} declared elsewhere and produce a union-type error that has nothing to do with this function.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class FieldsTableFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    /**
     * The central case. Two indices disagree on one field's type; an ordinary query can learn only that the field is
     * {@code unsupported}, and this reports which types disagree and which index holds each.
     */
    public void testAConflictIsVisibleAsARowCarryingItsTypesAndIndices() {
        createConflictingIndices();

        try (
            EsqlQueryResponse response = run(
                "FROM FIELDS(\"fields-conflict-*\") | WHERE conflict IS NOT NULL | KEEP name, type, conflicting_types, conflict"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            List<Object> row = rows.get(0);
            assertThat(row.get(0), equalTo("message"));
            assertThat(row.get(1), equalTo("unsupported"));
            assertThat(row.get(2), equalTo("keyword, text"));
            String conflict = (String) row.get(3);
            assertThat(conflict, containsString("mapped as [2] incompatible types"));
            assertThat(conflict, containsString("[keyword] in [fields-conflict-keyword]"));
            assertThat(conflict, containsString("[text] in [fields-conflict-text]"));
        }
    }

    /** A field every index agrees on reports no conflict, which is what makes {@code WHERE conflict IS NOT NULL} a useful filter. */
    public void testAgreedFieldsCarryNoConflict() {
        createConflictingIndices();

        try (
            EsqlQueryResponse response = run("FROM FIELDS(\"fields-conflict-*\") | WHERE conflict IS NULL | KEEP name, type | SORT name")
        ) {
            var rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(0)).toList(), contains("id"));
            assertThat(rows.get(0).get(1), equalTo("long"));
        }
    }

    /** Counting and grouping the disagreements, which is the whole of what an error message cannot do. */
    public void testConflictsGroupByTheirTypes() {
        createConflictingIndices();
        assertAcked(prepareCreate("fields-conflict-long").setMapping("message", "type=long", "id", "type=long"));

        try (
            EsqlQueryResponse response = run(
                "FROM FIELDS(\"fields-conflict-*\") | WHERE conflict IS NOT NULL | STATS n = COUNT(*) BY conflicting_types"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(1L));
            assertThat(rows.get(0).get(1), equalTo("keyword, long, text"));
        }
    }

    /** A pattern matching nothing is an answer — no fields — and still declares every column. */
    public void testColumnsAreDeclaredWhenThePatternMatchesNothing() {
        try (EsqlQueryResponse response = run("FROM FIELDS(\"fields-absent-*\")")) {
            assertThat(
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                contains("name", "type", "conflict", "conflicting_types")
            );
            assertThat(getValuesList(response), hasSize(0));
        }
    }

    /**
     * A relation with rows whose {@code conflict} column is null in every one of them, read rather than projected away. The page a
     * coordinator-rows function hands over has to agree with the columns it declared even when no value in a column implies its type.
     */
    public void testAnAllNullColumnIsStillReadableAsItsDeclaredType() {
        assertAcked(prepareCreate("fields-agreed").setMapping("message", "type=keyword", "id", "type=long"));

        try (EsqlQueryResponse response = run("FROM FIELDS(\"fields-agreed\") | KEEP name, conflict, conflicting_types | SORT name")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(2));
            for (List<Object> row : rows) {
                assertThat(row.get(1), nullValue());
                assertThat(row.get(2), nullValue());
            }
            assertThat(response.columns().stream().map(ColumnInfoImpl::outputType).toList(), contains("keyword", "keyword", "keyword"));
        }
    }

    /** Sub-fields are reported under their full path, which is the name a query would have to use. */
    public void testSubFieldsAreNamedByTheirFullPath() {
        assertAcked(prepareCreate("fields-nested").setMapping("host.ip", "type=ip", "host.name", "type=keyword"));

        try (EsqlQueryResponse response = run("FROM FIELDS(\"fields-nested\") | KEEP name, type | SORT name")) {
            var names = getValuesList(response).stream().map(row -> row.get(0)).toList();
            assertThat(names, contains("host", "host.ip", "host.name"));
        }
    }

    /**
     * A function stands where an index pattern stands, so a source list may hold both. {@code name} is declared as a keyword in the
     * index because a union over branches whose columns disagree on type is refused, and that refusal is about the mapping rather
     * than about table functions.
     */
    public void testBesideAnIndexPattern() {
        createConflictingIndices();
        assertAcked(prepareCreate("fields-plain").setMapping("name", "type=keyword"));
        prepareIndex("fields-plain").setSource("name", "from_the_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM fields-plain, FIELDS(\"fields-conflict-*\") | KEEP name | SORT name")) {
            var names = getValuesList(response).stream().map(row -> row.get(0)).toList();
            assertThat(names, hasItem("from_the_index"));
            assertThat(names, hasItem("message"));
            assertThat(names, hasItem("id"));
        }
    }

    public void testAnEmptyPatternIsRefused() {
        var e = expectThrows(Exception.class, () -> run("FROM FIELDS(\"\")").close());
        assertThat(e.getMessage(), containsString("[fields] requires an index pattern, found an empty one"));
    }

    /** The caller's own query sees the same field as nothing but unsupported, which is the gap this closes. */
    public void testAnOrdinaryQueryOnlySeesUnsupported() {
        createConflictingIndices();

        try (EsqlQueryResponse response = run("FROM fields-conflict-* | LIMIT 0")) {
            var message = response.columns().stream().filter(c -> c.name().equals("message")).findFirst().orElseThrow();
            assertThat(message.outputType(), equalTo("unsupported"));
        }
    }

    private void createConflictingIndices() {
        assertAcked(prepareCreate("fields-conflict-keyword").setMapping("message", "type=keyword", "id", "type=long"));
        assertAcked(prepareCreate("fields-conflict-text").setMapping("message", "type=text", "id", "type=long"));
        prepareIndex("fields-conflict-keyword").setSource("message", "a", "id", 1)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        prepareIndex("fields-conflict-text").setSource("message", "b", "id", 2)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }
}

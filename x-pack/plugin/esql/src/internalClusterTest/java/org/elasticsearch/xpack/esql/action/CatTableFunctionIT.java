/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Table;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.action.cat.RestShardsAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@code FROM CAT("shards", "cat-*")} against a real cluster.
 * <p>
 * Queries rather than plan assertions, because the thing under test is that a {@code _cat} table arrives as a
 * relation: the columns are the ones {@code _cat/shards} writes, the rows are the ones it would write, and both
 * come from that action rather than from anything here. A test that asserted a column list of its own would pass
 * while the two drifted apart, which is the defect this function exists to make impossible.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class CatTableFunctionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("requires table functions in FROM", EsqlCapabilities.Cap.FROM_TABLE_FUNCTION.isEnabled());
    }

    /**
     * The columns are {@code _cat/shards}' own, asked of the same method the function asks. Pinned by equality
     * rather than by naming a few, so a column added to {@code _cat/shards} does not need a change here and a
     * column this function invented would fail.
     */
    public void testTheColumnsAreTheCatShardsColumns() {
        createShards();

        try (EsqlQueryResponse response = run("FROM CAT(\"shards\", \"cat-*\")")) {
            List<String> expected = new ArrayList<>();
            for (Table.Cell header : RestShardsAction.getTableWithHeader(RestApiVersion.current()).getHeaders()) {
                expected.add(String.valueOf(header.value));
            }
            assertThat(response.columns().stream().map(ColumnInfoImpl::name).toList(), equalTo(expected));
            assertThat(expected, hasItem("node"));
        }
    }

    /** One row per shard copy of the pattern, and the index column names the index each came from. */
    public void testOneRowPerShardCopy() {
        createShards();

        try (EsqlQueryResponse response = run("FROM CAT(\"shards\", \"cat-one\") | KEEP index, shard, prirep | SORT shard")) {
            var rows = getValuesList(response);
            // Two shards, no replicas, so two copies.
            assertThat(rows, hasSize(2));
            assertThat(rows.get(0).get(0), equalTo("cat-one"));
            assertThat(rows.get(0).get(2), equalTo("p"));
        }
    }

    /**
     * The question the text table cannot answer. Grouping by node is why this is a relation rather than something
     * a reader pipes through another tool.
     */
    public void testShardsGroupByNode() {
        createShards();

        try (EsqlQueryResponse response = run("FROM CAT(\"shards\", \"cat-*\") | STATS copies = COUNT(*) BY node | SORT node")) {
            var rows = getValuesList(response);
            assertThat(rows, not(hasSize(0)));
            long total = 0;
            for (List<Object> row : rows) {
                total += (Long) row.get(0);
            }
            // cat-one has two shards and cat-two one, all unreplicated.
            assertThat(total, equalTo(3L));
        }
    }

    /** A function stands where an index pattern stands, so a source list may hold both. */
    public void testBesideAnIndexPattern() {
        createShards();
        assertAcked(prepareCreate("cat-plain").setMapping("index", "type=keyword"));
        prepareIndex("cat-plain").setSource("index", "from_the_index").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        try (EsqlQueryResponse response = run("FROM cat-plain, CAT(\"shards\", \"cat-one\") | KEEP index | SORT index")) {
            var names = getValuesList(response).stream().map(row -> row.get(0)).toList();
            assertThat(names, hasItem("from_the_index"));
            assertThat(names, hasItem("cat-one"));
        }
    }

    /** Two calls at different arguments in one query, which no source command could be. */
    public void testTwoCallsAtDifferentPatterns() {
        createShards();

        try (
            EsqlQueryResponse response = run(
                "FROM CAT(\"shards\", \"cat-one\"), CAT(\"shards\", \"cat-two\") | STATS copies = COUNT(*) BY index | SORT index"
            )
        ) {
            var rows = getValuesList(response);
            assertThat(rows.stream().map(row -> row.get(1)).toList(), contains("cat-one", "cat-two"));
            assertThat(rows.get(0).get(0), equalTo(2L));
            assertThat(rows.get(1).get(0), equalTo(1L));
        }
    }

    /** A pattern matching nothing is an answer — no shards — and still declares every column. */
    public void testAPatternMatchingNothingHasNoRows() {
        try (EsqlQueryResponse response = run("FROM CAT(\"shards\", \"cat-absent-*\")")) {
            assertThat(getValuesList(response), hasSize(0));
            assertThat(response.columns().stream().map(ColumnInfoImpl::name).toList(), hasItem("node"));
        }
    }

    /**
     * The columns nothing read: a stats column, which is null unless the stats request asks for everything, and a
     * dotted name, which has to resolve as a column name at all. Dropping {@code .all()} from the stats request,
     * or rendering a null cell as the string, left every other case green.
     */
    public void testStatsAndUnassignedColumnsAreReadable() {
        createShards();

        try (EsqlQueryResponse response = run("FROM CAT(\"shards\", \"cat-two\") | KEEP docs, store, unassigned.reason")) {
            var rows = getValuesList(response);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo("0"));
            assertThat((String) rows.get(0).get(1), matchesRegex("\\d+(\\.\\d+)?[a-z]+"));
            assertThat(rows.get(0).get(2), nullValue());
        }
    }

    public void testAnUnknownTableIsRefused() {
        var e = expectThrows(ParsingException.class, () -> run("FROM CAT(\"nosuchtable\", \"cat-*\")").close());
        assertThat(e.getMessage(), containsString("[cat] has no table named [nosuchtable]"));
    }

    public void testAnEmptyPatternIsRefused() {
        var e = expectThrows(ParsingException.class, () -> run("FROM CAT(\"shards\", \"\")").close());
        assertThat(e.getMessage(), containsString("[cat] requires an index pattern, found an empty one"));
    }

    private void createShards() {
        assertAcked(prepareCreate("cat-one").setSettings(indexSettings(2, 0)).setMapping("a", "type=long"));
        assertAcked(prepareCreate("cat-two").setSettings(indexSettings(1, 0)).setMapping("a", "type=long"));
        ensureGreen("cat-one", "cat-two");
    }
}

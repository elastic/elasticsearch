/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.contains;

@ESIntegTestCase.ClusterScope(scope = TEST, minNumDataNodes = 0, maxNumDataNodes = 0)
@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class EsqlSearchActionIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        createAndPopulateIndex("test");
    }

    public void testFilterRangeQuery() {
        var query = """
            SEARCH test [
              | WHERE id > 2 AND id <= 4
              ]
            | KEEP id, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "_score"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "float"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(3, 2.0f));
            assertThat(values.get(1), contains(4, 2.0f));
        }
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public void testRankRangeQuery() {
        var query = """
            SEARCH test [
              | RANK id > 1 AND id < 4
              ]
            | KEEP id, content, _score
            | SORT id
            """;

        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "content", "_score"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "text", "float"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(2, "This is a brown dog", 2.0f));
            assertThat(values.get(1), contains(3, "This dog is really brown", 2.0f));
        }
    }

    // @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
    public void testRankRangeQueryDropScore() {
        var query = """
            SEARCH test [
              | RANK id > 2 AND id < 6
              ]
            | KEEP id, content
            | SORT id desc
            """;

        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "content"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "text"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(5, "There is also a white cat"));
            assertThat(values.get(1), contains(4, "The dog is brown but this document is very very long"));
            assertThat(values.get(2), contains(3, "This dog is really brown"));
        }
    }

    public void testRankMatch() {
        var query1 = """
            SEARCH test [
              | RANK MATCH(content, "quick brown dog")
              | LIMIT 100
              ]
            | KEEP id, _score, content
            """;   // default rank sort is score descending
        var query2 = """
            SEARCH test [
              | RANK MATCH(content, "quick") OR MATCH(content, "brown") OR MATCH(content, "dog")
              | LIMIT 100
              ]
            | KEEP id, _score, content
            """;   // default rank sort is score descending
        for (var query : List.of(query1, query2)) {
            try (var resp = run(query)) {
                logger.info("response=" + prettyResponse(resp));
                assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "_score", "content"));
                assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "float", "text"));
                // values
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), contains(6, 1.968148F, "The quick brown fox jumps over the lazy dog"));
                assertThat(values.get(1), contains(2, 0.76719964F, "This is a brown dog"));
                assertThat(values.get(2), contains(3, 0.76719964F, "This dog is really brown"));
                assertThat(values.get(3), contains(4, 0.54663825F, "The dog is brown but this document is very very long"));
                assertThat(values.get(4), contains(1, 0.27089438F, "This is a brown fox"));
            }
        }
    }

    public void testRankMatchAnd() {
        var query = """
            SEARCH test [
              | RANK MATCH(content, "quick") AND MATCH(content, "brown") AND MATCH(content, "dog")
              | LIMIT 100
              ]
            | KEEP id, _score, content
            """;   // default rank sort is score descending
        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "_score", "content"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "float", "text"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(6, 1.968148F, "The quick brown fox jumps over the lazy dog"));
        }
    }

    public void testRankMatchAndOr() {
        var query = """
            SEARCH test [
              | RANK MATCH(content, "brown") AND (MATCH(content, "fox") OR MATCH(content, "dog"))
              | LIMIT 100
              ]
            | KEEP id, _score, content
            """;   // default rank sort is score descending
        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "_score", "content"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "float", "text"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(6, 1.5159746F, "The quick brown fox jumps over the lazy dog"));
            assertThat(values.get(1), contains(1, 1.4274533F, "This is a brown fox"));
            assertThat(values.get(2), contains(2, 0.76719964F, "This is a brown dog"));
            assertThat(values.get(3), contains(3, 0.76719964F, "This dog is really brown"));
            assertThat(values.get(4), contains(4, 0.54663825F, "The dog is brown but this document is very very long"));
        }
    }

    @AwaitsFix(bugUrl = "") // TODO: all scores are 0.0 ? why? fix this
    public void testRankMatchWithPrefilter() {
        var query = """
            SEARCH test [
              | WHERE id > 2
              | RANK MATCH(content, "quick brown dog")
              | LIMIT 100
              ]
            | KEEP id, _score, content
            """;   // default rank sort is score descending

        try (var resp = run(query)) {
            logger.info("response=" + prettyResponse(resp));
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("id", "_score", "content"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("integer", "float", "text"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(6, 1.968148F, "The quick brown fox jumps over the lazy dog"));
            assertThat(values.get(1), contains(3, 0.76719964F, "This dog is really brown"));
            assertThat(values.get(2), contains(4, 0.54663825F, "The dog is brown but this document is very very long"));
        }
    }

    public void testJustExperimentingWithRestFilter() {

        // var qb = QueryBuilders.matchAllQuery();
        // var qb = QueryBuilders.termQuery("color", "blue");
        // var qb = QueryBuilders.rangeQuery("cost").gt("2.5");
        var filter = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("color", "red"))
            .must(QueryBuilders.rangeQuery("cost").gt("2.5"));
        var query = """
            SEARCH test [
              | RANK id > 1 AND id < 4
              ]
            | KEEP id, content
            | SORT id
            """;

        try (var resp = run(query, null, filter)) {
            logger.info("response=" + resp);

        }
    }

    @AwaitsFix(bugUrl = "")
    public void testSearchScoreOnNumeric() {
        var query = """
            search test [
              | score item > 1 AND item < 4
              ]
            """;
        // | keep item, cost, color, sale _score
        // | sort _score
        try (var resp = run(query)) {
            logger.info("response=" + resp);

            // todo: fix all assertions
            assertThat(resp.columns().stream().map(ColumnInfo::name).toList(), contains("item", "cost", "color", "sale"));
            assertThat(resp.columns().stream().map(ColumnInfo::type).toList(), contains("long", "double", "keyword", "date"));
            // values
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), contains(2L, 2.1d, "blue", "1992-06-01T00:00:00.000Z"));
            assertThat(values.get(1), contains(3L, 3.1d, "green", "1965-06-01T00:00:00.000Z"));
        }
    }

    private void createAndPopulateIndex(String indexName) {
        var client = client().admin().indices();
        var CreateRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "cost", "type=double", "color", "type=keyword", "content", "type=text");
        assertAcked(CreateRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }

    static String prettyResponse(EsqlQueryResponse response) {
        var maxColWidth = maxColumnWidths(response);
        StringBuilder sb = new StringBuilder();

        // headings
        var headings = response.columns().stream().map(col -> col.name() + ":" + col.type()).toList();
        var h = IntStream.range(0, response.columns().size())
            .mapToObj(colIdx -> padEnd(headings.get(colIdx), maxColWidth.get(colIdx)))
            .collect(joining("|"));
        sb.append("\n").append(h);

        // line break
        var lb = IntStream.range(0, response.columns().size())
            .mapToObj(colIdx -> "-".repeat(maxColWidth.get(colIdx)))
            .collect(joining("+"));
        sb.append("\n").append(lb);

        // row values
        response.rows().forEach(row -> {
            var values = rowValues(row);
            var s = IntStream.range(0, values.size()).mapToObj(i -> padEnd(values.get(i), maxColWidth.get(i))).collect(joining("|"));
            sb.append("\n").append(s);
        });

        return sb.toString();
    }

    static List<Integer> maxColumnWidths(EsqlQueryResponse response) {
        var headings = response.columns().stream().map(col -> col.name() + ":" + col.type()).toList();
        var maxColWidth = headings.stream().map(String::length).collect(Collectors.toCollection(ArrayList::new));
        IntStream.range(0, response.columns().size())
            .forEach(
                colIdx -> maxColWidth.set(
                    colIdx,
                    Math.max(
                        maxColWidth.get(colIdx),
                        toStream(response.column(colIdx)).map(String::valueOf).mapToInt(String::length).max().getAsInt()
                    )
                )
            );
        return maxColWidth;
    }

    static <X> Stream<X> toStream(Iterator<X> iter) {
        return Stream.iterate(iter, Iterator::hasNext, UnaryOperator.identity()).map(Iterator::next);
    }

    static String padEnd(String value, int len) {
        return value + " ".repeat(len - value.length());
    }

    static List<String> rowValues(Iterable<Object> row) {
        return toStream(row.iterator()).map(String::valueOf).toList();
    }
}

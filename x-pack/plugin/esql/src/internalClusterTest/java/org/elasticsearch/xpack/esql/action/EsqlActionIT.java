/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.analysis.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class EsqlActionIT extends AbstractEsqlIntegTestCase {

    long epoch = System.currentTimeMillis();

    @Before
    public void setupIndex() {
        createAndPopulateIndex("test");
    }

    public void testRow() {
        long value = randomLongBetween(0, Long.MAX_VALUE);
        EsqlQueryResponse response = run("row " + value);
        assertEquals(List.of(List.of(value)), response.values());
    }

    public void testFromStatsGroupingAvgWithSort() {
        testFromStatsGroupingAvgImpl("from test | stats avg(count) by data | sort data | limit 2", "data", "avg(count)");
    }

    public void testFromStatsGroupingAvg() {
        testFromStatsGroupingAvgImpl("from test | stats avg(count) by data", "data", "avg(count)");
    }

    public void testFromStatsGroupingAvgWithAliases() {
        testFromStatsGroupingAvgImpl("from test | eval g = data | stats f = avg(count) by g", "g", "f");
    }

    private void testFromStatsGroupingAvgImpl(String command, String expectedGroupName, String expectedFieldName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());

        // assert column metadata
        ColumnInfo valuesColumn = results.columns().get(0);
        assertEquals(expectedFieldName, valuesColumn.name());
        assertEquals("double", valuesColumn.type());
        ColumnInfo groupColumn = results.columns().get(1);
        assertEquals(expectedGroupName, groupColumn.name());
        assertEquals("long", groupColumn.type());

        // assert column values
        List<List<Object>> valueValues = results.values();
        assertEquals(2, valueValues.size());
        // This is loathsome, find a declarative way to assert the expected output.
        if ((long) valueValues.get(0).get(1) == 1L) {
            assertEquals(42.0, (double) valueValues.get(0).get(0), 0.0);
            assertEquals(2L, (long) valueValues.get(1).get(1));
            assertEquals(44.0, (double) valueValues.get(1).get(0), 0.0);
        } else if ((long) valueValues.get(0).get(1) == 2L) {
            assertEquals(42.0, (double) valueValues.get(1).get(0), 0.0);
            assertEquals(1L, (long) valueValues.get(1).get(1));
            assertEquals(44.0, (double) valueValues.get(0).get(0), 0.0);
        } else {
            fail("Unexpected group value: " + valueValues.get(0).get(0));
        }
    }

    public void testFromStatsGroupingCount() {
        testFromStatsGroupingCountImpl("from test | stats count(count) by data", "data", "count(count)");
    }

    public void testFromStatsGroupingCountWithAliases() {
        testFromStatsGroupingCountImpl("from test | eval grp = data | stats total = count(count) by grp", "grp", "total");
    }

    private void testFromStatsGroupingCountImpl(String command, String expectedFieldName, String expectedGroupName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());

        // assert column metadata
        ColumnInfo groupColumn = results.columns().get(0);
        assertEquals(expectedGroupName, groupColumn.name());
        assertEquals("long", groupColumn.type());
        ColumnInfo valuesColumn = results.columns().get(1);
        assertEquals(expectedFieldName, valuesColumn.name());
        assertEquals("long", valuesColumn.type());

        // assert column values
        List<List<Object>> valueValues = results.values();
        assertEquals(2, valueValues.size());
        // This is loathsome, find a declarative way to assert the expected output.
        if ((long) valueValues.get(0).get(1) == 1L) {
            assertEquals(20L, valueValues.get(0).get(0));
            assertEquals(2L, valueValues.get(1).get(1));
            assertEquals(20L, valueValues.get(1).get(0));
        } else if ((long) valueValues.get(0).get(1) == 2L) {
            assertEquals(20L, valueValues.get(1).get(0));
            assertEquals(1L, valueValues.get(1).get(1));
            assertEquals(20L, valueValues.get(0).get(0));
        } else {
            fail("Unexpected group value: " + valueValues.get(0).get(1));
        }
    }

    // Grouping where the groupby field is of a date type.
    public void testFromStatsGroupingByDate() {
        EsqlQueryResponse results = run("from test | stats avg(count) by time");
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());
        Assert.assertEquals(40, results.values().size());

        // assert column metadata
        assertEquals("avg(count)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals("time", results.columns().get(1).name());
        assertEquals("long", results.columns().get(1).type());

        // assert column values
        List<Long> expectedValues = LongStream.range(0, 40).map(i -> epoch + i).sorted().boxed().toList();
        List<Long> actualValues = IntStream.range(0, 40).mapToLong(i -> (Long) results.values().get(i).get(1)).sorted().boxed().toList();
        assertEquals(expectedValues, actualValues);
    }

    public void testFromGroupingByNumericFieldWithNulls() {
        for (int i = 0; i < 5; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("no_count_old_" + i).source("data", between(1, 2), "data_d", 1d))
                .add(new IndexRequest("test").id("no_count_new_" + i).source("data", 99, "data_d", 1d))
                .add(new IndexRequest("test").id("no_data_" + i).source("count", between(0, 100), "count_d", between(0, 100)))
                .get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("test").get();
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        EsqlQueryResponse results = run("from test | stats avg(count) by data | sort data");
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());
        Assert.assertEquals(3, results.values().size());

        // assert column metadata
        assertEquals("avg(count)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals("data", results.columns().get(1).name());
        assertEquals("long", results.columns().get(1).type());

        record Group(Long data, Double avg) {

        }

        List<Group> expectedGroups = List.of(new Group(1L, 42.0), new Group(2L, 44.0), new Group(99L, null));

        // assert column values
        List<Group> actualGroups = results.values()
            .stream()
            .map(l -> new Group((Long) l.get(1), (Double) l.get(0)))
            .sorted(comparing(c -> c.data))
            .toList();
        assertEquals(expectedGroups, actualGroups);
        for (int i = 0; i < 5; i++) {
            client().prepareBulk()
                .add(new DeleteRequest("test").id("no_color_" + i))
                .add(new DeleteRequest("test").id("no_count_red_" + i))
                .add(new DeleteRequest("test").id("no_count_yellow_" + i))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
    }

    public void testFromStatsGroupingByKeyword() {
        EsqlQueryResponse results = run("from test | stats avg(count) by color");
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());
        Assert.assertEquals(3, results.values().size());

        // assert column metadata
        assertEquals("avg(count)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals("color", results.columns().get(1).name());
        assertEquals("keyword", results.columns().get(1).type());
        record Group(String color, double avg) {

        }
        List<Group> expectedGroups = List.of(new Group("blue", 42.0), new Group("green", 44.0), new Group("red", 43));
        List<Group> actualGroups = results.values()
            .stream()
            .map(l -> new Group((String) l.get(1), (Double) l.get(0)))
            .sorted(comparing(c -> c.color))
            .toList();
        assertThat(actualGroups, equalTo(expectedGroups));
    }

    public void testFromStatsGroupingByKeywordWithNulls() {
        for (int i = 0; i < 5; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("no_color_" + i).source("data", 12, "count", 120, "data_d", 2d, "count_d", 120d))
                .add(new IndexRequest("test").id("no_count_red_" + i).source("data", 2, "data_d", 2d, "color", "red"))
                .add(new IndexRequest("test").id("no_count_yellow_" + i).source("data", 2, "data_d", 2d, "color", "yellow"))
                .get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("test").get();
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        for (String field : List.of("count", "count_d")) {
            EsqlQueryResponse results = run("from test | stats avg = avg(" + field + ") by color");
            logger.info(results);
            Assert.assertEquals(2, results.columns().size());
            Assert.assertEquals(4, results.values().size());

            // assert column metadata
            assertEquals("avg", results.columns().get(0).name());
            assertEquals("double", results.columns().get(0).type());
            assertEquals("color", results.columns().get(1).name());
            assertEquals("keyword", results.columns().get(1).type());
            record Group(String color, Double avg) {

            }
            List<Group> expectedGroups = List.of(
                new Group("blue", 42.0),
                new Group("green", 44.0),
                new Group("red", 43.0),
                new Group("yellow", null)
            );
            List<Group> actualGroups = results.values()
                .stream()
                .map(l -> new Group((String) l.get(1), (Double) l.get(0)))
                .sorted(comparing(c -> c.color))
                .toList();
            assertThat(actualGroups, equalTo(expectedGroups));
        }
        for (int i = 0; i < 5; i++) {
            client().prepareBulk()
                .add(new DeleteRequest("test").id("no_color_" + i))
                .add(new DeleteRequest("test").id("no_count_red_" + i))
                .add(new DeleteRequest("test").id("no_count_yellow_" + i))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
    }

    public void testFromStatsMultipleAggs() {
        EsqlQueryResponse results = run(
            "from test | stats a=avg(count), mi=min(count), ma=max(count), s=sum(count), c=count(count) by color"
        );
        logger.info(results);
        Assert.assertEquals(6, results.columns().size());
        Assert.assertEquals(3, results.values().size());

        // assert column metadata
        assertEquals("a", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals("mi", results.columns().get(1).name());
        assertEquals("long", results.columns().get(1).type());
        assertEquals("ma", results.columns().get(2).name());
        assertEquals("long", results.columns().get(2).type());
        assertEquals("s", results.columns().get(3).name());
        assertEquals("long", results.columns().get(3).type());
        assertEquals("c", results.columns().get(4).name());
        assertEquals("long", results.columns().get(4).type());
        assertEquals("color", results.columns().get(5).name());
        assertEquals("keyword", results.columns().get(5).type());
        record Group(double avg, long mi, long ma, long s, long c, String color) {}
        List<Group> expectedGroups = List.of(
            new Group(42, 42, 42, 420, 10, "blue"),
            new Group(44, 44, 44, 440, 10, "green"),
            new Group(43, 40, 46, 860, 20, "red")
        );
        // TODO: each aggregator returns Double now, it should in fact mirror the data type of the fields it's aggregating
        List<Group> actualGroups = results.values()
            .stream()
            .map(l -> new Group((Double) l.get(0), (Long) l.get(1), (Long) l.get(2), (Long) l.get(3), (Long) l.get(4), (String) l.get(5)))
            .sorted(comparing(c -> c.color))
            .toList();
        assertThat(actualGroups, equalTo(expectedGroups));
    }

    public void testFromSortWithTieBreakerLimit() {
        EsqlQueryResponse results = run("from test | sort data, count desc, time | limit 5 | keep data, count, time");
        logger.info(results);
        assertThat(
            results.values(),
            contains(
                List.of(1L, 44L, epoch + 2),
                List.of(1L, 44L, epoch + 6),
                List.of(1L, 44L, epoch + 10),
                List.of(1L, 44L, epoch + 14),
                List.of(1L, 44L, epoch + 18)
            )
        );
    }

    public void testFromStatsProjectGroup() {
        EsqlQueryResponse results = run("from test | stats avg_count = avg(count) by data | keep data");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("data"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("long"));
        assertThat(results.values(), containsInAnyOrder(List.of(1L), List.of(2L)));
    }

    public void testRowStatsProjectGroupByInt() {
        EsqlQueryResponse results = run("row a = 1, b = 2 | stats count(b) by a | keep a");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("integer"));
        assertThat(results.values(), contains(List.of(1)));
    }

    public void testRowStatsProjectGroupByLong() {
        EsqlQueryResponse results = run("row a = 1000000000000, b = 2 | stats count(b) by a | keep a");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("long"));
        assertThat(results.values(), contains(List.of(1000000000000L)));
    }

    public void testRowStatsProjectGroupByDouble() {
        EsqlQueryResponse results = run("row a = 1.0, b = 2 | stats count(b) by a | keep a");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double"));
        assertThat(results.values(), contains(List.of(1.0)));
    }

    public void testRowStatsProjectGroupByKeyword() {
        EsqlQueryResponse results = run("row a = \"hello\", b = 2 | stats count(b) by a | keep a");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("keyword"));
        assertThat(results.values(), contains(List.of("hello")));
    }

    public void testFromStatsProjectGroupByDouble() {
        EsqlQueryResponse results = run("from test | stats count(count) by data_d | keep data_d");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("data_d"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double"));
        assertThat(results.values(), containsInAnyOrder(List.of(1.0), List.of(2.0)));
    }

    public void testFromStatsProjectGroupWithAlias() {
        String query = "from test | stats avg_count = avg(count) by data | eval d2 = data | rename d = data | keep d, d2";
        EsqlQueryResponse results = run(query);
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("d", "d2"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("long", "long"));
        assertThat(results.values(), containsInAnyOrder(List.of(1L, 1L), List.of(2L, 2L)));
    }

    public void testFromStatsProjectAgg() {
        EsqlQueryResponse results = run("from test | stats a = avg(count) by data | keep a");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double"));
        assertThat(results.values(), containsInAnyOrder(List.of(42d), List.of(44d)));
    }

    public void testFromStatsProjectAggWithAlias() {
        EsqlQueryResponse results = run("from test | stats a = avg(count) by data | rename b = a | keep b");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("b"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double"));
        assertThat(results.values(), containsInAnyOrder(List.of(42d), List.of(44d)));
    }

    public void testFromProjectStatsGroupByAlias() {
        EsqlQueryResponse results = run("from test | rename d = data | keep d, count | stats avg(count) by d");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("avg(count)", "d"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double", "long"));
        assertThat(results.values(), containsInAnyOrder(List.of(42d, 1L), List.of(44d, 2L)));
    }

    public void testFromProjectStatsAggregateAlias() {
        EsqlQueryResponse results = run("from test | rename c = count | keep c, data | stats avg(c) by data");
        logger.info(results);
        assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("avg(c)", "data"));
        assertThat(results.columns().stream().map(ColumnInfo::type).toList(), contains("double", "long"));
        assertThat(results.values(), containsInAnyOrder(List.of(42d, 1L), List.of(44d, 2L)));
    }

    public void testFromEvalStats() {
        EsqlQueryResponse results = run("from test | eval ratio = data_d / count_d | stats avg(ratio)");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("avg(ratio)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(0.034d, (double) results.values().get(0).get(0), 0.001d);
    }

    public void testFromStatsEvalWithPragma() {
        assumeTrue("pragmas only enabled on snapshot builds", Build.CURRENT.isSnapshot());
        EsqlQueryResponse results = run("from test | stats avg_count = avg(count) | eval x = avg_count + 7");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(2, results.values().get(0).size());
        assertEquals(50, (double) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("x", "double"))), 1d);
        assertEquals(43, (double) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("avg_count", "double"))), 1d);
    }

    public void testWhere() {
        EsqlQueryResponse results = run("from test | where count > 40");
        logger.info(results);
        Assert.assertEquals(30, results.values().size());
        var countIndex = results.columns().indexOf(new ColumnInfo("count", "long"));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(countIndex), greaterThan(40L));
        }
    }

    public void testProjectWhere() {
        EsqlQueryResponse results = run("from test | keep count | where count > 40");
        logger.info(results);
        Assert.assertEquals(30, results.values().size());
        int countIndex = results.columns().indexOf(new ColumnInfo("count", "long"));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(countIndex), greaterThan(40L));
        }
    }

    public void testEvalWhere() {
        EsqlQueryResponse results = run("from test | eval x = count / 2 | where x > 20");
        logger.info(results);
        Assert.assertEquals(30, results.values().size());
        int countIndex = results.columns().indexOf(new ColumnInfo("x", "long"));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(countIndex), greaterThan(20L));
        }
    }

    public void testFilterWithNullAndEval() {
        EsqlQueryResponse results = run("row a = 1 | eval b = a + null | where b > 1");
        logger.info(results);
        Assert.assertEquals(0, results.values().size());
    }

    public void testStringLength() {
        EsqlQueryResponse results = run("from test | eval l = length(color)");
        logger.info(results);
        assertThat(results.values(), hasSize(40));
        int countIndex = results.columns().indexOf(new ColumnInfo("l", "integer"));
        for (List<Object> values : results.values()) {
            assertThat((Integer) values.get(countIndex), greaterThanOrEqualTo(3));
        }
    }

    public void testFilterWithNullAndEvalFromIndex() {
        // append entry, with an absent count, to the index
        client().prepareBulk().add(new IndexRequest("test").id("no_count").source("data", 12, "data_d", 2d, "color", "red")).get();

        client().admin().indices().prepareRefresh("test").get();
        // sanity
        EsqlQueryResponse results = run("from test");
        Assert.assertEquals(41, results.values().size());

        results = run("from test | eval newCount = count + 1 | where newCount > 1");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("count", "long"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("count_d", "double"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("data", "long"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("data_d", "double"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("time", "long"))));

        // restore index to original pre-test state
        client().prepareBulk().add(new DeleteRequest("test").id("no_count")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        results = run("from test");
        Assert.assertEquals(40, results.values().size());
    }

    public void testMultiConditionalWhere() {
        EsqlQueryResponse results = run(
            "from test | eval abc = 1+2 | where (abc + count >= 44 or data_d == 2) and data == 1 | keep color, abc"
        );
        logger.info(results);
        Assert.assertEquals(10, results.values().size());
        Assert.assertEquals(2, results.columns().size());
        for (List<Object> values : results.values()) {
            assertThat((String) values.get(0), equalTo("green"));
            assertThat((Integer) values.get(1), equalTo(3));
        }
    }

    public void testWhereNegatedCondition() {
        EsqlQueryResponse results = run("from test | eval abc=1+2 | where abc + count > 45 and data != 1 | keep color, data");
        logger.info(results);
        Assert.assertEquals(10, results.values().size());
        Assert.assertEquals(2, results.columns().size());
        for (List<Object> values : results.values()) {
            assertThat((String) values.get(0), equalTo("red"));
            assertThat((Long) values.get(1), equalTo(2L));
        }
    }

    public void testEvalOverride() {
        EsqlQueryResponse results = run("from test | eval count = count + 1 | eval count = count + 1");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        Assert.assertEquals(1, results.columns().stream().filter(c -> c.name().equals("count")).count());
        int countIndex = results.columns().size() - 1;
        Assert.assertEquals(new ColumnInfo("count", "long"), results.columns().get(countIndex));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(countIndex), greaterThanOrEqualTo(42L));
        }
    }

    public void testProjectRename() {
        EsqlQueryResponse results = run("from test | eval y = count | rename x = count | keep x, y");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), contains(new ColumnInfo("x", "long"), new ColumnInfo("y", "long")));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
            assertThat(values.get(1), is(values.get(0)));
        }
    }

    public void testProjectRenameEval() {
        EsqlQueryResponse results = run("from test | eval y = count | rename x = count | keep x, y | eval x2 = x + 1 | eval y2 = y + 2");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(
            results.columns(),
            contains(new ColumnInfo("x", "long"), new ColumnInfo("y", "long"), new ColumnInfo("x2", "long"), new ColumnInfo("y2", "long"))
        );
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
            assertThat(values.get(1), is(values.get(0)));
            assertThat(values.get(2), is(((Long) values.get(0)) + 1));
            assertThat(values.get(3), is(((Long) values.get(0)) + 2));
        }
    }

    public void testProjectRenameEvalProject() {
        EsqlQueryResponse results = run("from test | eval y = count | rename x = count | keep x, y | eval z = x + y | keep x, y, z");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), contains(new ColumnInfo("x", "long"), new ColumnInfo("y", "long"), new ColumnInfo("z", "long")));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
            assertThat(values.get(1), is(values.get(0)));
            assertThat(values.get(2), is((Long) values.get(0) * 2));
        }
    }

    public void testProjectOverride() {
        EsqlQueryResponse results = run("from test | eval cnt = count | rename data = count | keep cnt, data");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), contains(new ColumnInfo("cnt", "long"), new ColumnInfo("data", "long")));
        for (List<Object> values : results.values()) {
            assertThat(values.get(1), is(values.get(0)));
        }
    }

    public void testRefreshSearchIdleShards() throws Exception {
        String indexName = "test_refresh";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .get()
        );
        ensureYellow(indexName);
        Index index = resolveIndex(indexName);
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(new IndexRequest(indexName).id("1" + i).source("data", 1, "count", 42))
                .add(new IndexRequest(indexName).id("2" + i).source("data", 2, "count", 44))
                .get();
        }
        logger.info("--> waiting for shards to have pending refresh");
        assertBusy(() -> {
            int pendingRefreshes = 0;
            for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
                IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    for (IndexShard shard : indexService) {
                        if (shard.hasRefreshPending()) {
                            pendingRefreshes++;
                        }
                    }
                }
            }
            assertThat("shards don't have any pending refresh", pendingRefreshes, greaterThan(0));
        }, 30, TimeUnit.SECONDS);
        EsqlQueryResponse results = run("from test_refresh");
        logger.info(results);
        Assert.assertEquals(20, results.values().size());
    }

    public void testESFilter() throws Exception {
        String indexName = "test_filter";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .get()
        );
        ensureYellow(indexName);
        int numDocs = randomIntBetween(1, 5000);
        Map<String, Long> docs = new HashMap<>();
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "id-" + i;
            long value = randomLongBetween(-100_000, 100_000);
            docs.put(id, value);
            indexRequests.add(client().prepareIndex().setIndex(indexName).setId(id).setSource(Map.of("val", value)));
        }
        indexRandom(true, randomBoolean(), indexRequests);
        String command = "from test_filter | stats avg = avg(val)";
        long from = randomBoolean() ? Long.MIN_VALUE : randomLongBetween(-1000, 1000);
        long to = randomBoolean() ? Long.MAX_VALUE : randomLongBetween(from, from + 1000);
        QueryBuilder filter = new RangeQueryBuilder("val").from(from, true).to(to, true);
        EsqlQueryResponse results = new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(command)
            .filter(filter)
            .pragmas(randomPragmas())
            .get();
        logger.info(results);
        OptionalDouble avg = docs.values().stream().filter(v -> from <= v && v <= to).mapToLong(n -> n).average();
        if (avg.isPresent()) {
            assertEquals(avg.getAsDouble(), (double) results.values().get(0).get(0), 0.01d);
        } else {
            assertThat(results.values().get(0).get(0), nullValue());
        }
    }

    public void testExtractFields() throws Exception {
        String indexName = "test_extract_fields";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .setMapping("val", "type=long", "tag", "type=keyword")
                .get()
        );
        int numDocs = randomIntBetween(1, 100);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        record Doc(long val, String tag) {

        }
        List<Doc> allDocs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Doc d = new Doc(i, "tag-" + randomIntBetween(1, 100));
            allDocs.add(d);
            indexRequests.add(
                client().prepareIndex().setIndex(indexName).setId(Integer.toString(i)).setSource(Map.of("val", d.val, "tag", d.tag))
            );
        }
        indexRandom(true, randomBoolean(), indexRequests);
        int limit = randomIntBetween(1, 10);
        String command = "from test_extract_fields | sort val | limit " + limit;
        EsqlQueryResponse results = run(command);
        logger.info(results);
        // _doc, _segment, _shard are pruned
        assertThat(results.columns().size(), equalTo(2));
        assertThat(results.values(), hasSize(Math.min(limit, numDocs)));
        assertThat(results.columns().get(1).name(), equalTo("val"));
        assertThat(results.columns().get(0).name(), equalTo("tag"));
        List<Doc> actualDocs = new ArrayList<>();
        for (int i = 0; i < results.values().size(); i++) {
            List<Object> values = results.values().get(i);
            actualDocs.add(new Doc((Long) values.get(1), (String) values.get(0)));
        }
        assertThat(actualDocs, equalTo(allDocs.stream().limit(limit).toList()));
    }

    public void testEvalWithNullAndAvg() {
        EsqlQueryResponse results = run("from test | eval nullsum = count_d + null | stats avg(nullsum)");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("avg(nullsum)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertNull(results.values().get(0).get(0));
    }

    public void testFromStatsLimit() {
        EsqlQueryResponse results = run("from test | stats ac = avg(count) by data | limit 1");
        logger.info(results);
        assertThat(results.columns(), contains(new ColumnInfo("ac", "double"), new ColumnInfo("data", "long")));
        assertThat(results.values(), contains(anyOf(contains(42.0, 1L), contains(44.0, 2L))));
    }

    public void testFromLimit() {
        EsqlQueryResponse results = run("from test | keep data | limit 2");
        logger.info(results);
        assertThat(results.columns(), contains(new ColumnInfo("data", "long")));
        assertThat(results.values(), contains(anyOf(contains(1L), contains(2L)), anyOf(contains(1L), contains(2L))));
    }

    public void testDropAllColumns() {
        EsqlQueryResponse results = run("from test | keep data | drop data | eval a = 1");
        logger.info(results);
        assertThat(results.columns(), hasSize(1));
        assertThat(results.columns(), contains(new ColumnInfo("a", "integer")));
        assertThat(results.values(), is(empty()));
    }

    public void testDropAllColumnsWithStats() {
        EsqlQueryResponse results = run("from test | stats g = count(data) | drop g");
        logger.info(results);
        assertThat(results.columns(), is(empty()));
        assertThat(results.values(), is(empty()));
    }

    public void testIndexPatterns() throws Exception {
        String[] indexNames = { "test_index_patterns_1", "test_index_patterns_2", "test_index_patterns_3" };
        int i = 0;
        for (String indexName : indexNames) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                    .setMapping("data", "type=long", "count", "type=long")
                    .get()
            );
            ensureYellow(indexName);
            client().prepareBulk()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .add(new IndexRequest(indexName).id("1").source("data", ++i, "count", i * 1000))
                .add(new IndexRequest(indexName).id("2").source("data", ++i, "count", i * 1000))
                .add(new IndexRequest(indexName).id("3").source("data", ++i, "count", i * 1000))
                .add(new IndexRequest(indexName).id("4").source("data", ++i, "count", i * 1000))
                .add(new IndexRequest(indexName).id("5").source("data", ++i, "count", i * 1000))
                .get();
        }

        EsqlQueryResponse results = run("from test_index_patterns* | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(15L, results.values().get(0).get(0));
        assertEquals(120000L, results.values().get(0).get(1));

        results = run("from test_index_patterns_1,test_index_patterns_2 | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(10L, results.values().get(0).get(0));
        assertEquals(55000L, results.values().get(0).get(1));

        results = run("from test_index_patterns_1*,test_index_patterns_2* | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(10L, results.values().get(0).get(0));
        assertEquals(55000L, results.values().get(0).get(1));

        results = run("from test_index_patterns_*,-test_index_patterns_1 | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(10L, results.values().get(0).get(0));
        assertEquals(105000L, results.values().get(0).get(1));

        results = run("from * | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(55L, results.values().get(0).get(0));
        assertEquals(121720L, results.values().get(0).get(1));

        results = run("from test_index_patterns_2 | stats count(data), sum(count)");
        assertEquals(1, results.values().size());
        assertEquals(5L, results.values().get(0).get(0));
        assertEquals(40000L, results.values().get(0).get(1));
    }

    public void testOverlappingIndexPatterns() throws Exception {
        String[] indexNames = { "test_overlapping_index_patterns_1", "test_overlapping_index_patterns_2" };

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test_overlapping_index_patterns_1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .setMapping("field", "type=long")
                .get()
        );
        ensureYellow("test_overlapping_index_patterns_1");
        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest("test_overlapping_index_patterns_1").id("1").source("field", 10))
            .get();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test_overlapping_index_patterns_2")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .setMapping("field", "type=keyword")
                .get()
        );
        ensureYellow("test_overlapping_index_patterns_2");
        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest("test_overlapping_index_patterns_2").id("1").source("field", "foo"))
            .get();

        expectThrows(VerificationException.class, () -> run("from test_overlapping_index_patterns_* | sort field"));
    }

    public void testEmptyIndex() {
        assertAcked(client().admin().indices().prepareCreate("test_empty").setMapping("k", "type=keyword", "v", "type=long").get());
        EsqlQueryResponse results = run("from test_empty");
        assertThat(results.columns(), equalTo(List.of(new ColumnInfo("k", "keyword"), new ColumnInfo("v", "long"))));
        assertThat(results.values(), empty());
    }

    public void testShowInfo() {
        EsqlQueryResponse results = run("show info");
        assertThat(
            results.columns(),
            equalTo(List.of(new ColumnInfo("version", "keyword"), new ColumnInfo("date", "keyword"), new ColumnInfo("hash", "keyword")))
        );
        assertThat(results.values().size(), equalTo(1));
        assertThat(results.values().get(0).get(0), equalTo(Build.CURRENT.version()));
        assertThat(results.values().get(0).get(1), equalTo(Build.CURRENT.date()));
        assertThat(results.values().get(0).get(2), equalTo(Build.CURRENT.hash()));
    }

    public void testShowFunctions() {
        EsqlQueryResponse results = run("show functions");
        assertThat(results.columns(), equalTo(List.of(new ColumnInfo("name", "keyword"), new ColumnInfo("synopsis", "keyword"))));
        assertThat(results.values().size(), equalTo(new EsqlFunctionRegistry().listFunctions().size()));
    }

    public void testInWithNullValue() {
        EsqlQueryResponse results = run("from test | where null in (data, 2) | keep data");
        assertThat(results.columns(), equalTo(List.of(new ColumnInfo("data", "long"))));
        assertThat(results.values().size(), equalTo(0));
    }

    public void testTopNPushedToLucene() {
        BulkRequestBuilder bulkDelete = client().prepareBulk();
        bulkDelete.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 5; i < 11; i++) {
            var yellowDocId = "yellow_" + i;
            var yellowNullCountDocId = "yellow_null_count_" + i;
            var yellowNullDataDocId = "yellow_null_data_" + i;

            client().prepareBulk()
                .add(new IndexRequest("test").id(yellowDocId).source("data", i, "count", i * 10, "color", "yellow"))
                .add(new IndexRequest("test").id(yellowNullCountDocId).source("data", i, "color", "yellow"))
                .add(new IndexRequest("test").id(yellowNullDataDocId).source("count", i * 10, "color", "yellow"))
                .get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("test").get();
            }

            // build the cleanup request now, as well, not to miss anything ;-)
            bulkDelete.add(new DeleteRequest("test").id(yellowDocId))
                .add(new DeleteRequest("test").id(yellowNullCountDocId))
                .add(new DeleteRequest("test").id(yellowNullDataDocId));
        }
        client().admin().indices().prepareRefresh("test").get();

        EsqlQueryResponse results = run("""
                from test
                | where color == "yellow"
                | sort data desc nulls first, count asc nulls first
                | limit 10
                | keep data, count, color
            """);
        logger.info(results);
        Assert.assertEquals(3, results.columns().size());
        Assert.assertEquals(10, results.values().size());

        // assert column metadata
        assertEquals("data", results.columns().get(0).name());
        assertEquals("long", results.columns().get(0).type());
        assertEquals("count", results.columns().get(1).name());
        assertEquals("long", results.columns().get(1).type());
        assertEquals("color", results.columns().get(2).name());
        assertEquals("keyword", results.columns().get(2).type());
        record Group(Long data, Long count, String color) {
            Group(Long data, Long count) {
                this(data, count, "yellow");
            }
        }
        List<Group> expectedGroups = List.of(
            // data sorted descending nulls first; count sorted ascending nulls first
            new Group(null, 50L),
            new Group(null, 60L),
            new Group(null, 70L),
            new Group(null, 80L),
            new Group(null, 90L),
            new Group(null, 100L),
            new Group(10L, null),
            new Group(10L, 100L),
            new Group(9L, null),
            new Group(9L, 90L)
        );
        List<Group> actualGroups = results.values()
            .stream()
            .map(l -> new Group((Long) l.get(0), (Long) l.get(1), (String) l.get(2)))
            .toList();
        assertThat(actualGroups, equalTo(expectedGroups));

        // clean-up what we created
        bulkDelete.get();
    }

    /**
     * This test covers the scenarios where Lucene is throwing a {@link org.apache.lucene.search.CollectionTerminatedException} when
     * it's signaling that it could stop collecting hits early. For example, in the case the index is sorted in the same order as the query.
     * The {@link org.elasticsearch.compute.lucene.LuceneTopNSourceOperator#getOutput()} is handling this exception by
     * ignoring it (which is the right thing to do) and sort of cleaning up and moving to the next docs collection.
     */
    public void testTopNPushedToLuceneOnSortedIndex() {
        var sortOrder = randomFrom("asc", "desc");
        createAndPopulateIndex(
            "sorted_test_index",
            Settings.builder().put("index.sort.field", "time").put("index.sort.order", sortOrder).build()
        );

        int limit = randomIntBetween(1, 5);
        EsqlQueryResponse results = run("from sorted_test_index | sort time " + sortOrder + " | limit " + limit + " | keep time");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(limit, results.values().size());

        // assert column metadata
        assertEquals("time", results.columns().get(0).name());
        assertEquals("long", results.columns().get(0).type());

        boolean sortedDesc = "desc".equals(sortOrder);
        var expected = LongStream.range(0, 40)
            .map(i -> epoch + i)
            .boxed()
            .sorted(sortedDesc ? reverseOrder() : naturalOrder())
            .limit(limit)
            .toList();
        var actual = results.values().stream().map(l -> (Long) l.get(0)).toList();
        assertThat(actual, equalTo(expected));

        // clean-up
        client().admin().indices().delete(new DeleteIndexRequest("sorted_test_index")).actionGet();
    }

    /*
     * Create two indices that both have nested documents in them. Create an alias pointing to the two indices.
     * Query an individual index, then query the alias checking that no nested documents are returned.
     */
    public void testReturnNoNestedDocuments() throws IOException, ExecutionException, InterruptedException {
        var indexName1 = "test_nested_docs_1";
        var indexName2 = "test_nested_docs_2";
        var indices = List.of(indexName1, indexName2);
        var alias = "test-alias";
        int docsCount = randomIntBetween(50, 100);
        int[] countValuesGreaterThanFifty = new int[indices.size()];

        for (int i = 0; i < indices.size(); i++) {
            String indexName = indices.get(i);
            createNestedMappingIndex(indexName);
            countValuesGreaterThanFifty[i] = indexDocsIntoNestedMappingIndex(indexName, docsCount);
        }
        createAlias(indices, alias);

        var indexToTest = randomIntBetween(0, indices.size() - 1);
        var indexNameToTest = indices.get(indexToTest);
        // simple query
        assertNoNestedDocuments("from " + indexNameToTest, docsCount, 0L, 100L);
        // simple query with filter that gets pushed to ES
        assertNoNestedDocuments("from " + indexNameToTest + " | where data >= 50", countValuesGreaterThanFifty[indexToTest], 50L, 100L);
        // simple query against alias
        assertNoNestedDocuments("from " + alias, docsCount * 2, 0L, 100L);
        // simple query against alias with filter that gets pushed to ES
        assertNoNestedDocuments("from " + alias + " | where data >= 50", Arrays.stream(countValuesGreaterThanFifty).sum(), 50L, 100L);
    }

    private void createNestedMappingIndex(String indexName) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("nested");
                {
                    builder.field("type", "nested");
                    builder.startObject("properties");
                    {
                        builder.startObject("foo");
                        builder.field("type", "long");
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                builder.startObject("data");
                builder.field("type", "long");
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 3)))
                .setMapping(builder)
                .get()
        );
    }

    private int indexDocsIntoNestedMappingIndex(String indexName, int docsCount) throws IOException {
        int countValuesGreaterThanFifty = 0;
        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        for (int j = 0; j < docsCount; j++) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            int randomValue = randomIntBetween(0, 100);
            countValuesGreaterThanFifty += randomValue >= 50 ? 1 : 0;
            builder.startObject();
            {
                builder.field("data", randomValue);
                builder.startArray("nested");
                {
                    for (int k = 0, max = randomIntBetween(1, 5); k < max; k++) {
                        // nested values are all greater than any non-nested values found in the "data" long field
                        builder.startObject().field("foo", randomIntBetween(1000, 10000)).endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
            bulkBuilder.add(new IndexRequest(indexName).id(Integer.toString(j)).source(builder));
        }
        bulkBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow(indexName);

        return countValuesGreaterThanFifty;
    }

    private void createAlias(List<String> indices, String alias) throws InterruptedException, ExecutionException {
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        for (String indexName : indices) {
            aliasesRequest.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(indexName).alias(alias));
        }
        assertAcked(admin().indices().aliases(aliasesRequest).get());
    }

    private void assertNoNestedDocuments(String query, int docsCount, long minValue, long maxValue) {
        EsqlQueryResponse results = run(query);
        assertThat(results.columns(), contains(new ColumnInfo("data", "long")));
        assertThat(results.columns().size(), is(1));
        assertThat(results.values().size(), is(docsCount));
        for (List<Object> row : results.values()) {
            assertThat(row.size(), is(1));
            // check that all the values returned are the regular ones
            assertThat((Long) row.get(0), allOf(greaterThanOrEqualTo(minValue), lessThanOrEqualTo(maxValue)));
        }
    }

    private void createAndPopulateIndex(String indexName) {
        createAndPopulateIndex(indexName, Settings.EMPTY);
    }

    private void createAndPopulateIndex(String indexName, Settings additionalSettings) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put(additionalSettings).put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)))
                .setMapping(
                    "data",
                    "type=long",
                    "data_d",
                    "type=double",
                    "count",
                    "type=long",
                    "count_d",
                    "type=double",
                    "time",
                    "type=long",
                    "color",
                    "type=keyword"
                )
                .get()
        );
        long timestamp = epoch;
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(
                    new IndexRequest(indexName).id("1" + i)
                        .source("data", 1, "count", 40, "data_d", 1d, "count_d", 40d, "time", timestamp++, "color", "red")
                )
                .add(
                    new IndexRequest(indexName).id("2" + i)
                        .source("data", 2, "count", 42, "data_d", 2d, "count_d", 42d, "time", timestamp++, "color", "blue")
                )
                .add(
                    new IndexRequest(indexName).id("3" + i)
                        .source("data", 1, "count", 44, "data_d", 1d, "count_d", 44d, "time", timestamp++, "color", "green")
                )
                .add(
                    new IndexRequest(indexName).id("4" + i)
                        .source("data", 2, "count", 46, "data_d", 2d, "count_d", 46d, "time", timestamp++, "color", "red")
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow(indexName);
    }
}

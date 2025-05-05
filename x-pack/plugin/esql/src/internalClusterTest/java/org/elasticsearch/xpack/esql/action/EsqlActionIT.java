/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;
import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // TODO: Allow relocation once we have retry in ESQL (see #103081)
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("cluster.routing.rebalance.enable", "none")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(DataStreamsPlugin.class, MapperExtrasPlugin.class)).toList();
    }

    public void testProjectConstant() {
        try (EsqlQueryResponse results = run("from test | eval x = 1 | keep x")) {
            assertThat(results.columns(), equalTo(List.of(new ColumnInfoImpl("x", "integer", null))));
            assertThat(getValuesList(results).size(), equalTo(40));
            assertThat(getValuesList(results).get(0).get(0), equalTo(1));
        }
    }

    public void testStatsOverConstant() {
        try (EsqlQueryResponse results = run("from test | eval x = 1 | stats x = count(x)")) {
            assertThat(results.columns(), equalTo(List.of(new ColumnInfoImpl("x", "long", null))));
            assertThat(getValuesList(results).size(), equalTo(1));
            assertThat(getValuesList(results).get(0).get(0), equalTo(40L));
        }
    }

    public void testRow() {
        long value = randomLongBetween(0, Long.MAX_VALUE);
        try (EsqlQueryResponse response = run("row " + value)) {
            assertEquals(List.of(List.of(value)), getValuesList(response));
        }
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
        try (EsqlQueryResponse results = run(command)) {
            logger.info(results);
            assertEquals(2, results.columns().size());

            // assert column metadata
            ColumnInfoImpl valuesColumn = results.columns().get(0);
            assertEquals(expectedFieldName, valuesColumn.name());
            assertEquals(DataType.DOUBLE, valuesColumn.type());
            ColumnInfoImpl groupColumn = results.columns().get(1);
            assertEquals(expectedGroupName, groupColumn.name());
            assertEquals(DataType.LONG, groupColumn.type());

            // assert column values
            List<List<Object>> valueValues = getValuesList(results);
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
    }

    public void testFromStatsGroupingCount() {
        testFromStatsGroupingCountImpl("from test | stats count(count) by data", "data", "count(count)");
    }

    public void testFromStatsGroupingCountWithAliases() {
        testFromStatsGroupingCountImpl("from test | eval grp = data | stats total = count(count) by grp", "grp", "total");
    }

    private void testFromStatsGroupingCountImpl(String command, String expectedFieldName, String expectedGroupName) {
        try (EsqlQueryResponse results = run(command)) {
            logger.info(results);
            assertEquals(2, results.columns().size());

            // assert column metadata
            ColumnInfoImpl groupColumn = results.columns().get(0);
            assertEquals(expectedGroupName, groupColumn.name());
            assertEquals(DataType.LONG, groupColumn.type());
            ColumnInfoImpl valuesColumn = results.columns().get(1);
            assertEquals(expectedFieldName, valuesColumn.name());
            assertEquals(DataType.LONG, valuesColumn.type());

            // assert column values
            List<List<Object>> valueValues = getValuesList(results);
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
    }

    // Grouping where the groupby field is of a date type.
    public void testFromStatsGroupingByDate() {
        try (EsqlQueryResponse results = run("from test | stats avg(count) by time")) {
            logger.info(results);
            assertEquals(2, results.columns().size());
            assertEquals(40, getValuesList(results).size());

            // assert column metadata
            assertEquals("avg(count)", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals("time", results.columns().get(1).name());
            assertEquals(DataType.LONG, results.columns().get(1).type());

            // assert column values
            List<Long> expectedValues = LongStream.range(0, 40).map(i -> epoch + i).sorted().boxed().toList();
            List<Long> actualValues = IntStream.range(0, 40)
                .mapToLong(i -> (Long) getValuesList(results).get(i).get(1))
                .sorted()
                .boxed()
                .toList();
            assertEquals(expectedValues, actualValues);
        }
    }

    public void testFromGroupingByNumericFieldWithNulls() {
        for (int i = 0; i < 5; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("no_count_old_" + i).source("data", between(1, 2), "data_d", 1d))
                .add(new IndexRequest("test").id("no_count_new_" + i).source("data", 99, "data_d", 1d))
                .add(new IndexRequest("test").id("no_data_" + i).source("count", 12, "count_d", 12d))
                .get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("test").get();
            }
        }
        client().admin().indices().prepareRefresh("test").get();
        try (EsqlQueryResponse results = run("from test | stats avg(count) by data | sort data")) {
            logger.info(results);

            assertThat(results.columns(), hasSize(2));
            assertEquals("avg(count)", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals("data", results.columns().get(1).name());
            assertEquals(DataType.LONG, results.columns().get(1).type());

            record Group(Long data, Double avg) {}
            List<Group> expectedGroups = List.of(new Group(1L, 42.0), new Group(2L, 44.0), new Group(99L, null), new Group(null, 12.0));
            List<Group> actualGroups = getValuesList(results).stream().map(l -> new Group((Long) l.get(1), (Double) l.get(0))).toList();
            assertThat(actualGroups, equalTo(expectedGroups));
        }
    }

    public void testFromStatsGroupingByKeyword() {
        try (EsqlQueryResponse results = run("from test | stats avg(count) by color")) {
            logger.info(results);
            assertEquals(2, results.columns().size());
            assertEquals(3, getValuesList(results).size());

            // assert column metadata
            assertEquals("avg(count)", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals("color", results.columns().get(1).name());
            assertEquals(DataType.KEYWORD, results.columns().get(1).type());
            record Group(String color, double avg) {

            }
            List<Group> expectedGroups = List.of(new Group("blue", 42.0), new Group("green", 44.0), new Group("red", 43));
            List<Group> actualGroups = getValuesList(results).stream()
                .map(l -> new Group((String) l.get(1), (Double) l.get(0)))
                .sorted(comparing(c -> c.color))
                .toList();
            assertThat(actualGroups, equalTo(expectedGroups));
        }
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
            try (EsqlQueryResponse results = run("from test | stats avg = avg(" + field + ") by color")) {
                logger.info(results);
                assertEquals(2, results.columns().size());
                assertEquals(5, getValuesList(results).size());

                // assert column metadata
                assertEquals("avg", results.columns().get(0).name());
                assertEquals(DataType.DOUBLE, results.columns().get(0).type());
                assertEquals("color", results.columns().get(1).name());
                assertEquals(DataType.KEYWORD, results.columns().get(1).type());
                record Group(String color, Double avg) {

                }
                List<Group> expectedGroups = List.of(
                    new Group(null, 120.0),
                    new Group("blue", 42.0),
                    new Group("green", 44.0),
                    new Group("red", 43.0),
                    new Group("yellow", null)
                );
                List<Group> actualGroups = getValuesList(results).stream()
                    .map(l -> new Group((String) l.get(1), (Double) l.get(0)))
                    .sorted(Comparator.comparing(c -> c.color, Comparator.nullsFirst(String::compareTo)))
                    .toList();
                assertThat(actualGroups, equalTo(expectedGroups));
            }
        }
    }

    public void testFromStatsMultipleAggs() {
        try (
            EsqlQueryResponse results = run(
                "from test | stats a=avg(count), mi=min(count), ma=max(count), s=sum(count), c=count(count) by color"
            )
        ) {
            logger.info(results);
            assertEquals(6, results.columns().size());
            assertEquals(3, getValuesList(results).size());

            // assert column metadata
            assertEquals("a", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals("mi", results.columns().get(1).name());
            assertEquals(DataType.LONG, results.columns().get(1).type());
            assertEquals("ma", results.columns().get(2).name());
            assertEquals(DataType.LONG, results.columns().get(2).type());
            assertEquals("s", results.columns().get(3).name());
            assertEquals(DataType.LONG, results.columns().get(3).type());
            assertEquals("c", results.columns().get(4).name());
            assertEquals(DataType.LONG, results.columns().get(4).type());
            assertEquals("color", results.columns().get(5).name());
            assertEquals(DataType.KEYWORD, results.columns().get(5).type());
            record Group(double avg, long mi, long ma, long s, long c, String color) {}
            List<Group> expectedGroups = List.of(
                new Group(42, 42, 42, 420, 10, "blue"),
                new Group(44, 44, 44, 440, 10, "green"),
                new Group(43, 40, 46, 860, 20, "red")
            );
            // TODO: each aggregator returns Double now, it should in fact mirror the data type of the fields it's aggregating
            List<Group> actualGroups = getValuesList(results).stream()
                .map(
                    l -> new Group((Double) l.get(0), (Long) l.get(1), (Long) l.get(2), (Long) l.get(3), (Long) l.get(4), (String) l.get(5))
                )
                .sorted(comparing(c -> c.color))
                .toList();
            assertThat(actualGroups, equalTo(expectedGroups));
        }
    }

    public void testFromSortWithTieBreakerLimit() {
        try (EsqlQueryResponse results = run("from test | sort data, count desc, time | limit 5 | keep data, count, time")) {
            logger.info(results);
            assertThat(
                getValuesList(results),
                contains(
                    List.of(1L, 44L, epoch + 2),
                    List.of(1L, 44L, epoch + 6),
                    List.of(1L, 44L, epoch + 10),
                    List.of(1L, 44L, epoch + 14),
                    List.of(1L, 44L, epoch + 18)
                )
            );
        }
    }

    public void testFromStatsProjectGroup() {
        try (EsqlQueryResponse results = run("from test | stats avg_count = avg(count) by data | keep data")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("data"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.LONG));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(1L), List.of(2L)));
        }
    }

    public void testRowStatsProjectGroupByInt() {
        try (EsqlQueryResponse results = run("row a = 1, b = 2 | stats count(b) by a | keep a")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.INTEGER));
            assertThat(getValuesList(results), contains(List.of(1)));
        }
    }

    public void testRowStatsProjectGroupByLong() {
        try (EsqlQueryResponse results = run("row a = 1000000000000, b = 2 | stats count(b) by a | keep a")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.LONG));
            assertThat(getValuesList(results), contains(List.of(1000000000000L)));
        }
    }

    public void testRowStatsProjectGroupByDouble() {
        try (EsqlQueryResponse results = run("row a = 1.0, b = 2 | stats count(b) by a | keep a")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE));
            assertThat(getValuesList(results), contains(List.of(1.0)));
        }
    }

    public void testRowStatsProjectGroupByKeyword() {
        try (EsqlQueryResponse results = run("row a = \"hello\", b = 2 | stats count(b) by a | keep a")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.KEYWORD));
            assertThat(getValuesList(results), contains(List.of("hello")));
        }
    }

    public void testFromStatsProjectGroupByDouble() {
        try (EsqlQueryResponse results = run("from test | stats count(count) by data_d | keep data_d")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("data_d"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(1.0), List.of(2.0)));
        }
    }

    public void testFromStatsProjectGroupWithAlias() {
        String query = "from test | stats avg_count = avg(count) by data | eval d2 = data | rename data as d | keep d, d2";
        try (EsqlQueryResponse results = run(query)) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("d", "d2"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.LONG, DataType.LONG));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(1L, 1L), List.of(2L, 2L)));
        }
    }

    public void testFromStatsProjectAgg() {
        try (EsqlQueryResponse results = run("from test | stats a = avg(count) by data | keep a")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(42d), List.of(44d)));
        }
    }

    public void testFromStatsProjectAggWithAlias() {
        try (EsqlQueryResponse results = run("from test | stats a = avg(count) by data | rename a as b | keep b")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("b"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(42d), List.of(44d)));
        }
    }

    public void testFromProjectStatsGroupByAlias() {
        try (EsqlQueryResponse results = run("from test | rename data as d | keep d, count | stats avg(count) by d")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("avg(count)", "d"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE, DataType.LONG));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(42d, 1L), List.of(44d, 2L)));
        }
    }

    public void testFromProjectStatsAggregateAlias() {
        try (EsqlQueryResponse results = run("from test | rename count as c | keep c, data | stats avg(c) by data")) {
            logger.info(results);
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("avg(c)", "data"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.DOUBLE, DataType.LONG));
            assertThat(getValuesList(results), containsInAnyOrder(List.of(42d, 1L), List.of(44d, 2L)));
        }
    }

    public void testFromEvalStats() {
        try (EsqlQueryResponse results = run("from test | eval ratio = data_d / count_d | stats avg(ratio)")) {
            logger.info(results);
            assertEquals(1, results.columns().size());
            assertEquals(1, getValuesList(results).size());
            assertEquals("avg(ratio)", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals(1, getValuesList(results).get(0).size());
            assertEquals(0.034d, (double) getValuesList(results).get(0).get(0), 0.001d);
        }
    }

    public void testUngroupedCountAll() {
        try (EsqlQueryResponse results = run("from test | stats count(*)")) {
            logger.info(results);
            assertEquals(1, results.columns().size());
            assertEquals(1, getValuesList(results).size());
            assertEquals("count(*)", results.columns().get(0).name());
            assertEquals(DataType.LONG, results.columns().get(0).type());
            var values = getValuesList(results).get(0);
            assertEquals(1, values.size());
            assertEquals(40, (long) values.get(0));
        }
    }

    public void testUngroupedCountAllWithFilter() {
        try (EsqlQueryResponse results = run("from test | where data > 1 | stats count(*)")) {
            logger.info(results);
            assertEquals(1, results.columns().size());
            assertEquals(1, getValuesList(results).size());
            assertEquals("count(*)", results.columns().get(0).name());
            assertEquals(DataType.LONG, results.columns().get(0).type());
            var values = getValuesList(results).get(0);
            assertEquals(1, values.size());
            assertEquals(20, (long) values.get(0));
        }
    }

    public void testGroupedCountAllWithFilter() {
        try (EsqlQueryResponse results = run("from test | where data > 1 | stats count(*) by data | sort data")) {
            logger.info(results);
            assertEquals(2, results.columns().size());
            assertEquals(1, getValuesList(results).size());
            assertEquals("count(*)", results.columns().get(0).name());
            assertEquals(DataType.LONG, results.columns().get(0).type());
            assertEquals("data", results.columns().get(1).name());
            assertEquals(DataType.LONG, results.columns().get(1).type());
            var values = getValuesList(results).get(0);
            assertEquals(2, values.size());
            assertEquals(20, (long) values.get(0));
            assertEquals(2L, (long) values.get(1));
        }
    }

    public void testFromStatsEvalWithPragma() {
        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        try (EsqlQueryResponse results = run("from test | stats avg_count = avg(count) | eval x = avg_count + 7")) {
            logger.info(results);
            assertEquals(1, getValuesList(results).size());
            assertEquals(2, getValuesList(results).get(0).size());
            assertEquals(
                50,
                (double) getValuesList(results).get(0).get(results.columns().indexOf(new ColumnInfoImpl("x", "double", null))),
                1d
            );
            assertEquals(
                43,
                (double) getValuesList(results).get(0).get(results.columns().indexOf(new ColumnInfoImpl("avg_count", "double", null))),
                1d
            );
        }
    }

    public void testWhere() {
        try (EsqlQueryResponse results = run("from test | where count > 40")) {
            logger.info(results);
            assertEquals(30, getValuesList(results).size());
            var countIndex = results.columns().indexOf(new ColumnInfoImpl("count", "long", null));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(countIndex), greaterThan(40L));
            }
        }
    }

    public void testProjectWhere() {
        try (EsqlQueryResponse results = run("from test | keep count | where count > 40")) {
            logger.info(results);
            assertEquals(30, getValuesList(results).size());
            int countIndex = results.columns().indexOf(new ColumnInfoImpl("count", "long", null));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(countIndex), greaterThan(40L));
            }
        }
    }

    public void testEvalWhere() {
        try (EsqlQueryResponse results = run("from test | eval x = count / 2 | where x > 20")) {
            logger.info(results);
            assertEquals(30, getValuesList(results).size());
            int countIndex = results.columns().indexOf(new ColumnInfoImpl("x", "long", null));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(countIndex), greaterThan(20L));
            }
        }
    }

    public void testFilterWithNullAndEval() {
        try (EsqlQueryResponse results = run("row a = 1 | eval b = a + null | where b > 1")) {
            logger.info(results);
            assertEquals(0, getValuesList(results).size());
        }
    }

    public void testSortWithNull() {
        try (EsqlQueryResponse results = run("row a = null | sort a")) {
            logger.info(results);
            assertEquals(1, getValuesList(results).size());
            int countIndex = results.columns().indexOf(new ColumnInfoImpl("a", "null", null));
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.NULL));
            assertNull(getValuesList(results).get(0).get(countIndex));
        }
    }

    public void testStatsByNull() {
        try (EsqlQueryResponse results = run("row a = null | stats by a")) {
            logger.info(results);
            assertEquals(1, getValuesList(results).size());
            int countIndex = results.columns().indexOf(new ColumnInfoImpl("a", "null", null));
            assertThat(results.columns().stream().map(ColumnInfo::name).toList(), contains("a"));
            assertThat(results.columns().stream().map(ColumnInfoImpl::type).toList(), contains(DataType.NULL));
            assertNull(getValuesList(results).get(0).get(countIndex));
        }
    }

    public void testStringLength() {
        try (EsqlQueryResponse results = run("from test | eval l = length(color)")) {
            logger.info(results);
            assertThat(getValuesList(results), hasSize(40));
            int countIndex = results.columns().indexOf(new ColumnInfoImpl("l", "integer", null));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Integer) values.get(countIndex), greaterThanOrEqualTo(3));
            }
        }
    }

    public void testFilterWithNullAndEvalFromIndex() {
        // append entry, with an absent count, to the index
        client().prepareBulk().add(new IndexRequest("test").id("no_count").source("data", 12, "data_d", 2d, "color", "red")).get();

        client().admin().indices().prepareRefresh("test").get();
        // sanity
        try (EsqlQueryResponse results = run("from test")) {
            assertEquals(41, getValuesList(results).size());
        }
        try (EsqlQueryResponse results = run("from test | eval newCount = count + 1 | where newCount > 1")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertThat(results.columns(), hasItem(equalTo(new ColumnInfoImpl("count", "long", null))));
            assertThat(results.columns(), hasItem(equalTo(new ColumnInfoImpl("count_d", "double", null))));
            assertThat(results.columns(), hasItem(equalTo(new ColumnInfoImpl("data", "long", null))));
            assertThat(results.columns(), hasItem(equalTo(new ColumnInfoImpl("data_d", "double", null))));
            assertThat(results.columns(), hasItem(equalTo(new ColumnInfoImpl("time", "long", null))));
        }
    }

    public void testMultiConditionalWhere() {
        try (var results = run("from test | eval abc = 1+2 | where (abc + count >= 44 or data_d == 2) and data == 1 | keep color, abc")) {
            logger.info(results);
            assertEquals(10, getValuesList(results).size());
            assertEquals(2, results.columns().size());
            for (List<Object> values : getValuesList(results)) {
                assertThat((String) values.get(0), equalTo("green"));
                assertThat((Integer) values.get(1), equalTo(3));
            }
        }
    }

    public void testWhereNegatedCondition() {
        try (var results = run("from test | eval abc=1+2 | where abc + count > 45 and data != 1 | keep color, data")) {
            logger.info(results);
            assertEquals(10, getValuesList(results).size());
            assertEquals(2, results.columns().size());
            for (List<Object> values : getValuesList(results)) {
                assertThat((String) values.get(0), equalTo("red"));
                assertThat((Long) values.get(1), equalTo(2L));
            }
        }
    }

    public void testEvalOverride() {
        try (var results = run("from test | eval count = count + 1 | eval count = count + 1")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertEquals(1, results.columns().stream().filter(c -> c.name().equals("count")).count());
            int countIndex = results.columns().size() - 1;
            assertEquals(new ColumnInfoImpl("count", "long", null), results.columns().get(countIndex));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(countIndex), greaterThanOrEqualTo(42L));
            }
        }
    }

    public void testProjectRename() {
        try (var results = run("from test | eval y = count | rename count as x | keep x, y")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertThat(results.columns(), contains(new ColumnInfoImpl("x", "long", null), new ColumnInfoImpl("y", "long", null)));
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
                assertThat(values.get(1), is(values.get(0)));
            }
        }
    }

    public void testProjectRenameEval() {
        try (var results = run("from test | eval y = count | rename count as x | keep x, y | eval x2 = x + 1 | eval y2 = y + 2")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertThat(
                results.columns(),
                contains(
                    new ColumnInfoImpl("x", "long", null),
                    new ColumnInfoImpl("y", "long", null),
                    new ColumnInfoImpl("x2", "long", null),
                    new ColumnInfoImpl("y2", "long", null)
                )
            );
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
                assertThat(values.get(1), is(values.get(0)));
                assertThat(values.get(2), is(((Long) values.get(0)) + 1));
                assertThat(values.get(3), is(((Long) values.get(0)) + 2));
            }
        }
    }

    public void testProjectRenameEvalProject() {
        try (var results = run("from test | eval y = count | rename count as x | keep x, y | eval z = x + y | keep x, y, z")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertThat(
                results.columns(),
                contains(
                    new ColumnInfoImpl("x", "long", null),
                    new ColumnInfoImpl("y", "long", null),
                    new ColumnInfoImpl("z", "long", null)
                )
            );
            for (List<Object> values : getValuesList(results)) {
                assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
                assertThat(values.get(1), is(values.get(0)));
                assertThat(values.get(2), is((Long) values.get(0) * 2));
            }
        }
    }

    public void testProjectOverride() {
        try (var results = run("from test | eval cnt = count | rename count as data | keep cnt, data")) {
            logger.info(results);
            assertEquals(40, getValuesList(results).size());
            assertThat(results.columns(), contains(new ColumnInfoImpl("cnt", "long", null), new ColumnInfoImpl("data", "long", null)));
            for (List<Object> values : getValuesList(results)) {
                assertThat(values.get(1), is(values.get(0)));
            }
        }
    }

    public void testRefreshSearchIdleShards() throws Exception {
        String indexName = "test_refresh";
        int numShards = between(1, 2);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), 0)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.routing.rebalance.enable", "none")
                )
        );
        ensureYellow(indexName);
        AtomicLong totalValues = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean();
        Thread indexingThread = new Thread(() -> {
            try {
                assertTrue(latch.await(30, TimeUnit.SECONDS));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
            int numDocs = randomIntBetween(10, 20);
            while (stopped.get() == false) {
                if (rarely()) {
                    numDocs++;
                }
                logger.info("--> indexing {} docs", numDocs);
                long sum = 0;
                for (int i = 0; i < numDocs; i++) {
                    long value = randomLongBetween(1, 1000);
                    client().prepareBulk().add(new IndexRequest(indexName).id("doc-" + i).source("data", 1, "value", value)).get();
                    sum += value;
                }
                totalValues.set(sum);
            }
        });
        indexingThread.start();
        try {
            logger.info("--> waiting for shards to have pending refresh");
            Index index = resolveIndex(indexName);
            latch.countDown();
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
                assertThat("shards don't have any pending refresh", pendingRefreshes, equalTo(numShards));
            }, 30, TimeUnit.SECONDS);
        } finally {
            stopped.set(true);
            indexingThread.join();
        }
        try (EsqlQueryResponse results = run("from test_refresh | stats s = sum(value)")) {
            logger.info(results);
            assertThat(getValuesList(results).get(0), equalTo(List.of(totalValues.get())));
        }
    }

    public void testESFilter() throws Exception {
        String indexName = "test_filter";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
        );
        ensureYellow(indexName);
        int numDocs = randomIntBetween(1, 5000);
        Map<String, Long> docs = new HashMap<>();
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = "id-" + i;
            long value = randomLongBetween(-100_000, 100_000);
            docs.put(id, value);
            indexRequests.add(prepareIndex(indexName).setId(id).setSource(Map.of("val", value)));
        }
        indexRandom(true, randomBoolean(), indexRequests);
        String command = "from test_filter | stats avg = avg(val)";
        long from = randomBoolean() ? Long.MIN_VALUE : randomLongBetween(-1000, 1000);
        long to = randomBoolean() ? Long.MAX_VALUE : randomLongBetween(from, from + 1000);
        QueryBuilder filter = new RangeQueryBuilder("val").from(from, true).to(to, true);
        try (
            EsqlQueryResponse results = EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client())
                .query(command)
                .filter(filter)
                .pragmas(randomPragmas())
                .get()
        ) {
            logger.info(results);
            OptionalDouble avg = docs.values().stream().filter(v -> from <= v && v <= to).mapToLong(n -> n).average();
            if (avg.isPresent()) {
                assertEquals(avg.getAsDouble(), (double) getValuesList(results).get(0).get(0), 0.01d);
            } else {
                assertThat(getValuesList(results).get(0).get(0), nullValue());
            }
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
        );
        int numDocs = randomIntBetween(1, 100);
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        record Doc(long val, String tag) {

        }
        List<Doc> allDocs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Doc d = new Doc(i, "tag-" + randomIntBetween(1, 100));
            allDocs.add(d);
            indexRequests.add(prepareIndex(indexName).setId(Integer.toString(i)).setSource(Map.of("val", d.val, "tag", d.tag)));
        }
        indexRandom(true, randomBoolean(), indexRequests);
        int limit = randomIntBetween(1, 10);
        String command = "from test_extract_fields | sort val | limit " + limit;
        try (EsqlQueryResponse results = run(command)) {
            logger.info(results);
            // _doc, _segment, _shard are pruned
            assertThat(results.columns().size(), equalTo(2));
            assertThat(getValuesList(results), hasSize(Math.min(limit, numDocs)));
            assertThat(results.columns().get(1).name(), equalTo("val"));
            assertThat(results.columns().get(0).name(), equalTo("tag"));
            List<Doc> actualDocs = new ArrayList<>();
            for (int i = 0; i < getValuesList(results).size(); i++) {
                List<Object> values = getValuesList(results).get(i);
                actualDocs.add(new Doc((Long) values.get(1), (String) values.get(0)));
            }
            assertThat(actualDocs, equalTo(allDocs.stream().limit(limit).toList()));
        }
    }

    public void testEvalWithNullAndAvg() {
        try (EsqlQueryResponse results = run("from test | eval nullsum = count_d + null | stats avg(nullsum)")) {
            logger.info(results);
            assertEquals(1, results.columns().size());
            assertEquals(1, getValuesList(results).size());
            assertEquals("avg(nullsum)", results.columns().get(0).name());
            assertEquals(DataType.DOUBLE, results.columns().get(0).type());
            assertEquals(1, getValuesList(results).get(0).size());
            assertNull(getValuesList(results).get(0).get(0));
        }
    }

    public void testFromStatsLimit() {
        try (EsqlQueryResponse results = run("from test | stats ac = avg(count) by data | limit 1")) {
            logger.info(results);
            assertThat(results.columns(), contains(new ColumnInfoImpl("ac", "double", null), new ColumnInfoImpl("data", "long", null)));
            assertThat(getValuesList(results), contains(anyOf(contains(42.0, 1L), contains(44.0, 2L))));
        }
    }

    public void testFromLimit() {
        try (EsqlQueryResponse results = run("from test | keep data | limit 2")) {
            logger.info(results);
            assertThat(results.columns(), contains(new ColumnInfoImpl("data", "long", null)));
            assertThat(getValuesList(results), contains(anyOf(contains(1L), contains(2L)), anyOf(contains(1L), contains(2L))));
        }
    }

    public void testDropAllColumns() {
        try (EsqlQueryResponse results = run("from test | keep data | drop data | eval a = 1")) {
            logger.info(results);
            assertThat(results.columns(), hasSize(1));
            assertThat(results.columns(), contains(new ColumnInfoImpl("a", "integer", null)));
            assertThat(getValuesList(results), is(empty()));
        }
    }

    public void testDropAllColumnsWithStats() {
        try (EsqlQueryResponse results = run("from test | stats g = count(data) | drop g")) {
            logger.info(results);
            assertThat(results.columns(), is(empty()));
            assertThat(getValuesList(results), is(empty()));
        }
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

        try (var results = run("from test_index_patterns* | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(15L, getValuesList(results).get(0).get(0));
            assertEquals(120000L, getValuesList(results).get(0).get(1));

        }

        try (var results = run("from test_index_patterns_1,test_index_patterns_2 | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(10L, getValuesList(results).get(0).get(0));
            assertEquals(55000L, getValuesList(results).get(0).get(1));
        }

        try (var results = run("from test_index_patterns_1*,test_index_patterns_2* | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(10L, getValuesList(results).get(0).get(0));
            assertEquals(55000L, getValuesList(results).get(0).get(1));
        }

        try (var results = run("from test_index_patterns_*,-test_index_patterns_1 | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(10L, getValuesList(results).get(0).get(0));
            assertEquals(105000L, getValuesList(results).get(0).get(1));
        }

        try (var results = run("from * | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(55L, getValuesList(results).get(0).get(0));
            assertEquals(121720L, getValuesList(results).get(0).get(1));

        }

        try (var results = run("from test_index_patterns_2 | stats count(data), sum(count)")) {
            assertEquals(1, getValuesList(results).size());
            assertEquals(5L, getValuesList(results).get(0).get(0));
            assertEquals(40000L, getValuesList(results).get(0).get(1));
        }
    }

    public void testDataStreamPatterns() throws Exception {
        Map<String, Long> testCases = new HashMap<>();
        // Concrete data stream with each selector
        testCases.put("test_ds_patterns_1", 5L);
        testCases.put("test_ds_patterns_1::data", 5L);
        testCases.put("test_ds_patterns_1::failures", 3L);
        testCases.put("test_ds_patterns_2", 5L);
        testCases.put("test_ds_patterns_2::data", 5L);
        testCases.put("test_ds_patterns_2::failures", 3L);

        // Wildcard pattern with each selector
        testCases.put("test_ds_patterns*", 15L);
        testCases.put("test_ds_patterns*::data", 15L);
        testCases.put("test_ds_patterns*::failures", 9L);

        // Match all pattern with each selector
        testCases.put("*", 15L);
        testCases.put("*::data", 15L);
        testCases.put("*::failures", 9L);

        // Concrete multi-pattern
        testCases.put("test_ds_patterns_1,test_ds_patterns_2", 10L);
        testCases.put("test_ds_patterns_1::data,test_ds_patterns_2::data", 10L);
        testCases.put("test_ds_patterns_1::failures,test_ds_patterns_2::failures", 6L);

        // Wildcard multi-pattern
        testCases.put("test_ds_patterns_1*,test_ds_patterns_2*", 10L);
        testCases.put("test_ds_patterns_1*::data,test_ds_patterns_2*::data", 10L);
        testCases.put("test_ds_patterns_1*::failures,test_ds_patterns_2*::failures", 6L);

        // Wildcard pattern with data stream exclusions for each selector combination (data stream exclusions need * on the end to negate)
        // None (default)
        testCases.put("test_ds_patterns*,-test_ds_patterns_2*", 10L);
        testCases.put("test_ds_patterns*,-test_ds_patterns_2*::data", 10L);
        testCases.put("test_ds_patterns*,-test_ds_patterns_2*::failures", 15L);
        // Subtracting from ::data
        testCases.put("test_ds_patterns*::data,-test_ds_patterns_2*", 10L);
        testCases.put("test_ds_patterns*::data,-test_ds_patterns_2*::data", 10L);
        testCases.put("test_ds_patterns*::data,-test_ds_patterns_2*::failures", 15L);
        // Subtracting from ::failures
        testCases.put("test_ds_patterns*::failures,-test_ds_patterns_2*", 9L);
        testCases.put("test_ds_patterns*::failures,-test_ds_patterns_2*::data", 9L);
        testCases.put("test_ds_patterns*::failures,-test_ds_patterns_2*::failures", 6L);
        // Subtracting from ::*
        testCases.put("test_ds_patterns*::data,test_ds_patterns*::failures,-test_ds_patterns_2*", 19L);
        testCases.put("test_ds_patterns*::data,test_ds_patterns*::failures,-test_ds_patterns_2*::data", 19L);
        testCases.put("test_ds_patterns*::data,test_ds_patterns*::failures,-test_ds_patterns_2*::failures", 21L);

        testCases.put("\"test_ds_patterns_1,test_ds_patterns_2\"::failures", 8L);

        runDataStreamTest(testCases, new String[] { "test_ds_patterns_1", "test_ds_patterns_2", "test_ds_patterns_3" }, (key, value) -> {
            try (var results = run("from " + key + " | stats count(@timestamp)")) {
                assertEquals(key, 1, getValuesList(results).size());
                assertEquals(key, value, getValuesList(results).get(0).get(0));
            }
        });
    }

    public void testDataStreamInvalidPatterns() throws Exception {
        Map<String, String> testCases = new HashMap<>();
        // === Errors
        // Only recognized components can be selected
        testCases.put("testXXX::custom", "invalid usage of :: separator, [custom] is not a recognized selector");
        // Spelling is important
        testCases.put("testXXX::failres", "invalid usage of :: separator, [failres] is not a recognized selector");
        // Only the match all wildcard is supported
        testCases.put("testXXX::d*ta", "invalid usage of :: separator, [d*ta] is not a recognized selector");
        // The first instance of :: is split upon so that you cannot chain the selector
        testCases.put("test::XXX::data", "mismatched input '::' expecting {");
        // Selectors must be outside of date math expressions or else they trip up the selector parsing
        testCases.put("<test-{now/d}::failures>", "Invalid index name [<test-{now/d}], must not contain the following characters [");
        // Only one selector separator is allowed per expression
        testCases.put("::::data", "mismatched input '::' expecting {");
        // Suffix case is not supported because there is no component named with the empty string
        testCases.put("index::", "missing {QUOTED_STRING, UNQUOTED_SOURCE} at '|'");

        runDataStreamTest(testCases, new String[] { "test_ds_patterns_1" }, (key, value) -> {
            logger.info(key);
            var exception = expectThrows(ParsingException.class, () -> { run("from " + key + " | stats count(@timestamp)").close(); });
            assertThat(exception.getMessage(), containsString(value));
        });
    }

    private <V> void runDataStreamTest(Map<String, V> testCases, String[] dsNames, BiConsumer<String, V> testMethod) throws IOException {
        boolean deleteTemplate = false;
        List<String> deleteDataStreams = new ArrayList<>();
        try {
            assertAcked(
                client().execute(
                    TransportPutComposableIndexTemplateAction.TYPE,
                    new TransportPutComposableIndexTemplateAction.Request("test_ds_template").indexTemplate(
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of("test_ds_patterns_*"))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .template(
                                Template.builder()
                                    .mappings(new CompressedXContent("""
                                        {
                                          "dynamic": false,
                                          "properties": {
                                            "@timestamp": {
                                              "type": "date"
                                            },
                                            "count": {
                                                "type": "long"
                                            }
                                          }
                                        }"""))
                                    .dataStreamOptions(
                                        new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(true).buildTemplate())
                                    )
                            )
                            .build()
                    )
                )
            );
            deleteTemplate = true;

            String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            int i = 0;
            for (String dsName : dsNames) {
                BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (String id : Arrays.asList("1", "2", "3", "4", "5")) {
                    bulk.add(createDoc(dsName, id, time, ++i * 1000));
                }
                for (String id : Arrays.asList("6", "7", "8")) {
                    bulk.add(createDoc(dsName, id, time, "garbage"));
                }
                BulkResponse bulkItemResponses = bulk.get();
                assertThat(bulkItemResponses.hasFailures(), is(false));
                deleteDataStreams.add(dsName);
                ensureYellow(dsName);
            }

            for (Map.Entry<String, V> testCase : testCases.entrySet()) {
                testMethod.accept(testCase.getKey(), testCase.getValue());
            }
        } finally {
            if (deleteDataStreams.isEmpty() == false) {
                assertAcked(
                    client().execute(
                        DeleteDataStreamAction.INSTANCE,
                        new DeleteDataStreamAction.Request(new TimeValue(30, TimeUnit.SECONDS), deleteDataStreams.toArray(String[]::new))
                    )
                );
            }
            if (deleteTemplate) {
                assertAcked(
                    client().execute(
                        TransportDeleteComposableIndexTemplateAction.TYPE,
                        new TransportDeleteComposableIndexTemplateAction.Request("test_ds_template")
                    )
                );
            }
        }
    }

    private static IndexRequest createDoc(String dsName, String id, String ts, Object count) {
        return new IndexRequest(dsName).opType(DocWriteRequest.OpType.CREATE).id(id).source("@timestamp", ts, "count", count);
    }

    public void testOverlappingIndexPatterns() throws Exception {
        String[] indexNames = { "test_overlapping_index_patterns_1", "test_overlapping_index_patterns_2" };

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test_overlapping_index_patterns_1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
                .setMapping("field", "type=long")
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
        );
        ensureYellow("test_overlapping_index_patterns_2");
        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(new IndexRequest("test_overlapping_index_patterns_2").id("1").source("field", "foo"))
            .get();

        assertThrows(VerificationException.class, () -> run("from test_overlapping_index_patterns_* | sort field"));
    }

    public void testErrorMessageForUnknownColumn() {
        var e = expectThrows(VerificationException.class, () -> run("row a = 1 | eval x = b"));
        assertThat(e.getMessage(), containsString("Unknown column [b]"));
    }

    public void testErrorMessageForEmptyParams() {
        var e = expectThrows(ParsingException.class, () -> run("row a = 1 | eval x = ?"));
        assertThat(e.getMessage(), containsString("Not enough actual parameters 0"));
    }

    public void testEmptyIndex() {
        assertAcked(client().admin().indices().prepareCreate("test_empty").setMapping("k", "type=keyword", "v", "type=long").get());
        try (EsqlQueryResponse results = run("from test_empty")) {
            assertThat(
                results.columns(),
                equalTo(List.of(new ColumnInfoImpl("k", "keyword", null), new ColumnInfoImpl("v", "long", null)))
            );
            assertThat(getValuesList(results), empty());
        }
    }

    public void testShowInfo() {
        try (EsqlQueryResponse results = run("show info")) {
            assertThat(
                results.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("version", "keyword", null),
                        new ColumnInfoImpl("date", "keyword", null),
                        new ColumnInfoImpl("hash", "keyword", null)
                    )
                )
            );
            assertThat(getValuesList(results).size(), equalTo(1));
            assertThat(getValuesList(results).get(0).get(0), equalTo(Build.current().version()));
            assertThat(getValuesList(results).get(0).get(1), equalTo(Build.current().date()));
            assertThat(getValuesList(results).get(0).get(2), equalTo(Build.current().hash()));
        }
    }

    public void testInWithNullValue() {
        try (EsqlQueryResponse results = run("from test | where null in (data, 2) | keep data")) {
            assertThat(results.columns(), equalTo(List.of(new ColumnInfoImpl("data", "long", null))));
            assertThat(getValuesList(results).size(), equalTo(0));
        }
    }

    public void testTopNPushedToLucene() {
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
        }
        client().admin().indices().prepareRefresh("test").get();

        try (EsqlQueryResponse results = run("""
                from test
                | where color == "yellow"
                | sort data desc nulls first, count asc nulls first
                | limit 10
                | keep data, count, color
            """)) {
            logger.info(results);
            assertEquals(3, results.columns().size());
            assertEquals(10, getValuesList(results).size());

            // assert column metadata
            assertEquals("data", results.columns().get(0).name());
            assertEquals(DataType.LONG, results.columns().get(0).type());
            assertEquals("count", results.columns().get(1).name());
            assertEquals(DataType.LONG, results.columns().get(1).type());
            assertEquals("color", results.columns().get(2).name());
            assertEquals(DataType.KEYWORD, results.columns().get(2).type());
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
            List<Group> actualGroups = getValuesList(results).stream()
                .map(l -> new Group((Long) l.get(0), (Long) l.get(1), (String) l.get(2)))
                .toList();
            assertThat(actualGroups, equalTo(expectedGroups));
        }
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
        try (EsqlQueryResponse results = run("from sorted_test_index | sort time " + sortOrder + " | limit " + limit + " | keep time")) {
            logger.info(results);
            assertEquals(1, results.columns().size());
            assertEquals(limit, getValuesList(results).size());

            // assert column metadata
            assertEquals("time", results.columns().get(0).name());
            assertEquals(DataType.LONG, results.columns().get(0).type());

            boolean sortedDesc = "desc".equals(sortOrder);
            var expected = LongStream.range(0, 40)
                .map(i -> epoch + i)
                .boxed()
                .sorted(sortedDesc ? reverseOrder() : naturalOrder())
                .limit(limit)
                .toList();
            var actual = getValuesList(results).stream().map(l -> (Long) l.get(0)).toList();
            assertThat(actual, equalTo(expected));
        }
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

    public void testGroupingMultiValueByOrdinals() {
        String indexName = "test-ordinals";
        assertAcked(client().admin().indices().prepareCreate(indexName).setMapping("kw", "type=keyword", "v", "type=long").get());
        int numDocs = randomIntBetween(10, 200);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> source = new HashMap<>();
            source.put("kw", "key-" + randomIntBetween(1, 20));
            List<Integer> values = new ArrayList<>();
            int numValues = between(0, 2);
            for (int v = 0; v < numValues; v++) {
                values.add(randomIntBetween(1, 1000));
            }
            if (values.isEmpty() == false) {
                source.put("v", values);
            }
            prepareIndex(indexName).setSource(source).get();
            if (randomInt(100) < 20) {
                client().admin().indices().prepareRefresh(indexName).get();
            }
        }
        client().admin().indices().prepareRefresh(indexName).get();
        var functions = List.of("min(v)", "max(v)", "count_distinct(v)", "count(v)", "sum(v)", "avg(v)", "percentile(v, 90)");
        for (String fn : functions) {
            String query = String.format(Locale.ROOT, "from %s | stats s = %s by kw", indexName, fn);
            run(query).close();
        }
    }

    public void testLoadId() {
        try (EsqlQueryResponse results = run("from test metadata _id | keep _id | sort _id ")) {
            assertThat(results.columns(), equalTo(List.of(new ColumnInfoImpl("_id", "keyword", null))));
            ListMatcher values = matchesList();
            for (int i = 10; i < 50; i++) {
                values = values.item(List.of(Integer.toString(i)));
            }
            assertMap(getValuesList(results), values);
        }
    }

    public void testUnsupportedTypesOrdinalGrouping() {
        assertAcked(
            client().admin().indices().prepareCreate("index-1").setMapping("f1", "type=keyword", "f2", "type=keyword", "v", "type=long")
        );
        assertAcked(
            client().admin().indices().prepareCreate("index-2").setMapping("f1", "type=object", "f2", "type=keyword", "v", "type=long")
        );
        Map<String, Long> groups = new HashMap<>();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String k = randomFrom("a", "b", "c");
            long v = randomIntBetween(1, 10);
            groups.merge(k, v, Long::sum);
            groups.merge(null, v, Long::sum); // null group
            prepareIndex("index-1").setSource("f1", k, "v", v).get();
            prepareIndex("index-2").setSource("f2", k, "v", v).get();
        }
        client().admin().indices().prepareRefresh("index-1", "index-2").get();
        for (String field : List.of("f1", "f2")) {
            try (var resp = run("from index-1,index-2 | stats sum(v) by " + field)) {
                Iterator<Iterator<Object>> values = resp.values();
                Map<String, Long> actual = new HashMap<>();
                while (values.hasNext()) {
                    Iterator<Object> row = values.next();
                    Long v = (Long) row.next();
                    String k = (String) row.next();
                    actual.put(k, v);
                }
                assertThat(actual, equalTo(groups));
            }
        }
    }

    public void testFilterNestedFields() {
        assertAcked(client().admin().indices().prepareCreate("index-1").setMapping("file.name", "type=keyword"));
        assertAcked(client().admin().indices().prepareCreate("index-2").setMapping("file", "type=keyword"));
        try (var resp = run("from index-1,index-2 | where file.name is not null")) {
            var valuesList = getValuesList(resp);
            assertEquals(2, resp.columns().size());
            assertEquals(0, valuesList.size());
        }
    }

    public void testStatsNestFields() {
        final String node1, node2;
        if (randomBoolean()) {
            internalCluster().ensureAtLeastNumDataNodes(2);
            node1 = randomDataNode().getName();
            node2 = randomValueOtherThan(node1, () -> randomDataNode().getName());
        } else {
            node1 = randomDataNode().getName();
            node2 = randomDataNode().getName();
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index-1")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node1))
                .setMapping("field_1", "type=integer")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index-2")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node2))
                .setMapping("field_2", "type=integer")
        );
        try (var resp = run("from index-1,index-2 | where field_1 is not null | stats c = count(*), c1 = count(field_1), m = count()")) {
            var valuesList = getValuesList(resp);
            assertEquals(3, resp.columns().size());
            assertEquals(1, valuesList.size());

            assertThat(valuesList.get(0), contains(0L, 0L, 0L));
        }

        try (var resp = run("from index-1,index-2 | where field_1 is not null | stats min = min(field_1), max = max(field_1)")) {
            var valuesList = getValuesList(resp);
            assertEquals(2, resp.columns().size());
            assertEquals(1, valuesList.size());

            assertThat(valuesList.get(0), contains(null, null));
        }
    }

    public void testStatsMissingFieldWithStats() {
        final String node1, node2;
        if (randomBoolean()) {
            internalCluster().ensureAtLeastNumDataNodes(2);
            node1 = randomDataNode().getName();
            node2 = randomValueOtherThan(node1, () -> randomDataNode().getName());
        } else {
            node1 = randomDataNode().getName();
            node2 = randomDataNode().getName();
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("foo-index")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node1))
                .setMapping("foo_int", "type=integer", "foo_long", "type=long", "foo_float", "type=float", "foo_double", "type=double")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("bar-index")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node2))
                .setMapping("bar_int", "type=integer", "bar_long", "type=long", "bar_float", "type=float", "bar_double", "type=double")
        );
        var fields = List.of("foo_int", "foo_long", "foo_float", "foo_double");
        var functions = List.of("sum", "count", "avg", "count_distinct");
        for (String field : fields) {
            for (String function : functions) {
                String stat = String.format(Locale.ROOT, "stats s = %s(%s)", function, field);
                String command = String.format(Locale.ROOT, "from foo-index,bar-index | where %s is not null | %s", field, stat);
                try (var resp = run(command)) {
                    var valuesList = getValuesList(resp);
                    assertEquals(1, resp.columns().size());
                    assertEquals(1, valuesList.size());
                }
            }
        }
    }

    public void testStatsMissingFieldKeepApp() {
        final String node1, node2;
        if (randomBoolean()) {
            internalCluster().ensureAtLeastNumDataNodes(2);
            node1 = randomDataNode().getName();
            node2 = randomValueOtherThan(node1, () -> randomDataNode().getName());
        } else {
            node1 = randomDataNode().getName();
            node2 = randomDataNode().getName();
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("foo-index")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node1))
                .setMapping("foo_int", "type=integer", "foo_long", "type=long", "foo_float", "type=float", "foo_double", "type=double")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("bar-index")
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node2))
                .setMapping("bar_int", "type=integer", "bar_long", "type=long", "bar_float", "type=float", "bar_double", "type=double")
        );
        String command = String.format(Locale.ROOT, "from foo-index,bar-index");
        try (var resp = run(command)) {
            var valuesList = getValuesList(resp);
            assertEquals(8, resp.columns().size());
            assertEquals(0, valuesList.size());
            assertEquals(Collections.emptyList(), valuesList);
        }
    }

    public void testCountTextField() {
        assertAcked(client().admin().indices().prepareCreate("test_count").setMapping("name", "type=text"));
        int numDocs = between(10, 1000);
        Set<String> names = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
            String name = "name-" + randomIntBetween(1, 100);
            names.add(name);
            IndexRequestBuilder indexRequest = client().prepareIndex("test_count").setSource("name", name);
            if (randomInt(100) < 5) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.get();
        }
        client().admin().indices().prepareRefresh("test_count").get();
        try (EsqlQueryResponse resp = run("FROM test_count | stats COUNT_DISTINCT(name)")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo((long) names.size()));
            assertFalse(row.hasNext());
        }
        try (EsqlQueryResponse resp = run("FROM test_count | stats COUNT(name)")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo((long) numDocs));
            assertFalse(row.hasNext());
        }
    }

    public void testQueryOnEmptyMappingIndex() {
        createIndex("empty-test", Settings.EMPTY);
        createIndex("empty-test2", Settings.EMPTY);
        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = indicesAdmin().prepareAliases()
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("empty-test").alias("alias-test"))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("empty-test2").alias("alias-test"));
        indicesAdmin().aliases(indicesAliasesRequestBuilder.request()).actionGet();

        String[] indexPatterns = new String[] { "empty-test", "empty-test,empty-test2", "empty-test*", "alias-test", "*-test*" };
        String from = "FROM " + randomFrom(indexPatterns) + " ";

        assertEmptyIndexQueries(from);

        try (EsqlQueryResponse resp = run(from + "METADATA _source | EVAL x = 123")) {
            assertFalse(resp.values().hasNext());
            assertThat(
                resp.columns(),
                equalTo(List.of(new ColumnInfoImpl("_source", "_source", null), new ColumnInfoImpl("x", "integer", null)))
            );
        }

        try (EsqlQueryResponse resp = run(from)) {
            assertFalse(resp.values().hasNext());
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("<no-fields>", "null", null))));
        }
    }

    public void testQueryOnEmptyDataIndex() {
        createIndex("empty_data-test", Settings.EMPTY);
        assertAcked(client().admin().indices().prepareCreate("empty_data-test2").setMapping("name", "type=text"));
        IndicesAliasesRequestBuilder indicesAliasesRequestBuilder = indicesAdmin().prepareAliases()
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("empty_data-test").alias("alias-empty_data-test"))
            .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("empty_data-test2").alias("alias-empty_data-test"));
        indicesAdmin().aliases(indicesAliasesRequestBuilder.request()).actionGet();

        String[] indexPatterns = new String[] {
            "empty_data-test2",
            "empty_data-test,empty_data-test2",
            "alias-empty_data-test",
            "*data-test" };
        String from = "FROM " + randomFrom(indexPatterns) + " ";

        assertEmptyIndexQueries(from);

        try (EsqlQueryResponse resp = run(from + "METADATA _source | EVAL x = 123")) {
            assertFalse(resp.values().hasNext());
            assertThat(
                resp.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("name", "text", null),
                        new ColumnInfoImpl("_source", "_source", null),
                        new ColumnInfoImpl("x", "integer", null)
                    )
                )
            );
        }

        try (EsqlQueryResponse resp = run(from)) {
            assertFalse(resp.values().hasNext());
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("name", "text", null))));
        }
    }

    private void assertEmptyIndexQueries(String from) {
        try (EsqlQueryResponse resp = run(from + "METADATA _source | KEEP _source | LIMIT 1")) {
            assertFalse(resp.values().hasNext());
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("_source", "_source", null))));
        }

        try (EsqlQueryResponse resp = run(from + "| EVAL y = 1 | KEEP y | LIMIT 1 | EVAL x = 1")) {
            assertFalse(resp.values().hasNext());
            assertThat(
                resp.columns(),
                equalTo(List.of(new ColumnInfoImpl("y", "integer", null), new ColumnInfoImpl("x", "integer", null)))
            );
        }

        try (EsqlQueryResponse resp = run(from + "| STATS c = count()")) {
            assertTrue(resp.values().hasNext());
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo((long) 0));
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("c", "long", null))));
        }

        try (EsqlQueryResponse resp = run(from + "| STATS c = count() | EVAL x = 123")) {
            assertTrue(resp.values().hasNext());
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo((long) 0));
            assertThat(row.next(), equalTo(123));
            assertFalse(row.hasNext());
            assertThat(resp.columns(), equalTo(List.of(new ColumnInfoImpl("c", "long", null), new ColumnInfoImpl("x", "integer", null))));
        }
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
        try (EsqlQueryResponse results = run(query)) {
            assertThat(results.columns(), contains(new ColumnInfoImpl("data", "long", null)));
            assertThat(results.columns().size(), is(1));
            assertThat(getValuesList(results).size(), is(docsCount));
            for (List<Object> row : getValuesList(results)) {
                assertThat(row.size(), is(1));
                // check that all the values returned are the regular ones
                assertThat((Long) row.get(0), allOf(greaterThanOrEqualTo(minValue), lessThanOrEqualTo(maxValue)));
            }
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

    public void testDefaultTruncationSizeSetting() {
        ClusterAdminClient client = admin().cluster();

        Settings settings = Settings.builder().put(EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getKey(), 1).build();

        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .persistentSettings(settings);

        client.updateSettings(settingsRequest).actionGet();
        try (EsqlQueryResponse results = run("from test")) {
            logger.info(results);
            assertEquals(1, getValuesList(results).size());
        } finally {
            clearPersistentSettings(EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE);
        }
    }

    public void testMaxTruncationSizeSetting() {
        ClusterAdminClient client = admin().cluster();

        Settings settings = Settings.builder().put(EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getKey(), 10).build();

        ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .persistentSettings(settings);

        client.updateSettings(settingsRequest).actionGet();
        try (EsqlQueryResponse results = run("from test | limit 40")) {
            logger.info(results);
            assertEquals(10, getValuesList(results).size());
        } finally {
            clearPersistentSettings(EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE);
        }
    }

    public void testScriptField() throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("runtime");
            {
                mapping.startObject("k1");
                mapping.field("type", "long");
                mapping.endObject();
                mapping.startObject("k2");
                mapping.field("type", "long");
                mapping.endObject();
            }
            mapping.endObject();
            {
                mapping.startObject("properties");
                mapping.startObject("meter").field("type", "double").endObject();
                mapping.endObject();
            }
        }
        mapping.endObject();
        String sourceMode = randomBoolean() ? "stored" : "synthetic";
        Settings.Builder settings = indexSettings(1, 0).put(indexSettings()).put("index.mapping.source.mode", sourceMode);
        client().admin().indices().prepareCreate("test-script").setMapping(mapping).setSettings(settings).get();
        int numDocs = 256;
        for (int i = 0; i < numDocs; i++) {
            index("test-script", Integer.toString(i), Map.of("k1", i, "k2", "b-" + i, "meter", 10000 * i));
        }
        refresh("test-script");

        var pragmas = randomPragmas();
        if (canUseQueryPragmas()) {
            Settings.Builder pragmaSettings = Settings.builder().put(pragmas.getSettings());
            pragmaSettings.put("task_concurrency", 10);
            pragmaSettings.put("data_partitioning", "doc");
            pragmas = new QueryPragmas(pragmaSettings.build());
        }
        try (EsqlQueryResponse resp = run("FROM test-script | SORT k1 | LIMIT " + numDocs, pragmas)) {
            List<Object> k1Column = Iterators.toList(resp.column(0));
            assertThat(k1Column, equalTo(LongStream.range(0L, numDocs).boxed().toList()));
            List<Object> k2Column = Iterators.toList(resp.column(1));
            assertThat(k2Column, equalTo(Collections.nCopies(numDocs, null)));
            List<Object> meterColumn = Iterators.toList(resp.column(2));
            var expectedMeterColumn = new ArrayList<>(numDocs);
            double val = 0.0;
            for (int i = 0; i < numDocs; i++) {
                expectedMeterColumn.add(val);
                val += 10000.0;
            }
            assertThat(meterColumn, equalTo(expectedMeterColumn));
        }
    }

    private void clearPersistentSettings(Setting<?>... settings) {
        Settings.Builder clearedSettings = Settings.builder();

        for (Setting<?> s : settings) {
            clearedSettings.putNull(s.getKey());
        }

        var clearSettingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).persistentSettings(
            clearedSettings.build()
        );
        admin().cluster().updateSettings(clearSettingsRequest).actionGet();
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}

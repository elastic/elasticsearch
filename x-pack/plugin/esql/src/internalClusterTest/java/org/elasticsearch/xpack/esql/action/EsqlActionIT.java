/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@Experimental
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class EsqlActionIT extends ESIntegTestCase {

    long epoch = System.currentTimeMillis();

    @Before
    public void setupIndex() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)))
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
                    "type=date",
                    "color",
                    "type=keyword"
                )
                .get()
        );
        long timestamp = epoch;
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(
                    new IndexRequest("test").id("1" + i)
                        .source("data", 1, "count", 40, "data_d", 1d, "count_d", 40d, "time", timestamp++, "color", "red")
                )
                .add(
                    new IndexRequest("test").id("2" + i)
                        .source("data", 2, "count", 42, "data_d", 2d, "count_d", 42d, "time", timestamp++, "color", "blue")
                )
                .add(
                    new IndexRequest("test").id("3" + i)
                        .source("data", 1, "count", 44, "data_d", 1d, "count_d", 44d, "time", timestamp++, "color", "green")
                )
                .add(
                    new IndexRequest("test").id("4" + i)
                        .source("data", 2, "count", 46, "data_d", 2d, "count_d", 46d, "time", timestamp++, "color", "red")
                )
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow("test");
    }

    public void testRow() {
        long value = randomLongBetween(0, Long.MAX_VALUE);
        EsqlQueryResponse response = run("row " + value);
        assertEquals(List.of(List.of(value)), response.values());
    }

    public void testFromStatsAvg() {
        testFromStatsAvgImpl("from test | stats avg(count)", "avg(count)");
    }

    public void testFromStatsAvgWithAlias() {
        testFromStatsAvgImpl("from test | stats f1 = avg(count)", "f1");
    }

    private void testFromStatsAvgImpl(String command, String expectedFieldName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals(expectedFieldName, results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(43, (double) results.values().get(0).get(0), 1d);
    }

    public void testFromStatsCount() {
        testFromStatsCountImpl("from test | stats count(data)", "count(data)");
    }

    public void testFromStatsCountWithAlias() {
        testFromStatsCountImpl("from test | stats dataCount = count(data)", "dataCount");
    }

    public void testFromStatsCountImpl(String command, String expectedFieldName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals(expectedFieldName, results.columns().get(0).name());
        assertEquals("long", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(40L, results.values().get(0).get(0));
    }

    @AwaitsFix(bugUrl = "line 1:45: Unknown column [data]")
    public void testFromStatsGroupingAvgWithSort() {  // FIX ME
        testFromStatsGroupingAvgImpl("from test | stats avg(count) by data | sort data | limit 2", "avg(count)", "data");
    }

    public void testFromStatsGroupingAvg() {
        testFromStatsGroupingAvgImpl("from test | stats avg(count) by data", "data", "avg(count)");
    }

    public void testFromStatsGroupingAvgWithAliases() {
        testFromStatsGroupingAvgImpl("from test | eval g = data | stats f = avg(count) by g", "g", "f");
    }

    private void testFromStatsGroupingAvgImpl(String command, String expectedFieldName, String expectedGroupName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());

        // assert column metadata
        ColumnInfo groupColumn = results.columns().get(0);
        assertEquals(expectedGroupName, groupColumn.name());
        assertEquals("double", groupColumn.type());
        ColumnInfo valuesColumn = results.columns().get(1);
        assertEquals(expectedFieldName, valuesColumn.name());
        assertEquals("long", valuesColumn.type());

        // assert column values
        List<List<Object>> valueValues = results.values();
        assertEquals(2, valueValues.size());
        // This is loathsome, find a declarative way to assert the expected output.
        if ((long) valueValues.get(0).get(1) == 1L) {
            assertEquals(42, (double) valueValues.get(0).get(0), 1d);
            assertEquals(2L, (long) valueValues.get(1).get(1));
            assertEquals(44, (double) valueValues.get(1).get(0), 1d);
        } else if ((long) valueValues.get(0).get(1) == 2L) {
            assertEquals(42, (double) valueValues.get(1).get(0), 1d);
            assertEquals(1L, (long) valueValues.get(1).get(1));
            assertEquals(44, (double) valueValues.get(0).get(0), 1d);
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
        assertEquals("date", results.columns().get(1).type());

        // assert column values
        List<Long> expectedValues = LongStream.range(0, 40).map(i -> epoch + i).sorted().boxed().toList();
        List<Long> actualValues = IntStream.range(0, 40).mapToLong(i -> (Long) results.values().get(i).get(1)).sorted().boxed().toList();
        assertEquals(expectedValues, actualValues);
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
        List<Group> expectedGroups = List.of(new Group("blue", 42), new Group("green", 44), new Group("red", 43));
        List<Group> actualGroups = results.values()
            .stream()
            .map(l -> new Group((String) l.get(1), (Double) l.get(0)))
            .sorted(Comparator.comparing(c -> c.color))
            .toList();
        assertThat(actualGroups, equalTo(expectedGroups));
    }

    public void testFrom() {
        EsqlQueryResponse results = run("from test");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("count", "long"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("count_d", "double"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("data", "long"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("data_d", "double"))));
        assertThat(results.columns(), hasItem(equalTo(new ColumnInfo("time", "date"))));
        // TODO: we have some extra internal columns as well (_doc_id, ...) that we should drop
    }

    public void testFromSortLimit() {
        EsqlQueryResponse results = run("from test | sort count | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        // trying to get the count
        var position = results.columns().indexOf(new ColumnInfo("count", "long"));
        assertEquals(40, (long) results.values().get(0).get(position));
    }

    public void testFromEvalSortLimit() {
        EsqlQueryResponse results = run("from test | eval x = count + 7 | sort x | limit 1");
        logger.info(results);
        // there are no shard, segment, doc_id
        assertThat(
            results.columns().stream().map(ColumnInfo::name).toList(),
            contains("color", "count", "count_d", "data", "data_d", "time", "x")
        );
        var values = results.values();
        Assert.assertEquals(1, values.size());
        var row = values.get(0);
        logger.info(row);
        // x is the last one
        var position = results.columns().size() - 1;
        assertEquals(47, (long) row.get(position));
    }

    public void testFromStatsEval() {
        EsqlQueryResponse results = run("from test | stats avg_count = avg(count) | eval x = avg_count + 7");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(2, results.values().get(0).size());
        assertEquals(50, (double) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("x", "double"))), 1d);
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
        EsqlQueryResponse results = run(
            "from test | stats avg_count = avg(count) | eval x = avg_count + 7",
            Settings.builder().put("add_task_parallelism_above_query", true).build()
        );
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
        EsqlQueryResponse results = run("from test | project count | where count > 40");
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

    public void testStatsWhere() {
        EsqlQueryResponse results = run("from test | stats x = avg(count) | where x > 100");
        logger.info(results);
        Assert.assertEquals(0, results.values().size());
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
        EsqlQueryResponse results = run("from test | project x = count, y = count");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), contains(new ColumnInfo("x", "long"), new ColumnInfo("y", "long")));
        for (List<Object> values : results.values()) {
            assertThat((Long) values.get(0), greaterThanOrEqualTo(40L));
            assertThat(values.get(1), is(values.get(0)));
        }
    }

    public void testProjectRenameEval() {
        EsqlQueryResponse results = run("from test | project x = count, y = count | eval x2 = x + 1 | eval y2 = y + 2");
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
        EsqlQueryResponse results = run("from test | project x = count, y = count | eval z = x + y | project x, y, z");
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
        EsqlQueryResponse results = run("from test | project count, data = count");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
        assertThat(results.columns(), contains(new ColumnInfo("count", "long"), new ColumnInfo("data", "long")));
        for (List<Object> values : results.values()) {
            assertThat(values.get(1), is(values.get(0)));
        }
    }

    public void testRefreshSearchIdleShards() throws Exception {
        String indexName = "test_refresh";
        ElasticsearchAssertions.assertAcked(
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
        ElasticsearchAssertions.assertAcked(
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
            assertEquals(Double.NaN, (double) results.values().get(0).get(0), 0.01d);
        }
    }

    public void testExtractFields() throws Exception {
        String indexName = "test_extract_fields";
        ElasticsearchAssertions.assertAcked(
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

    // @AwaitsFix(bugUrl = "#322")
    public void testEvalWithNull() {
        EsqlQueryResponse results = run("from test | project * | eval nullsum = count_d + null | sort nullsum | limit 1");
        logger.info(results);
        Assert.assertEquals(7, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("nullsum", results.columns().get(6).name());
        assertEquals("double", results.columns().get(6).type());
        assertEquals(7, results.values().get(0).size());
        assertNull(results.values().get(0).get(6));
    }

    public void testEvalWithNullAndAvg() {
        EsqlQueryResponse results = run("from test | eval nullsum = count_d + null | stats avg(nullsum)");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("avg(nullsum)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(Double.NaN, results.values().get(0).get(0));
    }

    public void testEvalWithNullAndCount() {
        EsqlQueryResponse results = run("from test | eval nullsum = count_d + null | stats count(nullsum)");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("count(nullsum)", results.columns().get(0).name());
        assertEquals("long", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(0L, results.values().get(0).get(0));
    }

    public void testFromStatsLimit() {
        EsqlQueryResponse results = run("from test | stats ac = avg(count) by data | limit 1");
        logger.info(results);
        assertThat(results.columns(), contains(new ColumnInfo("ac", "double"), new ColumnInfo("data", "long")));
        assertThat(results.values(), contains(anyOf(contains(42d, 1L), contains(44d, 2L))));
    }

    public void testFromLimit() {
        EsqlQueryResponse results = run("from test | project data | limit 2");
        logger.info(results);
        assertThat(results.columns(), contains(new ColumnInfo("data", "long")));
        assertThat(results.values(), contains(anyOf(contains(1L), contains(2L)), anyOf(contains(1L), contains(2L))));
    }

    static EsqlQueryResponse run(String esqlCommands) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(randomPragmas()).get();
    }

    static EsqlQueryResponse run(String esqlCommands, Settings pragmas) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(pragmas).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EsqlPlugin.class);
    }

    private static Settings randomPragmas() {
        Settings.Builder settings = Settings.builder();
        // pragmas are only enabled on snapshot builds
        if (Build.CURRENT.isSnapshot()) {
            if (randomBoolean()) {
                settings.put("add_task_parallelism_above_query", randomBoolean());
            }
            if (randomBoolean()) {
                settings.put("task_concurrency", randomLongBetween(1, 10));
            }
            if (randomBoolean()) {
                settings.put("buffer_max_pages", randomLongBetween(32, 2048));
            }
            if (randomBoolean()) {
                settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
            }
        }
        return settings.build();
    }
}

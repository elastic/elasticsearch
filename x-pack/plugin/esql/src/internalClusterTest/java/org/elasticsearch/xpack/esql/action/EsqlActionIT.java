/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.greaterThan;

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
                .setMapping("time", "type=date")
                .get()
        );
        long timestamp = epoch;
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("1" + i).source("data", 1, "count", 40, "data_d", 1d, "count_d", 40d, "time", timestamp++))
                .add(new IndexRequest("test").id("2" + i).source("data", 2, "count", 42, "data_d", 2d, "count_d", 42d, "time", timestamp++))
                .add(new IndexRequest("test").id("3" + i).source("data", 1, "count", 44, "data_d", 1d, "count_d", 44d, "time", timestamp++))
                .add(new IndexRequest("test").id("4" + i).source("data", 2, "count", 46, "data_d", 2d, "count_d", 46d, "time", timestamp++))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow("test");
    }

    public void testRow() {
        int value = randomIntBetween(0, Integer.MAX_VALUE);
        EsqlQueryResponse response = run("row" + value);
        assertEquals(List.of(List.of(value)), response.values());
    }

    public void testFromStats() {
        testFromStatsImpl("from test | stats avg(count)", "avg(count)");
    }

    public void testFromStatsWithAlias() {
        testFromStatsImpl("from test | stats f1 = avg(count)", "f1");
    }

    private void testFromStatsImpl(String command, String expectedFieldName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals(expectedFieldName, results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(43, (double) results.values().get(0).get(0), 1d);
    }

    @AwaitsFix(bugUrl = "line 1:45: Unknown column [data]")
    public void testFromStatsGroupingWithSort() {  // FIX ME
        testFromStatsGroupingImpl("from test | stats avg(count) by data | sort data | limit 2", "avg(count)", "data");
    }

    public void testFromStatsGrouping() {
        testFromStatsGroupingImpl("from test | stats avg(count) by data", "avg(count)", "data");
    }

    public void testFromStatsGroupingWithAliases() {
        testFromStatsGroupingImpl("from test | eval g = data | stats f = avg(count) by g", "f", "g");
    }

    private void testFromStatsGroupingImpl(String command, String expectedFieldName, String expectedGroupName) {
        EsqlQueryResponse results = run(command);
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());

        // assert column metadata
        ColumnInfo groupColumn = results.columns().get(0);
        assertEquals(expectedGroupName, groupColumn.name());
        assertEquals("long", groupColumn.type());
        ColumnInfo valuesColumn = results.columns().get(1);
        assertEquals(expectedFieldName, valuesColumn.name());
        assertEquals("double", valuesColumn.type());

        // assert column values
        List<List<Object>> valueValues = results.values();
        assertEquals(2, valueValues.size());
        // This is loathsome, find a declarative way to assert the expected output.
        if ((long) valueValues.get(0).get(0) == 1L) {
            assertEquals(42, (double) valueValues.get(0).get(1), 1d);
            assertEquals(2L, (long) valueValues.get(1).get(0));
            assertEquals(44, (double) valueValues.get(1).get(1), 1d);
        } else if ((long) valueValues.get(0).get(0) == 2L) {
            assertEquals(42, (double) valueValues.get(1).get(1), 1d);
            assertEquals(1L, (long) valueValues.get(1).get(0));
            assertEquals(44, (double) valueValues.get(0).get(1), 1d);
        } else {
            fail("Unexpected group value: " + valueValues.get(0).get(0));
        }
    }

    // Grouping where the groupby field is of a date type.
    public void testFromStatsGroupingByDate() {
        EsqlQueryResponse results = run("from test | stats avg(count) by time");
        logger.info(results);
        Assert.assertEquals(2, results.columns().size());
        Assert.assertEquals(40, results.values().size());

        // assert column metadata
        assertEquals("time", results.columns().get(0).name());
        assertEquals("date", results.columns().get(0).type());
        assertEquals("avg(count)", results.columns().get(1).name());
        assertEquals("double", results.columns().get(1).type());

        // assert column values
        List<Long> expectedValues = LongStream.range(0, 40).map(i -> epoch + i).sorted().boxed().toList();
        List<Long> actualValues = IntStream.range(0, 40).mapToLong(i -> (Long) results.values().get(i).get(0)).sorted().boxed().toList();
        assertEquals(expectedValues, actualValues);
    }

    public void testFrom() {
        EsqlQueryResponse results = run("from test");
        logger.info(results);
        Assert.assertEquals(40, results.values().size());
    }

    public void testFromSortLimit() {
        EsqlQueryResponse results = run("from test | sort count | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(40, (long) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("count", "long"))));
    }

    public void testFromEvalSortLimit() {
        EsqlQueryResponse results = run("from test | eval x = count + 7 | sort x | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(47, (long) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("x", "long"))));
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
        assertEquals(0.96d, (double) results.values().get(0).get(0), 0.01d);
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

    private EsqlQueryResponse run(String esqlCommands) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).get();
    }

    private EsqlQueryResponse run(String esqlCommands, Settings pragmas) {
        return new EsqlQueryRequestBuilder(client(), EsqlQueryAction.INSTANCE).query(esqlCommands).pragmas(pragmas).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(EsqlPlugin.class);
    }
}

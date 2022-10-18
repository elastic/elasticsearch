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

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.greaterThan;

@Experimental
@ESIntegTestCase.ClusterScope(scope = SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(value = "org.elasticsearch.xpack.esql.session:DEBUG", reason = "to better understand planning")
public class EsqlActionIT extends ESIntegTestCase {

    @Before
    public void setupIndex() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", ESTestCase.randomIntBetween(1, 5)))
                .get()
        );
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("1" + i).source("data", 1, "count", 42, "data_d", 1d, "count_d", 42d))
                .add(new IndexRequest("test").id("2" + i).source("data", 2, "count", 44, "data_d", 2d, "count_d", 44d))
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
        EsqlQueryResponse results = run("from test | stats avg(count)");
        logger.info(results);
        Assert.assertEquals(1, results.columns().size());
        Assert.assertEquals(1, results.values().size());
        assertEquals("avg(count)", results.columns().get(0).name());
        assertEquals("double", results.columns().get(0).type());
        assertEquals(1, results.values().get(0).size());
        assertEquals(43, (double) results.values().get(0).get(0), 1d);
    }

    public void testFrom() {
        EsqlQueryResponse results = run("from test");
        logger.info(results);
        Assert.assertEquals(20, results.values().size());
    }

    public void testFromSortLimit() {
        EsqlQueryResponse results = run("from test | sort count | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(42, (long) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("count", "long"))));
    }

    public void testFromEvalSortLimit() {
        EsqlQueryResponse results = run("from test | eval x = count + 7 | sort x | limit 1");
        logger.info(results);
        Assert.assertEquals(1, results.values().size());
        assertEquals(49, (long) results.values().get(0).get(results.columns().indexOf(new ColumnInfo("x", "long"))));
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.condition.script.ExecutableScriptCondition;
import org.elasticsearch.watcher.condition.script.ScriptCondition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.watch.Payload;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.messy.tests.MessyTestUtils.getScriptServiceProxy;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;

public class GroovyScriptConditionIT extends AbstractWatcherIntegrationTestCase {

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(GroovyPlugin.class);
        return types;
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    private static ThreadPool THREAD_POOL;
    private ScriptServiceProxy scriptService;

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new ThreadPool(GroovyScriptConditionIT.class.getSimpleName());
    }

    @Before
    public void init() throws Exception {
        scriptService = getScriptServiceProxy(THREAD_POOL);
    }

    @AfterClass
    public static void stopThreadPool() throws InterruptedException {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGroovyClosureWithAggregations() throws Exception {
        client().admin().indices().prepareCreate(".monitoring")
                .addMapping("cluster_stats", "_timestamp", "enabled=true")
                .get();

        for (int seconds = 0; seconds < 60; seconds += 5) {
            client().prepareIndex(".monitoring", "cluster_stats").setTimestamp("2005-01-01T00:00:" +
                    String.format(Locale.ROOT, "%02d", seconds)).setSource("status", randomFrom("green", "yellow")).get();
        }

        refresh();

        SearchRequestBuilder builder = client().prepareSearch(".monitoring")
                .addAggregation(
                        AggregationBuilders
                                .dateHistogram("minutes").field("_timestamp").interval(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS))
                                .order(Histogram.Order.COUNT_DESC)
                                .subAggregation(AggregationBuilders.terms("status").field("status.keyword").size(3)));
        SearchResponse unmetResponse = builder.get();

        ExecutableScriptCondition condition =
                new ExecutableScriptCondition(new ScriptCondition(Script.inline(
                        String.join(
                                " ",
                                "if (ctx.payload.hits.total < 1) return false;",
                                "def rows = ctx.payload.hits.hits;",
                                "if (ctx.payload.aggregations.minutes.buckets.size() < 12) return false;",
                                "def last60Seconds = ctx.payload.aggregations.minutes.buckets[-12..-1];",
                                "return last60Seconds.every { it.status.buckets.every { s -> s.key == 'red' } }"
                        )
                ).lang("groovy").build()), logger, scriptService);

        WatchExecutionContext unmetContext = mockExecutionContext("_name", new Payload.XContent(unmetResponse));
        assertFalse(condition.execute(unmetContext).met());

        for (int seconds = 0; seconds < 60; seconds += 5) {
            client().prepareIndex(".monitoring", "cluster_stats").setTimestamp("2005-01-01T00:01:" +
                    String.format(Locale.ROOT, "%02d", seconds)).setSource("status", randomFrom("red")).get();
        }

        refresh();

        SearchResponse metResponse = builder.get();

        WatchExecutionContext metContext = mockExecutionContext("_name", new Payload.XContent(metResponse));
        assertTrue(condition.execute(metContext).met());
    }

}

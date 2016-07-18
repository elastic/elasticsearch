/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.watcher.condition.script.ExecutableScriptCondition;
import org.elasticsearch.xpack.watcher.condition.script.ScriptCondition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.WatcherScript;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.watch.Payload;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.messy.tests.MessyTestUtils.createScriptService;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;

public class GroovyScriptConditionIT extends AbstractWatcherIntegrationTestCase {

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(GroovyPlugin.class);
        return types;
    }

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    private static ThreadPool THREAD_POOL;
    private ScriptService scriptService;

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(GroovyScriptConditionIT.class.getSimpleName());
    }

    @Before
    public void init() throws Exception {
        scriptService = createScriptService(THREAD_POOL);
    }

    @AfterClass
    public static void stopThreadPool() throws InterruptedException {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGroovyClosureWithAggregations() throws Exception {
        for (int seconds = 0; seconds < 60; seconds += 5) {
            String timestamp = "2005-01-01T00:00:" + String.format(Locale.ROOT, "%02d", seconds);
            client().prepareIndex(".monitoring", "cluster_stats")
                    .setSource("status", randomFrom("green", "yellow"), "@timestamp", timestamp).get();
        }

        refresh();

        SearchRequestBuilder builder = client().prepareSearch(".monitoring")
                .addAggregation(
                        AggregationBuilders
                                .dateHistogram("minutes").field("@timestamp").interval(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS))
                                .order(Histogram.Order.COUNT_DESC)
                                .subAggregation(AggregationBuilders.terms("status").field("status.keyword").size(3)));
        SearchResponse unmetResponse = builder.get();

        ExecutableScriptCondition condition =
                new ExecutableScriptCondition(new ScriptCondition(WatcherScript.inline(
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
            String timestamp = "2005-01-01T00:01:" + String.format(Locale.ROOT, "%02d", seconds);
            client().prepareIndex(".monitoring", "cluster_stats").setSource("status", randomFrom("red"), "@timestamp", timestamp).get();
        }

        refresh();

        SearchResponse metResponse = builder.get();

        WatchExecutionContext metContext = mockExecutionContext("_name", new Payload.XContent(metResponse));
        assertTrue(condition.execute(metContext).met());
    }

}

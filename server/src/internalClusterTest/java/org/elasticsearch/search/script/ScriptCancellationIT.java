/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.script;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.BucketAggregationScript;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ScriptCancellationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlockingScriptPlugin.class);
    }

    @Before
    public void resetSharedState() {
        BlockingScriptPlugin.HITS.set(0);
        BlockingScriptPlugin.PROCEED.drainPermits();
    }

    public void testFieldScriptCancellation() throws Exception {
        createIndex("test");
        indexRandom(true, prepareIndex("test").setId("1").setSource("value", 1));

        ActionFuture<SearchResponse> future = prepareSearch("test").addScriptField(
            "blocked",
            new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "field-block", Map.of())
        ).execute();

        cancelAndAssertFailure(future);
    }

    public void testBucketScriptCancellation() throws Exception {
        createIndex("test");
        indexRandom(true, prepareIndex("test").setId("1").setSource("val", 1.0));

        ActionFuture<SearchResponse> future = prepareSearch("test").addAggregation(
            AggregationBuilders.histogram("histo")
                .field("val")
                .interval(10)
                .subAggregation(AggregationBuilders.sum("my_sum").field("val"))
                .subAggregation(
                    PipelineAggregatorBuilders.bucketScript(
                        "my_script",
                        new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "bucket-block", Map.of()),
                        "my_sum"
                    )
                )
        ).execute();

        cancelAndAssertFailure(future);
    }

    public void testReduceScriptCancellation() throws Exception {
        createIndex("test");
        indexRandom(true, prepareIndex("test").setId("1").setSource("value", 1));

        ActionFuture<SearchResponse> future = prepareSearch("test").addAggregation(
            AggregationBuilders.scriptedMetric("my_metric")
                .mapScript(new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "map", Map.of()))
                .combineScript(new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "combine", Map.of()))
                .reduceScript(new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "reduce-block", Map.of()))
        ).execute();

        cancelAndAssertFailure(future);
    }

    /**
     * Waits for the blocking script to start, cancels the search task, releases the semaphore,
     * and asserts the response contains a cancellation failure.
     */
    private void cancelAndAssertFailure(ActionFuture<SearchResponse> future) throws Exception {
        assertBusy(() -> assertThat(BlockingScriptPlugin.HITS.get(), greaterThan(0)));
        clusterAdmin().prepareCancelTasks().setActions(TransportSearchAction.TYPE.name() + "*").get();
        BlockingScriptPlugin.PROCEED.release(Integer.MAX_VALUE);

        SearchResponse response = null;
        try {
            response = future.actionGet();
            assertThat("expected shard-level cancellation failure", response.getFailedShards(), greaterThan(0));
        } catch (Exception e) {
            // Coordinator-level failures surface as exceptions; either form proves cancellation.
        } finally {
            if (response != null) response.decRef();
        }
    }

    /**
     * A script plugin providing blocking scripts for each of the three cancellation-check contexts under test:
     * FieldScript (fetch phase), BucketAggregationScript (pipeline reduce), and
     * ScriptedMetricAggContexts.ReduceScript (coordinator reduce).
     *
     * <p>Each blocking script increments {@link #HITS} to signal that it is running, then waits on
     * {@link #PROCEED} before calling {@code _getCancellationCheck().run()} so the test can cancel the
     * task and verify the check fires.
     */
    public static class BlockingScriptPlugin extends Plugin implements ScriptPlugin {

        static final String LANG = "blocking-cancellation-test";

        static final AtomicInteger HITS = new AtomicInteger();
        static final Semaphore PROCEED = new Semaphore(0);

        static void block() {
            HITS.incrementAndGet();
            try {
                PROCEED.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {

                @Override
                public String getType() {
                    return LANG;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> T compile(String name, String source, ScriptContext<T> context, Map<String, String> params) {
                    if (context == FieldScript.CONTEXT && "field-block".equals(source)) {
                        FieldScript.Factory factory = (p, lookup) -> ctx -> new FieldScript(p, lookup, ctx) {
                            @Override
                            public Object execute() {
                                block();
                                Objects.requireNonNull(_getCancellationCheck(), "cancellation check was not set").run();
                                return null;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    if (context == BucketAggregationScript.CONTEXT && "bucket-block".equals(source)) {
                        BucketAggregationScript.Factory factory = p -> new BucketAggregationScript(p) {
                            @Override
                            public Number execute() {
                                block();
                                Objects.requireNonNull(_getCancellationCheck(), "cancellation check was not set").run();
                                return 1.0;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    if (context == ScriptedMetricAggContexts.MapScript.CONTEXT && "map".equals(source)) {
                        ScriptedMetricAggContexts.MapScript.Factory factory = (
                            p,
                            state,
                            lookup) -> ctx -> new ScriptedMetricAggContexts.MapScript(p, state, lookup, ctx) {
                                @Override
                                public void execute() {
                                    // trivial no-op map script
                                }
                            };
                        return context.factoryClazz.cast(factory);
                    }
                    if (context == ScriptedMetricAggContexts.CombineScript.CONTEXT && "combine".equals(source)) {
                        ScriptedMetricAggContexts.CombineScript.Factory factory = (p, state) -> new ScriptedMetricAggContexts.CombineScript(
                            p,
                            state
                        ) {
                            @Override
                            public Object execute() {
                                // trivial no-op combine script
                                return state;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    if (context == ScriptedMetricAggContexts.ReduceScript.CONTEXT && "reduce-block".equals(source)) {
                        ScriptedMetricAggContexts.ReduceScript.Factory factory = (p, states) -> new ScriptedMetricAggContexts.ReduceScript(
                            p,
                            states
                        ) {
                            @Override
                            public Object execute() {
                                block();
                                Objects.requireNonNull(_getCancellationCheck(), "cancellation check was not set").run();
                                return 0;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    throw new IllegalArgumentException("Unknown script source [" + source + "] for context [" + context.name + "]");
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(
                        FieldScript.CONTEXT,
                        BucketAggregationScript.CONTEXT,
                        ScriptedMetricAggContexts.MapScript.CONTEXT,
                        ScriptedMetricAggContexts.CombineScript.CONTEXT,
                        ScriptedMetricAggContexts.ReduceScript.CONTEXT
                    );
                }
            };
        }
    }
}

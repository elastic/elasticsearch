/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.BucketAggregationSelectorScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests verifying that {@code _setCancellationCheck} is properly wired for
 * {@link BucketAggregationSelectorScript} (via {@link BucketSelectorPipelineAggregator}) and
 * {@link MovingFunctionScript} (via {@link MovFnPipelineAggregator}).
 *
 * <p>Each test installs a blocking script that parks on a semaphore, then cancels the search task
 * and releases the semaphore.  After release the script calls {@code _getCancellationCheck().run()},
 * which must throw {@link org.elasticsearch.tasks.TaskCancelledException} if (and only if) the
 * cancellation check was properly set by the aggregator.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ScriptCancellationIT extends ESIntegTestCase {

    /** Counts the number of times the blocking script has been entered. */
    static final AtomicInteger HITS = new AtomicInteger();

    /** The blocking script parks here until the test releases it. */
    static final Semaphore PROCEED = new Semaphore(0);

    @After
    public void resetState() {
        HITS.set(0);
        PROCEED.drainPermits();
    }

    /** Increments {@link #HITS} then blocks until {@link #PROCEED} has a permit. */
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AggregationsPlugin.class, BlockingScriptPlugin.class);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Verifies that {@link BucketSelectorPipelineAggregator} calls
     * {@code executableScript._setCancellationCheck(cancellationCheck)} so that when the search task
     * is cancelled the script's cancellation check throws.
     */
    public void testBucketSelectorCancellation() throws Exception {
        createIndex("test");
        prepareIndex("test").setSource("val", 1.0).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ActionFuture<SearchResponse> future = client().prepareSearch("test")
            .addAggregation(
                histogram("histo").field("val")
                    .interval(10)
                    .subAggregation(sum("my_sum").field("val"))
                    .subAggregation(
                        new BucketSelectorPipelineAggregationBuilder(
                            "my_selector",
                            Map.of("my_sum_var", "my_sum"),
                            new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "selector-block", Map.of())
                        )
                    )
            )
            .execute();

        assertBusy(() -> assertThat(HITS.get(), greaterThan(0)));

        clusterAdmin().prepareCancelTasks().setActions(TransportSearchAction.TYPE.name() + "*").get();

        PROCEED.release(Integer.MAX_VALUE);

        assertCancelled(future);
    }

    /**
     * Verifies that {@link MovFnPipelineAggregator} calls
     * {@code executableScript._setCancellationCheck(cancellationCheck)} so that when the search task
     * is cancelled the script's cancellation check throws.
     */
    public void testMovingFunctionCancellation() throws Exception {
        createIndex("test");
        prepareIndex("test").setSource("date", "2024-01-01", "val", 1.0).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        prepareIndex("test").setSource("date", "2024-01-02", "val", 2.0).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ActionFuture<SearchResponse> future = client().prepareSearch("test")
            .addAggregation(
                dateHistogram("histo").field("date")
                    .calendarInterval(DateHistogramInterval.DAY)
                    .subAggregation(sum("my_sum").field("val"))
                    .subAggregation(
                        new MovFnPipelineAggregationBuilder(
                            "my_movfn",
                            "my_sum",
                            new Script(ScriptType.INLINE, BlockingScriptPlugin.LANG, "moving-fn-block", Map.of()),
                            5
                        )
                    )
            )
            .execute();

        assertBusy(() -> assertThat(HITS.get(), greaterThan(0)));

        clusterAdmin().prepareCancelTasks().setActions(TransportSearchAction.TYPE.name() + "*").get();

        PROCEED.release(Integer.MAX_VALUE);

        assertCancelled(future);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Asserts that the search completed with a cancellation-related failure (HTTP 400 / BAD_REQUEST).
     * The response may either throw an exception or return with shard failures.
     */
    private static void assertCancelled(ActionFuture<SearchResponse> future) {
        SearchResponse response = null;
        try {
            response = future.actionGet();
            assertNotEquals("Expected at least one shard failure from task cancellation", 0, response.getFailedShards());
        } catch (Exception ex) {
            // Coordinator-level cancellation surfaces as an exception; any failure proves the script was interrupted.
            assertThat(ExceptionsHelper.status(ex), equalTo(RestStatus.BAD_REQUEST));
        } finally {
            if (response != null) response.decRef();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Blocking script plugin
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * A {@link Plugin} / {@link ScriptPlugin} that provides a custom {@link ScriptEngine} with two
     * blocking scripts:
     * <ul>
     *   <li>{@code "selector-block"} – implements {@link BucketAggregationSelectorScript}</li>
     *   <li>{@code "moving-fn-block"} – implements {@link MovingFunctionScript}</li>
     * </ul>
     * Both scripts increment {@link ScriptCancellationIT#HITS}, park on {@link ScriptCancellationIT#PROCEED},
     * and then call {@code _getCancellationCheck().run()} to exercise the wired cancellation hook.
     */
    public static class BlockingScriptPlugin extends Plugin implements ScriptPlugin {

        public static final String LANG = "blocking-aggs-test";

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return LANG;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context == BucketAggregationSelectorScript.CONTEXT) {
                        BucketAggregationSelectorScript.Factory factory = p -> new BucketAggregationSelectorScript(p) {
                            @Override
                            public boolean execute() {
                                block();
                                Objects.requireNonNull(_getCancellationCheck(), "cancellation check was not set").run();
                                return true;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    if (context == MovingFunctionScript.CONTEXT) {
                        MovingFunctionScript.Factory factory = () -> new MovingFunctionScript() {
                            @Override
                            public double execute(Map<String, Object> scriptParams, double[] values) {
                                block();
                                Objects.requireNonNull(_getCancellationCheck(), "cancellation check was not set").run();
                                return 0.0;
                            }
                        };
                        return context.factoryClazz.cast(factory);
                    }
                    throw new IllegalArgumentException("Unsupported script context [" + context.name + "] for language [" + LANG + "]");
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(BucketAggregationSelectorScript.CONTEXT, MovingFunctionScript.CONTEXT);
                }
            };
        }
    }
}

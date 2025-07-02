/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.SuiteScopeTestCase
public class FiltersCancellationIT extends ESIntegTestCase {

    private static final String INDEX = "idx";
    private static final String SLEEP_FIELD = "sleep";
    private static final String NUMERIC_FIELD = "value";

    private static final int NUM_DOCS = 100_000;
    /**
     * The number of milliseconds to sleep in the script that simulates a long-running operation.
     * <p>
     *     As CancellableBulkScorer does a minimum of 4096 docs per batch, this number must be low to avoid long test times.
     * </p>
     */
    private static final long SLEEP_SCRIPT_MS = 1;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), pausableFieldPluginClass());
    }

    protected Class<? extends Plugin> pausableFieldPluginClass() {
        return SleepScriptPlugin.class;
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject(SLEEP_FIELD);
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", SleepScriptPlugin.PAUSE_SCRIPT_LANG).endObject();
            }
            mapping.endObject();
            mapping.startObject(NUMERIC_FIELD);
            {
                mapping.field("type", "long");
            }
            mapping.endObject();
        }
        mapping.endObject();
        client().admin().indices().prepareCreate(INDEX).setMapping(mapping.endObject()).get();

        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(prepareIndex(INDEX).setId(Integer.toString(i)).setSource(NUMERIC_FIELD, i));
        }
        bulk.get();
        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
    }

    public void testFiltersCountCancellation() throws Exception {
        ensureProperCancellation(
            client().prepareSearch(INDEX)
                .addAggregation(
                    filters(
                        "filters",
                        new KeyedFilter[] {
                            new KeyedFilter("filter1", termQuery(SLEEP_FIELD, 1)),
                            new KeyedFilter("filter2", termQuery(SLEEP_FIELD, 2)) }
                    )
                )
        );
    }

    public void testFiltersSubAggsCancellation() throws Exception {
        ensureProperCancellation(
            client().prepareSearch(INDEX)
                .addAggregation(
                    filters(
                        "filters",
                        new KeyedFilter[] {
                            new KeyedFilter("filter1", termQuery(SLEEP_FIELD, 1)),
                            new KeyedFilter("filter2", termQuery(SLEEP_FIELD, 2)) }
                    ).subAggregation(terms("sub").field(SLEEP_FIELD))
                )
        );
    }

    private void ensureProperCancellation(SearchRequestBuilder searchRequestBuilder) throws Exception {
        var searchRequestFuture = searchRequestBuilder.setTimeout(TimeValue.timeValueSeconds(1)).execute();
        assertFalse(searchRequestFuture.isCancelled());
        assertFalse(searchRequestFuture.isDone());

        // Check that there are search tasks running
        assertThat(getSearchTasks(), not(empty()));

        // Wait to ensure scripts started executing and that we don't cancel too early
        safeSleep(2000);

        // CancellableBulkScorer does a starting batch of 4096 items, x2 after each iteration.
        // That times SLEEP_SCRIPT_MS gives us the maximum time to wait (x2 to avoid flakiness)
        long maxWaitMs = 3 * (4096 * SLEEP_SCRIPT_MS);

        // Cancel the tasks
        client().admin()
            .cluster()
            .prepareCancelTasks()
            .setActions(TransportSearchAction.NAME + "*")
            .waitForCompletion(true)
            .setTimeout(TimeValue.timeValueMillis(maxWaitMs))
            .get();

        // Ensure the search request finished and that there are no more search tasks
        assertBusy(() -> {
            assertThat(getSearchTasks(), empty());
            assertTrue(searchRequestFuture.isDone());
        }, maxWaitMs, TimeUnit.MILLISECONDS);
    }

    private List<TaskInfo> getSearchTasks() {
        return client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransportSearchAction.NAME + "*")
            .setDetailed(true)
            .get()
            .getTasks();
    }

    public static class SleepScriptPlugin extends Plugin implements ScriptPlugin {
        public static final String PAUSE_SCRIPT_LANG = "pause";

        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return PAUSE_SCRIPT_LANG;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <FactoryType> FactoryType compile(
                    String name,
                    String code,
                    ScriptContext<FactoryType> context,
                    Map<String, String> params
                ) {
                    if (context == LongFieldScript.CONTEXT) {
                        return (FactoryType) new LongFieldScript.Factory() {
                            @Override
                            public LongFieldScript.LeafFactory newFactory(
                                String fieldName,
                                Map<String, Object> params,
                                SearchLookup searchLookup,
                                OnScriptError onScriptError
                            ) {
                                return ctx -> new LongFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                                    @Override
                                    public void execute() {
                                        try {
                                            Thread.sleep(SLEEP_SCRIPT_MS);
                                        } catch (InterruptedException e) {
                                            throw new AssertionError(e);
                                        }
                                        emit(1);
                                    }
                                };
                            }
                        };
                    }
                    throw new IllegalStateException("unsupported type " + context);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Set.of(LongFieldScript.CONTEXT);
                }
            };
        }
    }
}

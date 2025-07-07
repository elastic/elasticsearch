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
import java.util.concurrent.Semaphore;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.SuiteScopeTestCase
public class FiltersCancellationIT extends ESIntegTestCase {

    private static final String INDEX = "idx";
    private static final String PAUSE_FIELD = "pause";
    private static final String NUMERIC_FIELD = "value";

    private static final int NUM_DOCS = 100_000;
    private static final int SEMAPHORE_PERMITS = NUM_DOCS - 1000;
    private static final Semaphore SCRIPT_SEMAPHORE = new Semaphore(0);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), pausableFieldPluginClass());
    }

    protected Class<? extends Plugin> pausableFieldPluginClass() {
        return PauseScriptPlugin.class;
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        try (XContentBuilder mapping = JsonXContent.contentBuilder()) {
            mapping.startObject();
            mapping.startObject("runtime");
            {
                mapping.startObject(PAUSE_FIELD);
                {
                    mapping.field("type", "long");
                    mapping.startObject("script").field("source", "").field("lang", PauseScriptPlugin.PAUSE_SCRIPT_LANG).endObject();
                }
                mapping.endObject();
                mapping.startObject(NUMERIC_FIELD);
                {
                    mapping.field("type", "long");
                }
                mapping.endObject();
            }
            mapping.endObject();
            mapping.endObject();

            client().admin().indices().prepareCreate(INDEX).setMapping(mapping).get();
        }

        int DOCS_PER_BULK = 100_000;
        for (int i = 0; i < NUM_DOCS; i += DOCS_PER_BULK) {
            BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int j = 0; j < DOCS_PER_BULK; j++) {
                int docId = i + j;
                bulk.add(prepareIndex(INDEX).setId(Integer.toString(docId)).setSource(NUMERIC_FIELD, docId));
            }
            bulk.get();
        }

        client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
    }

    public void testFiltersCountCancellation() throws Exception {
        ensureProperCancellation(
            client().prepareSearch(INDEX)
                .addAggregation(
                    filters(
                        "filters",
                        new KeyedFilter[] {
                            new KeyedFilter("filter1", termQuery(PAUSE_FIELD, 1)),
                            new KeyedFilter("filter2", termQuery(PAUSE_FIELD, 2)) }
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
                            new KeyedFilter("filter1", termQuery(PAUSE_FIELD, 1)),
                            new KeyedFilter("filter2", termQuery(PAUSE_FIELD, 2)) }
                    ).subAggregation(terms("sub").field(PAUSE_FIELD))
                )
        );
    }

    private void ensureProperCancellation(SearchRequestBuilder searchRequestBuilder) throws Exception {
        var searchRequestFuture = searchRequestBuilder.setTimeout(TimeValue.timeValueSeconds(1)).execute();
        assertFalse(searchRequestFuture.isCancelled());
        assertFalse(searchRequestFuture.isDone());

        // Check that there are search tasks running
        assertThat(getSearchTasks(), not(empty()));

        // Wait for the script field to get blocked
        assertBusy(() -> { assertThat(SCRIPT_SEMAPHORE.getQueueLength(), greaterThan(0)); });

        // Cancel the tasks
        // Warning: Adding a waitForCompletion(true)/execute() here sometimes causes tasks to not get canceled and threads to get stuck
        client().admin().cluster().prepareCancelTasks().setActions(TransportSearchAction.NAME + "*").get();

        SCRIPT_SEMAPHORE.release(SEMAPHORE_PERMITS);

        // Ensure the search request finished and that there are no more search tasks
        assertBusy(() -> {
            assertTrue(searchRequestFuture.isDone());
            assertThat(getSearchTasks(), empty());
        });
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

    public static class PauseScriptPlugin extends Plugin implements ScriptPlugin {
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
                                            SCRIPT_SEMAPHORE.acquire();
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

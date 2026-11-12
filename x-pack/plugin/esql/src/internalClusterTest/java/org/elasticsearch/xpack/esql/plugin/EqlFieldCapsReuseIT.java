/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Proves the field-caps dedup actually fires: ES|QL already resolves the EQL pattern's field-caps on the coordinator and
 * hands the merged response to the EQL delegate, so the EQL engine issues NO {@code _field_caps} request of its own. An
 * {@link ActionFilter} counts every {@code field_caps} action invocation (it intercepts local executions too), so a count
 * of 0 for a local EQL query is the discriminator: if the retention/attach/inject chain silently fell back, the EQL engine
 * would resolve field-caps itself and the count would be at least 1. (ES|QL's own resolution goes through
 * {@code EsqlResolveFieldsAction}, which calls the merge directly and is not counted here.)
 */
public class EqlFieldCapsReuseIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "eql_reuse";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), EqlPlugin.class),
            FieldCapsCounterPlugin.class
        );
    }

    @Before
    public void setupIndex() {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "process.name", "type=keyword")
        );
        client().prepareBulk()
            .add(
                new IndexRequest(INDEX).id("p1")
                    .source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "process.name", "cmd.exe")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testLocalEqlQueryReusesCoordinatorFieldCaps() {
        FieldCapsCounterPlugin.FIELD_CAPS_CALLS.set(0);
        try (EsqlQueryResponse resp = run("EQL " + INDEX + " \"process where true\"")) {
            assertThat(getValuesList(resp), hasSize(1));
        }
        assertThat(
            "the EQL engine must not issue its own field_caps — ES|QL's resolution is reused",
            FieldCapsCounterPlugin.FIELD_CAPS_CALLS.get(),
            equalTo(0)
        );
    }

    public void testStandaloneEqlSearchStillResolvesFieldCaps() {
        // Discriminator for the counter above: a plain EQL search (no ES|QL, nothing injected) MUST self-resolve, so
        // the filter counts at least one field_caps call. If this saw 0, the counter would be broken and the reuse
        // assertion meaningless.
        FieldCapsCounterPlugin.FIELD_CAPS_CALLS.set(0);
        EqlSearchRequest request = new EqlSearchRequest().indices(INDEX).query("process where true");
        client().execute(EqlSearchAction.INSTANCE, request).actionGet().decRef();
        assertThat(
            "a standalone EQL search resolves field_caps itself",
            FieldCapsCounterPlugin.FIELD_CAPS_CALLS.get(),
            greaterThanOrEqualTo(1)
        );
    }

    public static class FieldCapsCounterPlugin extends Plugin implements ActionPlugin {
        static final AtomicInteger FIELD_CAPS_CALLS = new AtomicInteger();

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (TransportFieldCapabilitiesAction.NAME.equals(action)) {
                        FIELD_CAPS_CALLS.incrementAndGet();
                    }
                    chain.proceed(task, action, request, listener);
                }
            });
        }
    }
}

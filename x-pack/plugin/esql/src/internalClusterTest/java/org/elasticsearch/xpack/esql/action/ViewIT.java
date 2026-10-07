/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.view.PutViewAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class ViewIT extends AbstractEsqlIntegTestCase {

    /**
     * Registers a historic (not net-new) system index, so that accessing it without a system origin only issues a deprecation warning.
     */
    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        private static final String SYSTEM_INDEX_NAME = ".system-index";

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                    .setDescription("test system index for views")
                    .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return "view-it-system-index";
        }

        @Override
        public String getFeatureDescription() {
            return "test system index for views";
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(ReindexPlugin.class);
        plugins.add(TestSystemIndexPlugin.class);
        return plugins;
    }

    public void testIndicesAreNotValidateUponCreation() {
        assertAcked(createView("my_view", "FROM not-validated"));
    }

    public void testInvalidSyntaxQueryIsRejected() {
        expectThrows(ElasticsearchException.class, containsString("mismatched input"), () -> createView("my_view", "NOT VALID ESQL $$$$"));
    }

    public void testSetIsRejected() {
        expectThrows(
            ElasticsearchException.class,
            containsString("SET statements are not allowed in views"),
            () -> createView("my_view", "SET time_zone=\"Europe/Berlin\"; FROM index")
        );
    }

    public void testCannotCreateAliasToView() {
        assertAcked(createView("my-view", "FROM not-validated"));

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view]"),
            () -> indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("my-view").alias("some-alias"))
                .get()
        );
    }

    public void testViewOverAlias() {
        assertAcked(indicesAdmin().prepareCreate("source-index").setMapping("f1", "type=integer"));
        prepareIndex("source-index").setSource("f1", 42).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index("source-index").alias("source-alias"))
                .get()
        );

        assertAcked(createView("alias-view", "FROM source-alias | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM alias-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    public void testViewOverSystemIndex() {
        prepareIndex(TestSystemIndexPlugin.SYSTEM_INDEX_NAME).setSource("f1", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertAcked(createView("system-view", "FROM " + TestSystemIndexPlugin.SYSTEM_INDEX_NAME + " | KEEP f1"));

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        ThreadContext threadContext = internalCluster().getInstance(TransportService.class, coordinator.getName())
            .getThreadPool()
            .getThreadContext();

        Tuple<List<String>, List<List<Object>>> result = safeAwait(listener -> {
            client(coordinator.getName()).filterWithHeader(Map.of(SystemIndices.SYSTEM_INDEX_ACCESS_CONTROL_HEADER_KEY, "false"))
                .execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest("FROM system-view"), listener.map(r -> {
                    List<String> warnings = threadContext.getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .map(w -> HeaderWarning.decodeAndUnescape(HeaderWarning.extractWarningValueFromWarningHeader(w, false)))
                        .toList();
                    return Tuple.tuple(warnings, getValuesList(r));
                }));
        });

        assertThat(
            result.v1(),
            hasItem(containsString("this request accesses system indices: [" + TestSystemIndexPlugin.SYSTEM_INDEX_NAME + "]"))
        );
        assertThat(result.v2(), equalTo(List.of(List.of(42L))));
    }

    public void testViewOverDataStream() throws IOException {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("ds-view-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of("logs-view-test*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .template(Template.builder().mappings(new CompressedXContent("""
                            {
                              "properties": {
                                "@timestamp": { "type": "date" },
                                "f1": { "type": "integer" }
                              }
                            }""")))
                        .build()
                )
            )
        );

        String time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
        prepareIndex("logs-view-test").setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("@timestamp", time, "f1", 42)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(createView("ds-view", "FROM logs-view-test | KEEP f1"));

        try (EsqlQueryResponse response = run("FROM ds-view")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(42))));
        }
    }

    public void testViewCannotBeReindexSource() {
        assertAcked(createView("my-view", "FROM not-validated"));
        assertAcked(indicesAdmin().prepareCreate("dest-index"));

        expectThrows(
            IndexNotFoundException.class,
            containsString("no such index [my-view]"),
            () -> client().execute(ReindexAction.INSTANCE, new ReindexRequest().setSourceIndices("my-view").setDestIndex("dest-index"))
                .actionGet()
        );
    }

    public void testViewIsInvisibleInFieldCaps() {
        assertAcked(indicesAdmin().prepareCreate("my-index"));
        assertAcked(createView("my-view", "FROM my-index"));

        String[] indices = client().prepareFieldCaps("*").setFields("*").get().getIndices();
        assertThat(List.of(indices), contains("my-index"));
    }

    private AcknowledgedResponse createView(String viewName, String query) {
        return client().execute(
            PutViewAction.INSTANCE,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(viewName, query))
        ).actionGet(30, TimeUnit.SECONDS);
    }
}

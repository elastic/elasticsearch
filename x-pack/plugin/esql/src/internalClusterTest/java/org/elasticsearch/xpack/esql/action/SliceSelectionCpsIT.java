/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * The slices a source reads when cross-project search is enabled. Index names are then resolved across the origin project
 * and its linked projects, none of which are configured here: the test covers the resolution and planning of a source in
 * that mode, on the origin project. A linked project is queried like a remote cluster, which
 * {@link CrossClusterSliceSelectionIT} covers.
 */
public class SliceSelectionCpsIT extends AbstractEsqlIntegTestCase {

    private static final Map<String, Integer> DOCS_PER_SLICE = Map.of("acme", 4, "globex", 3);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ViewOverDatasetCpsIT.CpsSettingPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("serverless.cross_project.enabled", true).build();
    }

    @Before
    public void setupIndices() {
        assumeTrue("requires slice selection", EsqlCapabilities.Cap.SLICE_SELECTION_FROM_FILTER.isEnabled());
        assertAcked(
            prepareCreate("tenants").setSettings(
                Settings.builder().put("index.number_of_shards", 3).put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping("description", "type=text")
        );
        assertAcked(prepareCreate("reference").setMapping("description", "type=text"));
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DOCS_PER_SLICE.forEach((slice, docs) -> {
            for (int i = 0; i < docs; i++) {
                bulk.add(
                    new IndexRequest("tenants").id(Integer.toString(i))
                        .source("description", "a document of " + slice)
                        .routing(slice)
                        .setRoutingFromSlice(true)
                );
            }
        });
        // the test framework may require a routing value on any index
        bulk.add(new IndexRequest("reference").source("description", "a document of reference").routing("any"));
        assertNoFailures(bulk.get());
    }

    public void testFilterSelectsSlices() {
        try (EsqlQueryResponse response = run("FROM tenants METADATA _slice | WHERE _slice == \"acme\" | STATS c = COUNT(*)")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) DOCS_PER_SLICE.get("acme")))));
        }
        try (
            EsqlQueryResponse response = run("FROM tenants, reference METADATA _slice | WHERE _slice == \"globex\" | STATS c = COUNT(*)")
        ) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) DOCS_PER_SLICE.get("globex")))));
        }
        try (EsqlQueryResponse response = run("FROM tenants, reference | STATS c = COUNT(*)")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(8L))));
        }
    }

    public void testSearchFunction() {
        String match = "| WHERE MATCH(description, \"document\") ";
        try (EsqlQueryResponse response = run("FROM tenants METADATA _slice " + match + "AND _slice == \"acme\" | STATS c = COUNT(*)")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of((long) DOCS_PER_SLICE.get("acme")))));
        }
        try (EsqlQueryResponse response = run("FROM ten*, reference " + match + "| STATS c = COUNT(*)")) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(8L))));
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that update-by-query accepts the slice as a {@code /{index}/{_slice}/_update_by_query} path segment and only updates
 * documents of the requested slices.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class UpdateByQuerySliceEnabledIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testUpdateByQueryWithSlicePath() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String index = "ubq-slice";
        createSliceIndexWithDocs(index);

        ObjectPath response = updateByQuery(index, "tenant-b");
        assertThat(response.evaluate("updated"), equalTo(1));
        assertThat(response.evaluate("failures"), empty());

        response = updateByQuery(index, SliceIndexing.SLICE_ALL);
        assertThat(response.evaluate("updated"), equalTo(3));
        assertThat(response.evaluate("failures"), empty());
        assertHitCount(prepareSearch(index).setSize(0), 3L);
    }

    public void testUpdateByQueryWithMultipleSlicesPath() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String index = "ubq-multi-slice";
        createSliceIndexWithDocs(index);

        ObjectPath response = updateByQuery(index, "tenant-a,tenant-b");
        assertThat(response.evaluate("updated"), equalTo(3));
        assertThat(response.evaluate("failures"), empty());
        assertHitCount(prepareSearch(index).setSize(0), 3L);
    }

    public void testSlicePathOnNonSliceIndexMatchesNothing() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String index = "ubq-plain";
        assertAcked(prepareCreate(index).setSettings(Settings.builder().put("number_of_replicas", 0)).get());
        client().index(new IndexRequest(index).id("1").source("value", "a")).get();
        indicesAdmin().prepareRefresh(index).get();

        assertThat(updateByQuery(index, "tenant-a").evaluate("updated"), equalTo(0));
        assertHitCount(prepareSearch(index).setSize(0), 1L);
    }

    private void createSliceIndexWithDocs(String index) throws Exception {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put("index.slice.enabled", true)
                    .put("number_of_shards", randomIntBetween(1, 3))
                    .put("number_of_replicas", 0)
            ).get()
        );
        client().index(new IndexRequest(index).id("1").routing("tenant-a").setRoutingFromSlice(true).source("value", "a")).get();
        client().index(new IndexRequest(index).id("2").routing("tenant-a").setRoutingFromSlice(true).source("value", "a")).get();
        client().index(new IndexRequest(index).id("3").routing("tenant-b").setRoutingFromSlice(true).source("value", "b")).get();
        indicesAdmin().prepareRefresh(index).get();
    }

    private ObjectPath updateByQuery(String index, String slice) throws IOException {
        Request request = new Request("POST", "/" + index + "/" + slice + "/_update_by_query");
        request.addParameter("refresh", "true");
        request.setJsonEntity("""
            {"query": {"match_all": {}}}""");
        return ObjectPath.createFromResponse(getRestClient().performRequest(request));
    }
}

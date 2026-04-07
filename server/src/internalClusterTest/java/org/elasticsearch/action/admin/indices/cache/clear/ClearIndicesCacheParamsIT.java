/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.cache.clear;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ClearIndicesCacheParamsIT extends ESIntegTestCase {

    private static final String INDEX = "test";

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, getTestTransportPlugin());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), true)
            .build();
    }

    public void testNoParamsClearsAll() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should be cleared", getQueryCacheMemory(), equalTo(0L));
        assertThat("request cache should be cleared", getRequestCacheMemory(), equalTo(0L));
        assertThat("fielddata should be cleared", getFieldDataMemory(), equalTo(0L));
    }

    public void testClearOnlyQueryCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?query=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should be cleared", getQueryCacheMemory(), equalTo(0L));
        assertThat("request cache should not be cleared", getRequestCacheMemory(), greaterThan(0L));
        assertThat("fielddata should not be cleared", getFieldDataMemory(), greaterThan(0L));
    }

    public void testClearOnlyRequestCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?request=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should not be cleared", getQueryCacheMemory(), greaterThan(0L));
        assertThat("request cache should be cleared", getRequestCacheMemory(), equalTo(0L));
        assertThat("fielddata should not be cleared", getFieldDataMemory(), greaterThan(0L));
    }

    public void testClearOnlyFielddataCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?fielddata=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should not be cleared", getQueryCacheMemory(), greaterThan(0L));
        assertThat("request cache should not be cleared", getRequestCacheMemory(), greaterThan(0L));
        assertThat("fielddata should be cleared", getFieldDataMemory(), equalTo(0L));
    }

    public void testClearQueryAndRequestCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?query=true&request=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should be cleared", getQueryCacheMemory(), equalTo(0L));
        assertThat("request cache should be cleared", getRequestCacheMemory(), equalTo(0L));
        assertThat("fielddata should not be cleared", getFieldDataMemory(), greaterThan(0L));
    }

    public void testClearQueryAndFielddataCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?query=true&fielddata=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should be cleared", getQueryCacheMemory(), equalTo(0L));
        assertThat("request cache should not be cleared", getRequestCacheMemory(), greaterThan(0L));
        assertThat("fielddata should be cleared", getFieldDataMemory(), equalTo(0L));
    }

    public void testClearRequestAndFielddataCache() throws Exception {
        createTestIndex();
        populateAllCaches();

        Request request = new Request("POST", "/" + INDEX + "/_cache/clear?request=true&fielddata=true");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        assertThat("query cache should not be cleared", getQueryCacheMemory(), greaterThan(0L));
        assertThat("request cache should be cleared", getRequestCacheMemory(), equalTo(0L));
        assertThat("fielddata should be cleared", getFieldDataMemory(), equalTo(0L));
    }

    private void createTestIndex() {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
                        .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping("field", "type=text,fielddata=true")
        );
        ensureGreen(INDEX);

        prepareIndex(INDEX).setId("1").setSource("field", "value").get();
        indicesAdmin().prepareRefresh(INDEX).get();
    }

    private void populateAllCaches() {
        // Populate query cache
        prepareSearch(INDEX).setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("field", "value"))).get().decRef();
        assertThat(getQueryCacheMemory(), greaterThan(0L));

        // Populate fielddata
        prepareSearch(INDEX).addSort("field", SortOrder.ASC).get().decRef();
        assertThat(getFieldDataMemory(), greaterThan(0L));

        // Populate request cache
        assertHitCount(prepareSearch(INDEX).setSearchType(SearchType.QUERY_THEN_FETCH).setSize(0).setRequestCache(true), 1);
        assertThat(getRequestCacheMemory(), greaterThan(0L));
    }

    private long getQueryCacheMemory() {
        IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX).setQueryCache(true).get();
        return stats.getTotal().getQueryCache().getMemorySizeInBytes();
    }

    private long getRequestCacheMemory() {
        IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX).setRequestCache(true).get();
        return stats.getTotal().getRequestCache().getMemorySizeInBytes();
    }

    private long getFieldDataMemory() {
        IndicesStatsResponse stats = indicesAdmin().prepareStats(INDEX).setFieldData(true).get();
        return stats.getTotal().getFieldData().getMemorySizeInBytes();
    }

}

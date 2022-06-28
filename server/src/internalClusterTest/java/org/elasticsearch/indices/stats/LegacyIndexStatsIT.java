/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LegacyIndexStatsIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testFieldDataFieldsParam() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test1")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_6_0_0))
                .addMapping("_doc", "bar", "type=text,fielddata=true", "baz", "type=text,fielddata=true")
                .get()
        );

        ensureGreen();

        client().prepareIndex("test1", "_doc", Integer.toString(1)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        client().prepareIndex("test1", "_doc", Integer.toString(2)).setSource("{\"bar\":\"bar\",\"baz\":\"baz\"}", XContentType.JSON).get();
        refresh();

        client().prepareSearch("_all").addSort("bar", SortOrder.ASC).addSort("baz", SortOrder.ASC).execute().actionGet();

        final IndicesStatsRequestBuilder builder = client().admin().indices().prepareStats();

        {
            final IndicesStatsResponse stats = builder.execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields(), is(nullValue()));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("bar", "baz").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("baz"), greaterThan(0L));
        }

        {
            final IndicesStatsResponse stats = builder.setFieldDataFields("*r").execute().actionGet();
            assertThat(stats.getTotal().fieldData.getMemorySizeInBytes(), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("bar"), is(true));
            assertThat(stats.getTotal().fieldData.getFields().get("bar"), greaterThan(0L));
            assertThat(stats.getTotal().fieldData.getFields().containsField("baz"), is(false));
        }

    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SyntheticSourceIT extends AbstractEsqlIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
    }

    public void testMatchOnlyText() throws Exception {
        createIndex(b -> b.field("type", "match_only_text"));

        int numDocs = between(10, 1000);
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder indexRequest = prepareIndex("test").setSource("id", "i" + i, "field", "n" + i);
            if (randomInt(100) < 5) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.get();
        }
        client().admin().indices().prepareRefresh("test").get();

        try (EsqlQueryResponse resp = run("from test | sort id asc | limit 1")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo("n0"));
            assertThat(row.next(), equalTo("i0"));
            assertFalse(row.hasNext());
        }
    }

    public void testText() throws Exception {
        createIndex(b -> b.field("type", "text").field("store", true));

        int numDocs = between(10, 1000);
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder indexRequest = client().prepareIndex("test").setSource("id", "i" + i, "field", "n" + i);
            if (randomInt(100) < 5) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.get();
        }
        client().admin().indices().prepareRefresh("test").get();
        try (EsqlQueryResponse resp = run("from test | keep field, id | sort id asc | limit 1")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo("n0"));
            assertThat(row.next(), equalTo("i0"));
            assertFalse(row.hasNext());
        }
    }

    private void createIndex(CheckedFunction<XContentBuilder, XContentBuilder, IOException> fieldMapping) throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("properties");
            mapping.startObject("id").field("type", "keyword").endObject();
            mapping.startObject("field");
            fieldMapping.apply(mapping);
            mapping.endObject();
            mapping.endObject();
        }
        mapping.endObject();

        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.mapping.source.mode", "synthetic");

        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings).setMapping(mapping));
    }
}

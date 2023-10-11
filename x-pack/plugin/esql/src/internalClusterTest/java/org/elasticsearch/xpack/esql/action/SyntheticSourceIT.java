/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

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
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        if (true || randomBoolean()) {
            mapping.startObject("_source");
            mapping.field("mode", "synthetic");
            mapping.endObject();
        }
        {
            mapping.startObject("properties");
            mapping.startObject("uid");
            mapping.field("type", "keyword");
            mapping.endObject();
            mapping.startObject("name");
            mapping.field("type", "match_only_text");
            mapping.endObject();
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        int numDocs = between(10, 1000);
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder indexRequest = client().prepareIndex("test").setSource("uid", "u" + i);
            if (randomInt(100) < 5) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.get();
        }
        client().admin().indices().prepareRefresh("test").get();
        try (EsqlQueryResponse resp = run("from test | keep uid, name | sort uid asc | limit 1")) {
            Iterator<Object> row = resp.values().next();
            assertThat(row.next(), equalTo("u0"));
            assertNull(row.next());
        }
    }
}

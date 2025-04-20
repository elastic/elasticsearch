/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class SyntheticSourceIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(PainlessPlugin.class);
    }

    public void testSearchUsingRuntimeField() throws Exception {
        createIndex();

        int numDocs = between(1000, 5000);
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder indexRequest = client().prepareIndex("test").setSource("id", "" + i);
            if (randomInt(100) < 5) {
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            indexRequest.get();
        }
        client().admin().indices().prepareRefresh("test").get();
        assertNoFailures(client().prepareSearch("test").setQuery(QueryBuilders.rangeQuery("long_id").from(0)));
    }

    private void createIndex() throws IOException {
        XContentBuilder mapping = JsonXContent.contentBuilder();
        mapping.startObject();
        {
            mapping.startObject("_source");
            mapping.field("mode", "synthetic");
            mapping.endObject();
        }
        {
            mapping.startObject("runtime");
            mapping.startObject("long_id");
            mapping.field("type", "long");
            mapping.field("script", "emit(Long.parseLong(params._source.id));");
            mapping.endObject();
            mapping.endObject();
            mapping.startObject("properties");
            mapping.startObject("id").field("type", "keyword").endObject();
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping).get());
    }
}

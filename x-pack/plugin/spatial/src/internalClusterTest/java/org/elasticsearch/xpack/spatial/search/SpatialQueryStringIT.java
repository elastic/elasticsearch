/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;

public class SpatialQueryStringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateSpatialPlugin.class);
    }

    @Before
    public void setup() {
        String mapping = """
            {
              "settings": {
                "index": {
                  "number_of_shards": 1,
                  "number_of_replicas": 0
                }
              },
              "mappings": {
                "_doc": {
                  "properties": {
                    "geo_shape": {"type": "geo_shape"},
                    "shape": {"type": "shape"},
                    "point": {"type": "point"}
                    }
                  }
                }
              }
            }

            """;
        prepareCreate("test").setSource(mapping, XContentType.JSON).get();
        ensureGreen("test");
    }

    public void testBasicAllQuery() throws Exception {
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(
            client().prepareIndex("test").setId("1").setSource("geo_shape", "POINT(0 0)", "shape", "POINT(0 0)", "point", "POINT(0 0)")
        );
        // nothing matches
        indexRandom(true, false, reqs);
        SearchResponse resp = client().prepareSearch("test").setQuery(queryStringQuery("foo")).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("\"2015/09/02\"")).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("127.0.0.1 OR 1.8")).get();
        assertHitCount(resp, 0L);

        resp = client().prepareSearch("test").setQuery(queryStringQuery("POINT(0 0)")).get();
        assertHitCount(resp, 0L);

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("POINT(0 0)").field("geo_shape")).get()
        );
        assertThat(e.getCause().getMessage(), containsString("Field [geo_shape] of type [geo_shape] does not support match queries"));

        e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("POINT(0 0)").field("shape")).get()
        );
        assertThat(e.getCause().getMessage(), containsString("Field [shape] of type [shape] does not support match queries"));

        e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("test").setQuery(queryStringQuery("POINT(0 0)").field("point")).get()
        );
        assertThat(e.getCause().getMessage(), containsString("Field [point] of type [point] does not support match queries"));

        resp = client().prepareSearch("test").setQuery(queryStringQuery("POINT(0 0)").field("*shape")).get();
        assertHitCount(resp, 0L);
    }
}

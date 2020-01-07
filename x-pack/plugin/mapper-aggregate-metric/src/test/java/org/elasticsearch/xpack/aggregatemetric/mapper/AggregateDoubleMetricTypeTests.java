/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Names.METRICS;

public class AggregateDoubleMetricTypeTests extends ESSingleNodeTestCase {

    public static final String METRICS_FIELD = METRICS.getPreferredName();
    public static final String CONTENT_TYPE = AggregateDoubleMetricFieldMapper.CONTENT_TYPE;
    public static final String DEFAULT_METRIC = AggregateDoubleMetricFieldMapper.Names.DEFAULT_METRIC.getPreferredName();
    public static final String TEST_IDX = "test";

    public void testExistsQuery() throws Exception {
        ensureGreen();

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max" })
            .field(DEFAULT_METRIC, "max")
            .endObject().endObject()
            .endObject().endObject();
        createIndex(TEST_IDX);

        PutMappingRequest request = new PutMappingRequest(TEST_IDX).source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();

        int numDocs = 1000;
        for (int i =0; i < numDocs; i ++) {
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", i)
                .field("max", i + 1000)
                .endObject().endObject();
            client().prepareIndex(TEST_IDX).setSource(doc).get();
        }
        client().admin().indices().refresh(new RefreshRequest(TEST_IDX)).get();

        SearchResponse response = client().prepareSearch(TEST_IDX)
            .setQuery(new ExistsQueryBuilder("metric")).get();
        assertEquals(numDocs, response.getHits().getTotalHits().value);

        response = client().prepareSearch(TEST_IDX)
            .setQuery(new ExistsQueryBuilder("non_existent_")).get();
        assertEquals(0, response.getHits().getTotalHits().value);
    }


    public void testTermQuery() throws Exception {
        ensureGreen();

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max" })
            .field(DEFAULT_METRIC, "max")
            .endObject().endObject()
            .endObject().endObject();
        createIndex(TEST_IDX);

        PutMappingRequest request = new PutMappingRequest(TEST_IDX).source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();

        int numDocs = 1000;
        for (int i =0; i < numDocs; i ++) {
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", i)
                .field("max", i + 1000)
                .endObject().endObject();
            client().prepareIndex(TEST_IDX).setSource(doc).get();
        }
        client().admin().indices().refresh(new RefreshRequest(TEST_IDX)).get();

        SearchResponse response = client().prepareSearch(TEST_IDX)
            .setQuery(new TermQueryBuilder("metric", 1100)).get();
        assertEquals(1, response.getHits().getTotalHits().value);

        // TODO: Check why this test fails
//        response = client().prepareSearch(TEST_IDX)
//            .setQuery(new TermQueryBuilder("metric._max", 1100)).get();
//        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testRangeQuery() throws Exception {
        ensureGreen();

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject().startObject("_doc")
            .startObject("properties").startObject("metric")
            .field("type", CONTENT_TYPE)
            .field(METRICS_FIELD,  new String[] {"min", "max" })
            .field(DEFAULT_METRIC, "max")
            .endObject().endObject()
            .endObject().endObject();
        createIndex(TEST_IDX);

        PutMappingRequest request = new PutMappingRequest(TEST_IDX).source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();

        int numDocs = 1000;
        for (int i =0; i < numDocs; i ++) {
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject().startObject("metric")
                .field("min", i)
                .field("max", i + 1000)
                .endObject().endObject();
            client().prepareIndex(TEST_IDX).setSource(doc).get();
        }
        client().admin().indices().refresh(new RefreshRequest(TEST_IDX)).get();

        SearchResponse response = client().prepareSearch(TEST_IDX)
            .setQuery(new RangeQueryBuilder("metric").gte(1500).lte(3000)).get();
        assertEquals(500, response.getHits().getTotalHits().value);

        // TODO: Check why this test fails
//        response = client().prepareSearch(TEST_IDX)
//            .setQuery(new RangeQueryBuilder("metric._max").gte(1500).lte(3000)).get();
//        assertEquals(500, response.getHits().getTotalHits().value);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AggregateMetricMapperPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }
}

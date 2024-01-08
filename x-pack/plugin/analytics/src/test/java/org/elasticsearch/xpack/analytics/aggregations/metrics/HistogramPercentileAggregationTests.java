/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleHistogramIterationValue;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.boxplot.Boxplot;
import org.elasticsearch.xpack.analytics.boxplot.BoxplotAggregationBuilder;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class HistogramPercentileAggregationTests extends ESSingleNodeTestCase {

    public void testHDRHistogram() throws Exception {

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("data")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("raw");
        PutMappingRequest request = new PutMappingRequest("raw").source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();

        XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("data")
            .field("type", "histogram")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("pre_agg");
        PutMappingRequest request2 = new PutMappingRequest("pre_agg").source(xContentBuilder2);
        client().admin().indices().putMapping(request2).actionGet();

        int numberOfSignificantValueDigits = TestUtil.nextInt(random(), 1, 5);
        DoubleHistogram histogram = new DoubleHistogram(numberOfSignificantValueDigits);
        BulkRequest bulkRequest = new BulkRequest();

        int numDocs = 10000;
        int frq = 1000;

        for (int i = 0; i < numDocs; i++) {
            double value = random().nextDouble();
            XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("data", value).endObject();
            bulkRequest.add(new IndexRequest("raw").source(doc));
            histogram.recordValue(value);
            if ((i + 1) % frq == 0) {
                client().bulk(bulkRequest);
                bulkRequest = new BulkRequest();
                List<Double> values = new ArrayList<>();
                List<Integer> counts = new ArrayList<>();
                Iterator<DoubleHistogramIterationValue> iterator = histogram.recordedValues().iterator();
                while (iterator.hasNext()) {
                    DoubleHistogramIterationValue histValue = iterator.next();
                    values.add(histValue.getValueIteratedTo());
                    counts.add(Math.toIntExact(histValue.getCountAtValueIteratedTo()));
                }
                XContentBuilder preAggDoc = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("data")
                    .field("values", values.toArray(new Double[values.size()]))
                    .field("counts", counts.toArray(new Integer[counts.size()]))
                    .endObject()
                    .endObject();
                prepareIndex("pre_agg").setSource(preAggDoc).get();
                histogram.reset();
            }
        }
        client().admin().indices().refresh(new RefreshRequest("raw", "pre_agg")).get();

        assertHitCount(client().prepareSearch("raw").setTrackTotalHits(true), numDocs);

        assertHitCount(client().prepareSearch("pre_agg"), numDocs / frq);

        PercentilesAggregationBuilder builder = AggregationBuilders.percentiles("agg")
            .field("data")
            .method(PercentilesMethod.HDR)
            .numberOfSignificantValueDigits(numberOfSignificantValueDigits)
            .percentiles(10);

        assertResponse(
            client().prepareSearch("raw").addAggregation(builder),
            responseRaw -> assertResponse(
                client().prepareSearch("pre_agg").addAggregation(builder),
                responsePreAgg -> assertResponse(client().prepareSearch("pre_agg", "raw").addAggregation(builder), responseBoth -> {
                    InternalHDRPercentiles percentilesRaw = responseRaw.getAggregations().get("agg");
                    InternalHDRPercentiles percentilesPreAgg = responsePreAgg.getAggregations().get("agg");
                    InternalHDRPercentiles percentilesBoth = responseBoth.getAggregations().get("agg");
                    for (int i = 1; i < 100; i++) {
                        assertEquals(percentilesRaw.percentile(i), percentilesPreAgg.percentile(i), 0.0);
                        assertEquals(percentilesRaw.percentile(i), percentilesBoth.percentile(i), 0.0);
                    }
                })
            )
        );
    }

    private void setupTDigestHistogram(int compression) throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("inner")
            .startObject("properties")
            .startObject("data")
            .field("type", "double")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("raw");
        PutMappingRequest request = new PutMappingRequest("raw").source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();

        XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("inner")
            .startObject("properties")
            .startObject("data")
            .field("type", "histogram")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("pre_agg");
        PutMappingRequest request2 = new PutMappingRequest("pre_agg").source(xContentBuilder2);
        client().admin().indices().putMapping(request2).actionGet();

        TDigestState histogram = TDigestState.create(compression);
        BulkRequest bulkRequest = new BulkRequest();

        int numDocs = 10000;
        int frq = 1000;

        for (int i = 0; i < numDocs; i++) {
            double value = random().nextDouble();
            XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("inner")
                .field("data", value)
                .endObject()
                .endObject();
            bulkRequest.add(new IndexRequest("raw").source(doc));
            histogram.add(value);
            if ((i + 1) % frq == 0) {
                client().bulk(bulkRequest);
                bulkRequest = new BulkRequest();
                List<Double> values = new ArrayList<>();
                List<Long> counts = new ArrayList<>();
                Collection<Centroid> centroids = histogram.centroids();
                for (Centroid centroid : centroids) {
                    values.add(centroid.mean());
                    counts.add(centroid.count());
                }
                XContentBuilder preAggDoc = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("inner")
                    .startObject("data")
                    .field("values", values.toArray(new Double[values.size()]))
                    .field("counts", counts.toArray(new Long[counts.size()]))
                    .endObject()
                    .endObject()
                    .endObject();
                prepareIndex("pre_agg").setSource(preAggDoc).get();
                histogram = TDigestState.create(compression);
            }
        }
        client().admin().indices().refresh(new RefreshRequest("raw", "pre_agg")).get();

        assertHitCount(client().prepareSearch("raw").setTrackTotalHits(true), numDocs);
        assertHitCount(client().prepareSearch("pre_agg"), numDocs / frq);
    }

    public void testTDigestHistogram() throws Exception {
        int compression = TestUtil.nextInt(random(), 200, 300);
        setupTDigestHistogram(compression);

        PercentilesAggregationBuilder builder = AggregationBuilders.percentiles("agg")
            .field("inner.data")
            .method(PercentilesMethod.TDIGEST)
            .compression(compression)
            .percentiles(10, 25, 50, 75);

        assertResponse(
            client().prepareSearch("raw").addAggregation(builder),
            responseRaw -> assertResponse(
                client().prepareSearch("pre_agg").addAggregation(builder),
                responsePreAgg -> assertResponse(client().prepareSearch("raw", "pre_agg").addAggregation(builder), responseBoth -> {
                    InternalTDigestPercentiles percentilesRaw = responseRaw.getAggregations().get("agg");
                    InternalTDigestPercentiles percentilesPreAgg = responsePreAgg.getAggregations().get("agg");
                    InternalTDigestPercentiles percentilesBoth = responseBoth.getAggregations().get("agg");
                    for (int i = 1; i < 100; i++) {
                        assertEquals(percentilesRaw.percentile(i), percentilesPreAgg.percentile(i), 1.0);
                        assertEquals(percentilesRaw.percentile(i), percentilesBoth.percentile(i), 1.0);
                    }
                })
            )
        );
    }

    public void testBoxplotHistogram() throws Exception {
        int compression = TestUtil.nextInt(random(), 200, 300);
        setupTDigestHistogram(compression);
        BoxplotAggregationBuilder bpBuilder = new BoxplotAggregationBuilder("agg").field("inner.data").compression(compression);

        assertResponse(
            client().prepareSearch("raw").addAggregation(bpBuilder),
            bpResponseRaw -> assertResponse(
                client().prepareSearch("pre_agg").addAggregation(bpBuilder),
                bpResponsePreAgg -> assertResponse(client().prepareSearch("raw", "pre_agg").addAggregation(bpBuilder), bpResponseBoth -> {
                    Boxplot bpRaw = bpResponseRaw.getAggregations().get("agg");
                    Boxplot bpPreAgg = bpResponsePreAgg.getAggregations().get("agg");
                    Boxplot bpBoth = bpResponseBoth.getAggregations().get("agg");
                    assertEquals(bpRaw.getMax(), bpPreAgg.getMax(), 0.0);
                    assertEquals(bpRaw.getMax(), bpBoth.getMax(), 0.0);
                    assertEquals(bpRaw.getMin(), bpPreAgg.getMin(), 0.0);
                    assertEquals(bpRaw.getMin(), bpBoth.getMin(), 0.0);

                    assertEquals(bpRaw.getQ1(), bpPreAgg.getQ1(), 1.0);
                    assertEquals(bpRaw.getQ1(), bpBoth.getQ1(), 1.0);
                    assertEquals(bpRaw.getQ2(), bpPreAgg.getQ2(), 1.0);
                    assertEquals(bpRaw.getQ2(), bpBoth.getQ2(), 1.0);
                    assertEquals(bpRaw.getQ3(), bpPreAgg.getQ3(), 1.0);
                    assertEquals(bpRaw.getQ3(), bpBoth.getQ3(), 1.0);
                })
            )
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AnalyticsPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

}

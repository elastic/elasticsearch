/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.AbstractLinearCounting;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HyperLogLogPlusPlusFieldMapper;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;


public class HyperLogLogPlusPlusBackedCardinalityAggregationTests extends ESSingleNodeTestCase {

    private static final String TYPE = HyperLogLogPlusPlusFieldMapper.CONTENT_TYPE;
    private static final String HLL = HyperLogLogPlusPlusFieldMapper.HLL_FIELD.getPreferredName();
    private static final String LC = HyperLogLogPlusPlusFieldMapper.LC_FIELD.getPreferredName();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(AnalyticsPlugin.class);
        plugins.add(LocalStateCompositeXPackPlugin.class);
        return plugins;
    }

    public void testCardinalityAggregation() throws Exception {
        final int precision = TestUtil.nextInt(random(), 4, 18);

        final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
             .startObject("_doc")
              .startObject("properties")
               .startObject("data")
                .field("type", "integer")
               .endObject()
              .endObject()
             .endObject()
            .endObject();
        createIndex("raw");
        PutMappingRequest request = new PutMappingRequest("raw").source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();


        final XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
             .startObject("_doc")
              .startObject("properties")
               .startObject("data")
                .field("type", TYPE)
                .field("precision", precision)
               .endObject()
              .endObject()
             .endObject()
            .endObject();
        createIndex("pre_agg");
        PutMappingRequest request2 = new PutMappingRequest("pre_agg").source(xContentBuilder2);
        client().admin().indices().putMapping(request2).actionGet();

        final BiConsumer<BulkRequest, Integer> raw = (bulkRequest, value) -> {
            try {
                XContentBuilder doc = XContentFactory.jsonBuilder()
                    .startObject()
                     .field("data", value)
                    .endObject();
                bulkRequest.add(new IndexRequest("raw").source(doc));
            } catch(IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        };

        final BiConsumer<String, int[]> agg = (type, sketch) -> {
            try {
                XContentBuilder preAggDoc = XContentFactory.jsonBuilder()
                    .startObject()
                     .startObject("data")
                      .field(type, sketch)
                     .endObject()
                    .endObject();
                client().prepareIndex("pre_agg").setSource(preAggDoc).get();
            } catch(IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        };

        doTestCardinalityAggregation(raw, agg, "data", precision);
    }

    public void testCardinalityAggregationWithPath() throws Exception {
        final int precision = TestUtil.nextInt(random(), 4, 18);

        final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
             .startObject("_doc")
              .startObject("properties")
               .startObject("parent")
                .startObject("properties")
                 .startObject("data")
                  .field("type", "integer")
                 .endObject()
                .endObject()
               .endObject()
              .endObject()
             .endObject()
            .endObject();
        createIndex("raw");
        PutMappingRequest request = new PutMappingRequest("raw").source(xContentBuilder);
        client().admin().indices().putMapping(request).actionGet();


        final XContentBuilder xContentBuilder2 = XContentFactory.jsonBuilder()
            .startObject()
             .startObject("_doc")
              .startObject("properties")
               .startObject("parent")
                .startObject("properties")
                 .startObject("data")
                  .field("type", TYPE)
                  .field("precision", precision)
                 .endObject()
                .endObject()
               .endObject()
              .endObject()
             .endObject()
            .endObject();
        createIndex("pre_agg");
        PutMappingRequest request2 = new PutMappingRequest("pre_agg").source(xContentBuilder2);
        client().admin().indices().putMapping(request2).actionGet();

        final BiConsumer<BulkRequest, Integer> raw = (bulkRequest, value) -> {
            try {
                XContentBuilder doc = XContentFactory.jsonBuilder()
                    .startObject()
                     .startObject("parent")
                      .field("data", value)
                     .endObject()
                    .endObject();
                bulkRequest.add(new IndexRequest("raw").source(doc));
            } catch(IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        };

        final BiConsumer<String, int[]> agg = (type, sketch) -> {
            try {
                XContentBuilder preAggDoc = XContentFactory.jsonBuilder()
                    .startObject()
                     .startObject("parent")
                      .startObject("data")
                       .field(type, sketch)
                      .endObject()
                     .endObject()
                    .endObject();
                client().prepareIndex("pre_agg").setSource(preAggDoc).get();
            } catch(IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        };

        doTestCardinalityAggregation(raw, agg, "parent.data", precision);
    }


    private void doTestCardinalityAggregation(BiConsumer<BulkRequest, Integer> rawIngestor,
                                              BiConsumer<String, int[]> aggIngestor,
                                              String field,
                                              int precision) throws Exception {
        final int numDocs = 100000;
        final int frq = randomBoolean() ? 10000 : 1000;
        final int maxValue = randomIntBetween(1, randomBoolean() ? 1000: 100000);

        HyperLogLogPlusPlus counts = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
        BulkRequest bulkRequest = new BulkRequest();

        for (int i =0; i < numDocs; i ++) {
            final int value = randomInt(maxValue);
            final long hash = BitMixer.mix64(value);
            rawIngestor.accept(bulkRequest, value);
            counts.collect(0, hash);
            if ((i + 1) % frq == 0) {
                String type = counts.getAlgorithm(0) == AbstractHyperLogLogPlusPlus.HYPERLOGLOG ? HLL : LC;
                int[] sketch = HLL.equals(type) ?
                    getHyperLogLog(counts.getHyperLogLog(0)) : getLinearCounting(counts.getLinearCounting(0));
                client().bulk(bulkRequest);
                bulkRequest = new BulkRequest();
                aggIngestor.accept(type, sketch);
                counts = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            }
        }

        client().admin().indices().refresh(new RefreshRequest("raw", "pre_agg")).get();

        SearchResponse response = client().prepareSearch("raw").setTrackTotalHits(true).get();
        assertEquals(numDocs, response.getHits().getTotalHits().value);

        response = client().prepareSearch("pre_agg").get();
        assertEquals(numDocs / frq, response.getHits().getTotalHits().value);

        final int aggPrecision = TestUtil.nextInt(random(), 4, precision);

        final CardinalityAggregationBuilder builder =
            AggregationBuilders.cardinality("agg").field(field)
                .precisionThreshold(AbstractHyperLogLog.thresholdFromPrecision(aggPrecision));

        final SearchResponse responseRaw = client().prepareSearch("raw").addAggregation(builder).get();
        final SearchResponse responsePreAgg = client().prepareSearch("pre_agg").addAggregation(builder).get();
        final SearchResponse responseBoth = client().prepareSearch("pre_agg", "raw").addAggregation(builder).get();

        final InternalCardinality cardinalityRaw =  responseRaw.getAggregations().get("agg");
        assertThat(cardinalityRaw.getValue(), Matchers.greaterThan(0L));
        final InternalCardinality cardinalityPreAgg =  responsePreAgg.getAggregations().get("agg");
        assertThat(cardinalityRaw.getValue(), CoreMatchers.equalTo(cardinalityPreAgg.getValue()));
        final InternalCardinality cardinalityBoth =  responseBoth.getAggregations().get("agg");
        assertThat(cardinalityRaw.getValue(), CoreMatchers.equalTo(cardinalityBoth.getValue()));
    }

    private static int[] getHyperLogLog(AbstractHyperLogLog.RunLenIterator iterator) {
        final int m = 1 << iterator.precision();
        final int[] runLens = new int[m];
        for (int i = 0; i < m; i++) {
            iterator.next();
            runLens[i] = iterator.value();
        }
        return runLens;
    }

    private static int[] getLinearCounting(AbstractLinearCounting.EncodedHashesIterator iterator) {
        final int size = iterator.size();
        final int[] encoded = new int[size];
        for (int i = 0; i < size; i++) {
            iterator.next();
            encoded[i] = iterator.value();
        }
        return encoded;
    }
}

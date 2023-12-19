/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.xpack.exponentialhistogram.agg.ExponentialHistogramAggregationBuilder;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;

public class ExponentialHistogramAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testHistograms() throws Exception {
        ExponentialHistogramFieldMapper mapper = new ExponentialHistogramFieldMapper.Builder(FIELD_NAME).build(
            MapperBuilderContext.root(false, false)
        );

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            ExponentialHistogramFieldMapper.ExponentialHistogramBuckets negative =
                new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));
            ExponentialHistogramFieldMapper.ExponentialHistogramBuckets positive =
                new ExponentialHistogramFieldMapper.ExponentialHistogramBuckets(0, List.of(1L, 0L, 0L, 2L));

            w.addDocument(singleton(
                ExponentialHistogramFieldMapper.encodeBinaryDocValuesField(FIELD_NAME, 1, negative, positive)
            ));

            // TODO(axw) set scale
            ExponentialHistogramAggregationBuilder aggBuilder =
                new ExponentialHistogramAggregationBuilder("my_agg").field(FIELD_NAME);
            try (IndexReader reader = w.getReader()) {
                InternalHistogram histogram = searchAndReduce(reader, new AggTestConfig(aggBuilder, defaultFieldType(FIELD_NAME)));
                assertEquals(9, histogram.getBuckets().size());
                /*
                assertEquals(-10d, histogram.getBuckets().get(0).getKey());
                assertEquals(1, histogram.getBuckets().get(0).getDocCount());
                assertEquals(-5d, histogram.getBuckets().get(1).getKey());
                assertEquals(0, histogram.getBuckets().get(1).getDocCount());
                assertEquals(0d, histogram.getBuckets().get(2).getKey());
                assertEquals(3, histogram.getBuckets().get(2).getDocCount());
                assertEquals(5d, histogram.getBuckets().get(3).getKey());
                assertEquals(3, histogram.getBuckets().get(3).getDocCount());
                assertEquals(10d, histogram.getBuckets().get(4).getKey());
                assertEquals(4, histogram.getBuckets().get(4).getDocCount());
                assertEquals(15d, histogram.getBuckets().get(5).getKey());
                assertEquals(0, histogram.getBuckets().get(5).getDocCount());
                assertEquals(20d, histogram.getBuckets().get(6).getKey());
                assertEquals(2, histogram.getBuckets().get(6).getDocCount());
                assertEquals(25d, histogram.getBuckets().get(7).getKey());
                assertEquals(0, histogram.getBuckets().get(7).getDocCount());
                assertEquals(30d, histogram.getBuckets().get(8).getKey());
                assertEquals(1, histogram.getBuckets().get(8).getDocCount());
                assertTrue(AggregationInspectionHelper.hasValue(histogram));
                */
            }
        }
    }

    /*
    private static BinaryDocValuesField exponentialHistogramFieldDocValues(String fieldName, double[] values) throws IOException {
        DoubleHistogram histogram = new DoubleHistogram(3);
        histogram.setAutoResize(true);
        for (double value : values) {
            histogram.recordValue(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        for (DoubleHistogramIterationValue value : histogram.recordedValues()) {
            streamOutput.writeVInt((int) value.getCountAtValueIteratedTo());
            streamOutput.writeDouble(value.getValueIteratedTo());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }
    */

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new ExponentialHistogramAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        return new ExponentialHistogramFieldMapper.ExponentialHistogramFieldType(fieldName, Collections.emptyMap());
    }
}

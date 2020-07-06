/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.movingPercentiles;

import org.HdrHistogram.DoubleHistogram;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;

import java.io.IOException;


public class MovingPercentilesHDRAggregatorTests extends MovingPercentilesAbstractAggregatorTests {

    @Override
    protected PercentilesConfig getPercentileConfig() {
        return new PercentilesConfig.Hdr(1);
    }

    @Override
    protected  void executeTestCase(int window, int shift, Query query,
                                 DateHistogramAggregationBuilder aggBuilder) throws IOException {

        DoubleHistogram[] states = new DoubleHistogram[datasetTimes.size()];
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                int counter = 0;
                for (String date : datasetTimes) {
                    states[counter] = new DoubleHistogram(1);
                    final int numberDocs = randomIntBetween(5, 50);
                    long instant = asLong(date);
                    for (int i =0; i < numberDocs; i++) {
                        if (frequently()) {
                            indexWriter.commit();
                        }
                        double value = randomDoubleBetween(0, 10, true);
                        states[counter].recordValue(value);
                        document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                        document.add(new LongPoint(INSTANT_FIELD, instant));
                        document.add(new NumericDocValuesField(VALUE_FIELD, NumericUtils.doubleToSortableLong(value)));
                        indexWriter.addDocument(document);
                        document.clear();
                    }
                    counter++;
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(aggBuilder.field());
                MappedFieldType valueFieldType
                    = new NumberFieldMapper.NumberFieldType("value_field", NumberFieldMapper.NumberType.DOUBLE);

                InternalDateHistogram histogram;
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, 1000,
                    new MappedFieldType[]{fieldType, valueFieldType});
                for (int i = 0; i < histogram.getBuckets().size(); i++) {
                    InternalDateHistogram.Bucket bucket = histogram.getBuckets().get(i);
                    InternalHDRPercentiles values = bucket.getAggregations().get("MovingPercentiles");
                    DoubleHistogram expected = reduce(i, window, shift, states);
                    if (values == null) {
                        assertNull(expected);
                    } else {
                        DoubleHistogram agg = values.getState();
                        assertEquals(expected.getTotalCount(), agg.getTotalCount());
                        assertEquals(expected.getMaxValue(), agg.getMaxValue(), 0d);
                        assertEquals(expected.getMinValue(), agg.getMinValue(), 0d);
                    }
                }
            }
        }
    }

    private DoubleHistogram reduce(int index, int window, int shift, DoubleHistogram[] buckets) {
        int fromIndex = clamp(index - window + shift, buckets.length);
        int toIndex = clamp(index + shift, buckets.length);
        if (fromIndex == toIndex) {
            return null;
        }
        DoubleHistogram result = new DoubleHistogram(buckets[0].getNumberOfSignificantValueDigits());
        for (int i = fromIndex; i < toIndex; i++) {
            result.add(buckets[i]);
        }
        return result;
    }
}

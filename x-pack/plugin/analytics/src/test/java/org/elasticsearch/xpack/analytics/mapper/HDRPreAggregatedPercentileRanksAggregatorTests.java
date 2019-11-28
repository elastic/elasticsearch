/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.mapper;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.DoubleHistogramIterationValue;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Iterator;

public class HDRPreAggregatedPercentileRanksAggregatorTests extends AggregatorTestCase {

    private BinaryDocValuesField getDocValue(String fieldName, double[] values) throws IOException {
        DoubleHistogram histogram = new DoubleHistogram(3);//default
        for (double value : values) {
            histogram.recordValue(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        DoubleHistogram.RecordedValues recordedValues = histogram.recordedValues();
        Iterator<DoubleHistogramIterationValue> iterator = recordedValues.iterator();
        while (iterator.hasNext()) {
            DoubleHistogramIterationValue value = iterator.next();
            long count = value.getCountAtValueIteratedTo();
            streamOutput.writeVInt(Math.toIntExact(count));
            double d = value.getValueIteratedTo();
            streamOutput.writeDouble(d);
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    public void testSimple() throws IOException {
        try (Directory dir = newDirectory();
                RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(getDocValue("field", new double[] {3, 0.2, 10}));
            w.addDocument(doc);

            PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("my_agg", new double[]{0.1, 0.5, 12})
                    .field("field")
                    .method(PercentilesMethod.HDR);
            MappedFieldType fieldType = new HistogramFieldMapper.Builder("field").fieldType();
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                PercentileRanks ranks = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                Iterator<Percentile> rankIterator = ranks.iterator();
                Percentile rank = rankIterator.next();
                assertEquals(0.1, rank.getValue(), 0d);
                assertThat(rank.getPercent(), Matchers.equalTo(0d));
                rank = rankIterator.next();
                assertEquals(0.5, rank.getValue(), 0d);
                assertThat(rank.getPercent(), Matchers.greaterThan(0d));
                assertThat(rank.getPercent(), Matchers.lessThan(100d));
                rank = rankIterator.next();
                assertEquals(12, rank.getValue(), 0d);
                assertThat(rank.getPercent(), Matchers.equalTo(100d));
                assertFalse(rankIterator.hasNext());
                assertTrue(AggregationInspectionHelper.hasValue((InternalHDRPercentileRanks)ranks));
            }
        }
    }

}

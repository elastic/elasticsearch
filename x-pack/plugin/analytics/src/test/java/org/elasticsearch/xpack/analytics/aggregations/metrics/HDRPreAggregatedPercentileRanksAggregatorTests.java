/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

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
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.PercentileRanks;
import org.elasticsearch.search.aggregations.metrics.PercentileRanksAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class HDRPreAggregatedPercentileRanksAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new PercentileRanksAggregationBuilder("hdr_percentiles", new double[]{1.0})
            .field(fieldName)
            .percentilesConfig(new PercentilesConfig.Hdr());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // Note: this is the same list as Core, plus Analytics
        return List.of(CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.BOOLEAN,
            AnalyticsValuesSourceType.HISTOGRAM);
    }

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
            MappedFieldType fieldType = new HistogramFieldMapper.HistogramFieldType("field", Collections.emptyMap());
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                PercentileRanks ranks = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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

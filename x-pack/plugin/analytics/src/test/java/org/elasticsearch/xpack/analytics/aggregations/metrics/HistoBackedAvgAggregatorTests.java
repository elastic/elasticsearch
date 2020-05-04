/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.TDigest;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.aggregations.support.AnalyticsValuesSourceType;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;

public class HistoBackedAvgAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(getDocValue("wrong_field", new double[] {3, 1.2, 10})));
            iw.addDocument(singleton(getDocValue("wrong_field", new double[] {5.3, 6, 20})));
        }, avg -> {
            assertEquals(Double.NaN, avg.getValue(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testSimpleHistogram() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {3, 1.2, 10})));
            iw.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {5.3, 6, 6, 20})));
            iw.addDocument(singleton(getDocValue(FIELD_NAME, new double[] {-10, 0.01, 1, 90})));
        }, avg -> {
            assertEquals(12.0463d, avg.getValue(), 0.01d);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(new TermQuery(new Term("match", "yes")), iw -> {
            iw.addDocument(Arrays.asList(
                new StringField("match", "yes", Field.Store.NO),
                getDocValue(FIELD_NAME, new double[] {3, 1.2, 10}))
            );
            iw.addDocument(Arrays.asList(
                new StringField("match", "yes", Field.Store.NO),
                getDocValue(FIELD_NAME, new double[] {5.3, 6, 20}))
            );
            iw.addDocument(Arrays.asList(
                new StringField("match", "no", Field.Store.NO),
                getDocValue(FIELD_NAME, new double[] {3, 1.2, 10}))
            );
            iw.addDocument(Arrays.asList(
                new StringField("match", "no", Field.Store.NO),
                getDocValue(FIELD_NAME, new double[] {3, 1.2, 10}))
            );
            iw.addDocument(Arrays.asList(
                new StringField("match", "yes", Field.Store.NO),
                getDocValue(FIELD_NAME, new double[] {-10, 0.01, 1, 90}))
            );
        }, avg -> {
            assertEquals(12.651d, avg.getValue(), 0.01d);
            assertTrue(AggregationInspectionHelper.hasValue(avg));
        });
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> indexer,
                          Consumer<InternalAvg> verify) throws IOException {
        testCase(avg("_name").field(FIELD_NAME), query, indexer, verify, defaultFieldType(FIELD_NAME));
    }

    private BinaryDocValuesField getDocValue(String fieldName, double[] values) throws IOException {
        TDigest histogram = new TDigestState(100.0); //default
        for (double value : values) {
            histogram.add(value);
        }
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        histogram.compress();
        Collection<Centroid> centroids = histogram.centroids();
        Iterator<Centroid> iterator = centroids.iterator();
        while ( iterator.hasNext()) {
            Centroid centroid = iterator.next();
            streamOutput.writeVInt(centroid.count());
            streamOutput.writeDouble(centroid.mean());
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // Note: this is the same list as Core, plus Analytics
        return List.of(
            CoreValuesSourceType.NUMERIC,
            AnalyticsValuesSourceType.HISTOGRAM
        );
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new AvgAggregationBuilder("_name").field(fieldName);
    }

    private MappedFieldType defaultFieldType(String fieldName) {
        MappedFieldType fieldType = new HistogramFieldMapper.Builder("field").fieldType();
        fieldType.setName("field");
        return fieldType;
    }
}

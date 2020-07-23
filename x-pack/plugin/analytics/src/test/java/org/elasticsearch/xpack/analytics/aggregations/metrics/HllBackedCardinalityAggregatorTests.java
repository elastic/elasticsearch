/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HllFieldMapper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class HllBackedCardinalityAggregatorTests extends AggregatorTestCase {

    private static int[] RUNLENS = new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CardinalityAggregationBuilder("cardinality")
            .field(fieldName).precisionThreshold(HyperLogLog.thresholdFromPrecision(4));
    }

    private BinaryDocValuesField getDocValue(String fieldName,int[] runLens) throws IOException {
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        streamOutput.writeByte((byte) 0);
        for (int i = 0; i < runLens.length; i++) {
            streamOutput.writeByte((byte) runLens[i]);
        }
        return new BinaryDocValuesField(fieldName, streamOutput.bytes().toBytesRef());
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(getDocValue("wrong_field",RUNLENS)));
        }, hll -> {
            assertFalse(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("field"), iw -> {
            iw.addDocument(singleton(getDocValue("field", RUNLENS)));
        }, hll -> {
            assertEquals(86, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesMultiBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("field"), iw -> {
            iw.addDocument(singleton(getDocValue("field", RUNLENS)));
            iw.addDocument(singleton(getDocValue("field", RUNLENS)));
            iw.addDocument(singleton(getDocValue("field", RUNLENS)));
            iw.addDocument(singleton(getDocValue("field", RUNLENS)));
        }, hll -> {
            assertEquals(86, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testIncompatiblePrecision() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexWriter.addDocument(singleton(getDocValue("field", RUNLENS)));
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                CardinalityAggregationBuilder builder =
                    new CardinalityAggregationBuilder("test").field("field").precisionThreshold(192);

                MappedFieldType fieldType = new HllFieldMapper.HllFieldType("field", true, 10, false, Collections.emptyMap());
                IllegalArgumentException ex =
                    expectThrows(IllegalArgumentException.class, () -> createAggregator(builder, indexSearcher, fieldType));
                assertThat(ex.getMessage(),
                    Matchers.is("Cardinality aggregation precision [11] is not compatible with field precision [10]." +
                        " Precision threshold must be lower or equal than [191]"));
                CardinalityAggregationBuilder builder2 =
                    new CardinalityAggregationBuilder("test").field("field").precisionThreshold(191);
                assertThat(createAggregator(builder2, indexSearcher, fieldType), Matchers.notNullValue());
            }
        }
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalCardinality> verify) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                buildIndex.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                CardinalityAggregationBuilder builder =
                        new CardinalityAggregationBuilder("test").field("field")
                            .precisionThreshold(HyperLogLog.thresholdFromPrecision(4));

                MappedFieldType fieldType = new HllFieldMapper.HllFieldType("field", true, 4, false, Collections.emptyMap());
                Aggregator aggregator = createAggregator(builder, indexSearcher, fieldType);
                aggregator.preCollection();
                indexSearcher.search(query, aggregator);
                aggregator.postCollection();
                verify.accept((InternalCardinality) aggregator.buildTopLevel());
            }
        }
    }
}

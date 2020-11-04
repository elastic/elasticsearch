/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.aggregations.metrics;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.analytics.mapper.HyperLogLogPlusPlusDocValuesBuilder;
import org.elasticsearch.xpack.analytics.mapper.HyperLogLogPlusPlusFieldMapper;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class HyperLogLogPlusPlusBackedCardinalityAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";
    private static final byte[] RUNLENS = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    private static final int[] HASHES = new int[] {1, 2};
    // murmur3 are transformed to LC in the mapper so no need to test it here

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CardinalityAggregationBuilder("cardinality")
            .field(fieldName).precisionThreshold(AbstractHyperLogLog.thresholdFromPrecision(4));
    }

    private BinaryDocValuesField getHllDocValue(String fieldName, byte[] runLens) throws IOException {
        ByteBuffersDataOutput streamOutput = new ByteBuffersDataOutput();
        ByteArrayList list = new ByteArrayList();
        list.add(runLens);
        HyperLogLogPlusPlusDocValuesBuilder.writeHLL(list, streamOutput);
        return new BinaryDocValuesField(fieldName, new BytesRef(streamOutput.toArrayCopy()));
    }

    private BinaryDocValuesField getLcDocValue(String fieldName, int[] hashes) throws IOException {
        ByteBuffersDataOutput streamOutput = new ByteBuffersDataOutput();
        IntArrayList list = new IntArrayList();
        list.add(hashes);
        HyperLogLogPlusPlusDocValuesBuilder.writeLC(list, streamOutput);
        return new BinaryDocValuesField(fieldName, new BytesRef(streamOutput.toArrayCopy()));
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(getHllDocValue("wrong_field" ,RUNLENS)));
            iw.addDocument(singleton(getLcDocValue("wrong_field" ,HASHES)));
        }, hll -> {
            assertFalse(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesHllBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
        }, hll -> {
            assertEquals(86, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesLcBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(singleton(getLcDocValue(FIELD_NAME, HASHES)));
        }, hll -> {
            assertEquals(2, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesBothBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
            iw.addDocument(singleton(getLcDocValue(FIELD_NAME, HASHES)));
        }, hll -> {
            assertEquals(172, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testSomeMatchesMultiBinaryDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
            iw.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
        }, hll -> {
            assertEquals(86, hll.getValue());
            assertTrue(AggregationInspectionHelper.hasValue(hll));
        });
    }

    public void testIncompatiblePrecision() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexWriter.addDocument(singleton(getHllDocValue(FIELD_NAME, RUNLENS)));
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                CardinalityAggregationBuilder builder =
                    new CardinalityAggregationBuilder("test").field(FIELD_NAME).precisionThreshold(192);

                MappedFieldType fieldType =
                    new HyperLogLogPlusPlusFieldMapper.HyperLogLogPlusPlusFieldType(FIELD_NAME, Collections.emptyMap(), 10);
                IllegalArgumentException ex =
                    expectThrows(IllegalArgumentException.class, () -> createAggregator(builder, indexSearcher, fieldType));
                assertThat(ex.getMessage(),
                    Matchers.is("Cardinality aggregation precision [11] is not compatible with field precision [10]." +
                        " Precision threshold must be lower or equal than [191]"));
                CardinalityAggregationBuilder builder2 =
                    new CardinalityAggregationBuilder("test").field(FIELD_NAME).precisionThreshold(191);
                assertThat(createAggregator(builder2, indexSearcher, fieldType), Matchers.notNullValue());
            }
        }
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalCardinality> verify) throws IOException {
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder("test").field(FIELD_NAME)
                .precisionThreshold(AbstractHyperLogLog.thresholdFromPrecision(4));
        testCase(builder, query, buildIndex, verify, defaultFieldType());
    }

    private MappedFieldType defaultFieldType() {
        return new HyperLogLogPlusPlusFieldMapper.HyperLogLogPlusPlusFieldType(FIELD_NAME, Collections.emptyMap(), 4);
    }
}

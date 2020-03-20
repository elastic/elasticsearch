package org.elasticsearch.xpack.aggregatemetric.aggregations.metrics;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.aggregatemetric.aggregations.support.AggregateMetricsValuesSourceType;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class AggregateMetricBackedSumAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "aggregate_metric_field";

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(Arrays.asList(
                new NumericDocValuesField(FIELD_NAME + "._sum", Double.doubleToLongBits(10)),
                new NumericDocValuesField(FIELD_NAME + "._value_count", Double.doubleToLongBits(2))
            ));

            iw.addDocument(Arrays.asList(
                new NumericDocValuesField(FIELD_NAME + "._sum", Double.doubleToLongBits(50)),
                new NumericDocValuesField(FIELD_NAME + "._value_count", Double.doubleToLongBits(5))
            ));
        }, sum -> {
            assertEquals(60, sum.getValue(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(sum));
        });
    }

    private AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType createDefaultFieldType() {
        AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType fieldType =
            new AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType();
        fieldType.setName(FIELD_NAME);

        for (AggregateDoubleMetricFieldMapper.Metric m : List.of(
            AggregateDoubleMetricFieldMapper.Metric.value_count, AggregateDoubleMetricFieldMapper.Metric.sum)) {

            NumberFieldMapper.NumberFieldType subfield = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.DOUBLE);
            fieldType.addMetricField(m, subfield);
        }

        fieldType.setDefaultMetric(AggregateDoubleMetricFieldMapper.Metric.sum);
        return fieldType;
    }

    private void testCase(Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalSum> verify) throws IOException {
        AggregateDoubleMetricFieldMapper.AggregateDoubleMetricFieldType fieldType = createDefaultFieldType();
        fieldType.setName(FIELD_NAME);
        AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, FIELD_NAME);
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        T aggregationBuilder, Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify, MappedFieldType fieldType) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                verify.accept(agg);
            }
        }
    }

    @BeforeClass()
    public static void registerBuilder() {
        AggregateMetricsAggregatorsRegistrar.registerSumAggregator(valuesSourceRegistry);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new SumAggregationBuilder("sum_agg").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(AggregateMetricsValuesSourceType.AGGREGATE_METRIC);
    }

}

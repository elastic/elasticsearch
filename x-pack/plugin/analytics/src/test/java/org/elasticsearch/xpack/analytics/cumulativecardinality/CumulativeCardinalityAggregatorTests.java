/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.StubAggregatorFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class CumulativeCardinalityAggregatorTests extends AggregatorTestCase {

    private static final String HISTO_FIELD = "histo";
    private static final String VALUE_FIELD = "value_field";

    private static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45", //1
        "2017-01-01T03:43:34", //1
        "2017-01-03T04:11:00", //3
        "2017-01-03T05:11:31", //1
        "2017-01-05T08:24:05", //5
        "2017-01-05T13:09:32", //1
        "2017-01-07T13:47:43", //7
        "2017-01-08T16:14:34", //1
        "2017-01-09T17:09:50", //9
        "2017-01-09T22:55:46");//10

    private static final List<Integer> datasetValues = Arrays.asList(1,1,3,1,5,1,7,1,9,10);
    private static final List<Double> cumulativeCardinality = Arrays.asList(1.0,1.0,2.0,2.0,3.0,3.0,4.0,4.0,6.0);

    public void testSimple() throws IOException {

        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new CardinalityAggregationBuilder("the_cardinality", ValueType.NUMERIC).field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_cardinality"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(9, ((Histogram)histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram)histogram).getBuckets();
            int counter = 0;
            for (Histogram.Bucket bucket : buckets) {
                assertThat(((InternalSimpleLongValue) (bucket.getAggregations().get("cumulative_card"))).value(),
                    equalTo(cumulativeCardinality.get(counter)));
                counter += 1;
            }
        });
    }

    public void testAllNull() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new CardinalityAggregationBuilder("the_cardinality", ValueType.NUMERIC).field("foo"));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_cardinality"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(9, ((Histogram)histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram)histogram).getBuckets();
            for (Histogram.Bucket bucket : buckets) {
                assertThat(((InternalSimpleLongValue) (bucket.getAggregations().get("cumulative_card"))).value(), equalTo(0.0));
            }
        });
    }

    public void testParentValidations() throws IOException {
        ValuesSourceConfig<ValuesSource> valuesSource = new ValuesSourceConfig<>(CoreValuesSourceType.NUMERIC);

        // Histogram
        Set<PipelineAggregationBuilder> aggBuilders = new HashSet<>();
        aggBuilders.add(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "sum"));
        AggregatorFactory parent = new HistogramAggregatorFactory("name", valuesSource, 0.0d, 0.0d,
            mock(InternalOrder.class), false, 0L, 0.0d, 1.0d, mock(QueryShardContext.class), null,
            new AggregatorFactories.Builder(), Collections.emptyMap());
        CumulativeCardinalityPipelineAggregationBuilder builder
            = new CumulativeCardinalityPipelineAggregationBuilder("name", "valid");
        builder.validate(parent, Collections.emptySet(), aggBuilders);

        // Date Histogram
        aggBuilders.clear();
        aggBuilders.add(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "sum"));
        parent = new DateHistogramAggregatorFactory("name", valuesSource,
            mock(InternalOrder.class), false, 0L, mock(Rounding.class), mock(Rounding.class),
            mock(ExtendedBounds.class), mock(QueryShardContext.class), mock(AggregatorFactory.class),
            new AggregatorFactories.Builder(), Collections.emptyMap());
        builder = new CumulativeCardinalityPipelineAggregationBuilder("name", "valid");
        builder.validate(parent, Collections.emptySet(), aggBuilders);

        // Auto Date Histogram
        ValuesSourceConfig<ValuesSource.Numeric> numericVS = new ValuesSourceConfig<>(CoreValuesSourceType.NUMERIC);
        aggBuilders.clear();
        aggBuilders.add(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "sum"));
        AutoDateHistogramAggregationBuilder.RoundingInfo[] roundings = new AutoDateHistogramAggregationBuilder.RoundingInfo[1];
        parent = new AutoDateHistogramAggregatorFactory("name", numericVS,
            1, roundings,
            mock(QueryShardContext.class), null, new AggregatorFactories.Builder(), Collections.emptyMap());
        builder = new CumulativeCardinalityPipelineAggregationBuilder("name", "valid");
        builder.validate(parent, Collections.emptySet(), aggBuilders);

        // Mocked "test" agg, should fail validation
        aggBuilders.clear();
        aggBuilders.add(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "sum"));
        StubAggregatorFactory parentFactory = StubAggregatorFactory.createInstance();

        CumulativeCardinalityPipelineAggregationBuilder failBuilder
            = new CumulativeCardinalityPipelineAggregationBuilder("name", "invalid_agg>metric");
        IllegalStateException ex = expectThrows(IllegalStateException.class,
            () -> failBuilder.validate(parentFactory, Collections.emptySet(), aggBuilders));
        assertEquals("cumulative_cardinality aggregation [name] must have a histogram, date_histogram or auto_date_histogram as parent",
            ex.getMessage());
    }

    public void testNonCardinalityAgg() {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new SumAggregationBuilder("the_sum").field("foo"));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_sum"));

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Test should not have executed")));
        assertThat(e.getMessage(), equalTo("buckets_path must reference a cardinality aggregation, " +
            "got: [InternalSum] at aggregation [the_sum]"));
    }

    private void executeTestCase(Query query, AggregationBuilder aggBuilder, Consumer<InternalAggregation> verify) throws IOException {
        executeTestCase(query, aggBuilder, verify, indexWriter -> {
            Document document = new Document();
            int counter = 0;
            for (String date : datasetTimes) {
                if (frequently()) {
                    indexWriter.commit();
                }

                long instant = asLong(date);
                document.add(new SortedNumericDocValuesField(HISTO_FIELD, instant));
                document.add(new NumericDocValuesField(VALUE_FIELD, datasetValues.get(counter)));
                indexWriter.addDocument(document);
                document.clear();
                counter += 1;
            }
        });
    }

    private void executeTestCase(Query query, AggregationBuilder aggBuilder, Consumer<InternalAggregation> verify,
                                 CheckedConsumer<RandomIndexWriter, IOException> setup) throws IOException {


        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                setup.accept(indexWriter);
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
                DateFieldMapper.DateFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(HISTO_FIELD);

                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                valueFieldType.setHasDocValues(true);
                valueFieldType.setName("value_field");

                InternalAggregation histogram;
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, fieldType, valueFieldType);
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class CumulativeCardinalityAggregatorTests extends AggregatorTestCase {

    private static final String HISTO_FIELD = "histo";
    private static final String VALUE_FIELD = "value_field";

    private static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45", // 1
        "2017-01-01T03:43:34", // 1
        "2017-01-03T04:11:00", // 3
        "2017-01-03T05:11:31", // 1
        "2017-01-05T08:24:05", // 5
        "2017-01-05T13:09:32", // 1
        "2017-01-07T13:47:43", // 7
        "2017-01-08T16:14:34", // 1
        "2017-01-09T17:09:50", // 9
        "2017-01-09T22:55:46"
    );// 10

    private static final List<Integer> datasetValues = Arrays.asList(1, 1, 3, 1, 5, 1, 7, 1, 9, 10);
    private static final List<Double> cumulativeCardinality = Arrays.asList(1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0, 6.0);

    public void testSimple() throws IOException {

        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new CardinalityAggregationBuilder("the_cardinality").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_cardinality"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(9, ((Histogram) histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
            int counter = 0;
            for (Histogram.Bucket bucket : buckets) {
                assertThat(
                    ((InternalSimpleLongValue) (bucket.getAggregations().get("cumulative_card"))).value(),
                    equalTo(cumulativeCardinality.get(counter))
                );
                counter += 1;
            }
        });
    }

    public void testAllNull() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new CardinalityAggregationBuilder("the_cardinality").field("foo"));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_cardinality"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(9, ((Histogram) histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
            for (Histogram.Bucket bucket : buckets) {
                assertThat(((InternalSimpleLongValue) (bucket.getAggregations().get("cumulative_card"))).value(), equalTo(0.0));
            }
        });
    }

    public void testNonCardinalityAgg() {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new SumAggregationBuilder("the_sum").field("foo"));
        aggBuilder.subAggregation(new CumulativeCardinalityPipelineAggregationBuilder("cumulative_card", "the_sum"));

        AggregationExecutionException e = expectThrows(
            AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Test should not have executed"))
        );
        assertThat(
            e.getMessage(),
            equalTo("buckets_path must reference a cardinality aggregation, " + "got: [Sum] at aggregation [the_sum]")
        );
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

    private void executeTestCase(
        Query query,
        AggregationBuilder aggBuilder,
        Consumer<InternalAggregation> verify,
        CheckedConsumer<RandomIndexWriter, IOException> setup
    ) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                setup.accept(indexWriter);
            }

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(HISTO_FIELD);
                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value_field", NumberFieldMapper.NumberType.LONG);

                InternalAggregation histogram;
                histogram = searchAndReduce(indexSearcher, new AggTestConfig(aggBuilder, fieldType, valueFieldType).withQuery(query));
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AnalyticsPlugin());
    }
}

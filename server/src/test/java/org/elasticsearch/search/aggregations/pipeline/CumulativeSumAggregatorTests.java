/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class CumulativeSumAggregatorTests extends AggregatorTestCase {

    private static final String HISTO_FIELD = "histo";
    private static final String VALUE_FIELD = "value_field";

    private static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45",
        "2017-01-02T03:43:34",
        "2017-01-03T04:11:00",
        "2017-01-04T05:11:31",
        "2017-01-05T08:24:05",
        "2017-01-06T13:09:32",
        "2017-01-07T13:47:43",
        "2017-01-08T16:14:34",
        "2017-01-09T17:09:50",
        "2017-01-10T22:55:46"
    );

    private static final List<Integer> datasetValues = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    public void testSimple() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_avg"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(10, ((Histogram) histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
            double sum = 0.0;
            for (Histogram.Bucket bucket : buckets) {
                sum += ((InternalAvg) (bucket.getAggregations().get("the_avg"))).value();
                assertThat(((InternalSimpleValue) (bucket.getAggregations().get("cusum"))).value(), equalTo(sum));
                assertTrue(AggregationInspectionHelper.hasValue(((InternalAvg) (bucket.getAggregations().get("the_avg")))));
            }
        });
    }

    /**
     * First value from a derivative is null, so this makes sure the cusum can handle that
     */
    public void testDerivative() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("the_deriv", "the_avg"));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_deriv"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(10, ((Histogram) histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
            double sum = 0.0;
            for (int i = 0; i < buckets.size(); i++) {
                if (i == 0) {
                    assertThat(((InternalSimpleValue) (buckets.get(i).getAggregations().get("cusum"))).value(), equalTo(0.0));
                    assertTrue(
                        AggregationInspectionHelper.hasValue(((InternalSimpleValue) (buckets.get(i).getAggregations().get("cusum"))))
                    );
                } else {
                    sum += 1.0;
                    assertThat(((InternalSimpleValue) (buckets.get(i).getAggregations().get("cusum"))).value(), equalTo(sum));
                    assertTrue(
                        AggregationInspectionHelper.hasValue(((InternalSimpleValue) (buckets.get(i).getAggregations().get("cusum"))))
                    );
                }
            }
        });
    }

    public void testCount() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.fixedInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "_count"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertEquals(10, ((Histogram) histogram).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
            double sum = 1.0;
            for (Histogram.Bucket bucket : buckets) {
                assertThat(((InternalSimpleValue) (bucket.getAggregations().get("cusum"))).value(), equalTo(sum));
                assertTrue(AggregationInspectionHelper.hasValue(((InternalSimpleValue) (bucket.getAggregations().get("cusum")))));
                sum += 1.0;
            }
        });
    }

    public void testDocCount() throws IOException {
        Query query = new MatchAllDocsQuery();

        int numDocs = randomIntBetween(6, 20);
        int interval = randomIntBetween(2, 5);

        int minRandomValue = 0;
        int maxRandomValue = 20;

        int numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        long[] valueCounts = new long[numValueBuckets];

        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo").field(VALUE_FIELD)
            .interval(interval)
            .extendedBounds(minRandomValue, maxRandomValue);
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "_count"));

        executeTestCase(query, aggBuilder, histogram -> {
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();

            assertThat(buckets.size(), equalTo(numValueBuckets));

            double sum = 0;
            for (int i = 0; i < numValueBuckets; ++i) {
                Histogram.Bucket bucket = buckets.get(i);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
                assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
                sum += bucket.getDocCount();
                InternalSimpleValue cumulativeSumValue = bucket.getAggregations().get("cusum");
                assertThat(cumulativeSumValue, notNullValue());
                assertThat(cumulativeSumValue.getName(), equalTo("cusum"));
                assertThat(cumulativeSumValue.value(), equalTo(sum));
            }
        }, indexWriter -> {
            Document document = new Document();

            for (int i = 0; i < numDocs; i++) {
                int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
                document.add(new NumericDocValuesField(VALUE_FIELD, fieldValue));
                final int bucket = (fieldValue / interval);
                valueCounts[bucket]++;

                indexWriter.addDocument(document);
                document.clear();
            }
        });
    }

    public void testMetric() throws IOException {
        Query query = new MatchAllDocsQuery();

        int numDocs = randomIntBetween(6, 20);
        int interval = randomIntBetween(2, 5);

        int minRandomValue = 0;
        int maxRandomValue = 20;

        int numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        long[] valueCounts = new long[numValueBuckets];

        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo").field(VALUE_FIELD)
            .interval(interval)
            .extendedBounds(minRandomValue, maxRandomValue);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "sum"));

        executeTestCase(query, aggBuilder, histogram -> {
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();

            assertThat(buckets.size(), equalTo(numValueBuckets));

            double bucketSum = 0;
            for (int i = 0; i < numValueBuckets; ++i) {
                Histogram.Bucket bucket = buckets.get(i);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                bucketSum += sum.value();

                InternalSimpleValue sumBucketValue = bucket.getAggregations().get("cusum");
                assertThat(sumBucketValue, notNullValue());
                assertThat(sumBucketValue.getName(), equalTo("cusum"));
                assertThat(sumBucketValue.value(), equalTo(bucketSum));
            }
        }, indexWriter -> {
            Document document = new Document();

            for (int i = 0; i < numDocs; i++) {
                int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
                document.add(new NumericDocValuesField(VALUE_FIELD, fieldValue));
                final int bucket = (fieldValue / interval);
                valueCounts[bucket]++;

                indexWriter.addDocument(document);
                document.clear();
            }
        });
    }

    public void testNoBuckets() throws IOException {
        int numDocs = randomIntBetween(6, 20);
        int interval = randomIntBetween(2, 5);

        int minRandomValue = 0;
        int maxRandomValue = 20;

        int numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        long[] valueCounts = new long[numValueBuckets];

        Query query = new MatchNoDocsQuery();

        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo").field(VALUE_FIELD).interval(interval);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "sum"));

        executeTestCase(query, aggBuilder, histogram -> {
            List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();

            assertThat(buckets.size(), equalTo(0));

        }, indexWriter -> {
            Document document = new Document();

            for (int i = 0; i < numDocs; i++) {
                int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
                document.add(new NumericDocValuesField(VALUE_FIELD, fieldValue));
                final int bucket = (fieldValue / interval);
                valueCounts[bucket]++;

                indexWriter.addDocument(document);
                document.clear();
            }
        });
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

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(HISTO_FIELD);
                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value_field", NumberFieldMapper.NumberType.LONG);

                InternalAggregation histogram;
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, new MappedFieldType[] { fieldType, valueFieldType });
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}

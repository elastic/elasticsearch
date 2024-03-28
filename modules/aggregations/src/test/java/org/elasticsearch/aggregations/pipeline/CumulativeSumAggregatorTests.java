/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
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
import org.elasticsearch.search.aggregations.pipeline.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearhc.search.aggregations.support.ValueType;





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

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new AggregationsPlugin());
    }

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

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(HISTO_FIELD);
                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType("value_field", NumberFieldMapper.NumberType.LONG);

                InternalAggregation histogram;
                histogram = searchAndReduce(indexReader, new AggTestConfig(aggBuilder, fieldType, valueFieldType).withQuery(query));
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }

    public void testTermsHelper(ValueType vType, String bPath, Consumer<InternalAggregation> verify, CheckedConsumer<RandomIndexWriter, IOException> setup, String terms) throws IOException {
        TermsAggregationBuilder termsAggResult = createTermsAggregation(vType, bPath, terms);

        Query matchQuery = new MatchAllDocsQuery();

        executeTestCase(matchQuery, termsAggResult, verify, setup);
    }

    public TermsAggregationBuilder createTermsAggregation(ValueType vType, String bPath, String terms) {
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("terms");
        termsAgg.field(terms);
        termsAgg.userValueTypeHint(vType);

        SumAggregationBuilder sumAgg = new SumAggregationBuilder("sum");
        sumAgg.field("value_field");
        termsAgg.subAggregation(sumAgg);

        CumulativeSumPipelineAggregationBuilder cusumAgg = new CumulativeSumPipelineAggregationBuilder("cusum", bPath);
        termsAgg.subAggregation(cusumAgg);

        return termsAgg;
    }

    public Consumer<InternalAggregation> createConsumer(List datasetTems) {
        return terms -> {
            Document currDoc = new Document();
            for (int i : datasetTerms) {
                int counterValue = datasetTerms.get(i);
                SortedNumericDocValuesField sorted = new SortedNumericDocValuesField("value_field", new BytesRef((String) sorted));
                currDoc.add(sorted.apply(counterValue));
                currDoc.add(new NumericDocValuesField("value_field", datasetTerms.get(i).intValue()));

                terms.addDocument(currDoc);
                currDoc.clear();
            }
        };
    }

    public CheckedConsumer<RandomIndexWriter, IOException> createSetup(List expected) {
        return checkedTerms -> {
            List <? extends Terms.Bucket> buckets = ((InternalTerms) checkedTerms).getBuckets();
            assertThat(buckets.size(), equalTo(expected.size()));
            for (int i = 0; i < buckets.size(); i++) {
                InternalSimpleValue value = (InternalSimpleValue) buckets.get(i).getAggregations().get("cusum");
                assertThat(value.value(), equalTo(expected.get(i)));
            }
        };
    }

    public void testTermsAggSimple() {
        List<String> simpleDataset = Array.asList("10", "20", "30", "40", "50");

        List<Double> expectedCt = Array.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        List<Double> expectedSum = Array.asList(10.0, 30.0, 60.0, 100.0, 150.0);

        verify = createConsumer(simpleDataset);

        setup = createSetup(expectedCt);

        testTermsHelper(ValueType.STRING, "_count", verify, setup, "keyword_field");

        verify = createConsumer(simpleDataset);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.STRING, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggCheckCountVal() {
        List<String> datasetTerms = Array.asList("10", "10", "20", "20", "30", "30", "30", "40", "40", "50");

        List<Double> expectedCt = Array.asList(2.0, 4.0, 7.0, 9.0, 10.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedCt);

        testTermsHelper(ValueType.STRING, "_count", verify, setup, "keyword_field");
    }

    public void testTermsAggDouble() {
        List<Double> datasetTerms  = Array.asList(10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0, 20.0);

        List<Double> expectedSum = Array.asList(80.0, 120.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.DOUBLE, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggString() {
        List<String> datasetTerms = Array.asList("10", "20", "30", "40", "50", "60", "70", "80", "90", "100");

        List<Double> expectedSum = Array.asList(10.0, 30.0, 60.0, 100.0, 150.0, 210.0, 280.0, 360.0, 450.0, 550.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.STRING, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggLong() {
        List<Long> datasetTerms = Array.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);

        List<Double> expectedSum = Array.asList(1.0, 3.0, 6.0, 10.0, 15.0, 21.0, 28.0, 36.0, 45.0, 55.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.LONG, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggInteger() {
        List<Integer> datasetTerms = Array.asList(100, 200, 300);

        List<Double> expectedSum = Array.asList(100.0, 300.0, 600.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.INTEGER, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggShort() {
        List<Short> datasetTerms = Array.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

        List<Double> expectedSum = Array.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.SHORT, "sum", verify, setup, "keyword_field");
    }

    public void testTermsAggOneEntry() {
        List<Double> datasetTerms = Array.asList(13.0);

        List<Double> expectedSum = Array.asList(13.0);

        verify = createConsumer(datasetTerms);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.DOUBLE, "sum", verify, setup, "keyword_field");
    }

    public void testTermsNoValues() {
        List<String> datasetEmpty = Array.asList("");

        List<String> expectedSum = Array.asList("");

        List<Double> expectedCt = Array.asList(0.0);

        vertify = createConsumer(datasetEmpty);

        setup = createSetup(expectedSum);

        testTermsHelper(ValueType.STRING, "sum", verify, setup, "keyword_field");

        verify = createConsumer(datasetEmpty);

        setup = createSetup(expectedCt);

        testTermsHelper(ValueType.STRING, "_count", verify, setup, "keyword_field");
    }
}
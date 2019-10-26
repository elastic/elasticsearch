/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class DerivativeAggregatorTests extends AggregatorTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static int interval = 5;
    private static int numValueBuckets;
    private static int numFirstDerivValueBuckets;
    private static int numSecondDerivValueBuckets;
    private static long[] valueCounts;
    private static long[] firstDerivValueCounts;
    private static long[] secondDerivValueCounts;

    private static Long[] valueCounts_empty;
    private static long numDocsEmptyIdx;
    private static Double[] firstDerivValueCounts_empty;

    // expected bucket values for random setup with gaps
    private static int numBuckets_empty_rnd;
    private static Long[] valueCounts_empty_rnd;
    private static Double[] firstDerivValueCounts_empty_rnd;
    private static long numDocsEmptyIdx_rnd;


    private void setupValueCounts() {
        numDocsEmptyIdx = 0L;
        numDocsEmptyIdx_rnd = 0L;
        interval = 5;
        numValueBuckets = randomIntBetween(6, 80);

        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            valueCounts[i] = randomIntBetween(1, 20);
        }

        numFirstDerivValueBuckets = numValueBuckets - 1;
        firstDerivValueCounts = new long[numFirstDerivValueBuckets];
        Long lastValueCount = null;
        for (int i = 0; i < numValueBuckets; i++) {
            long thisValue = valueCounts[i];
            if (lastValueCount != null) {
                long diff = thisValue - lastValueCount;
                firstDerivValueCounts[i - 1] = diff;
            }
            lastValueCount = thisValue;
        }

        numSecondDerivValueBuckets = numFirstDerivValueBuckets - 1;
        secondDerivValueCounts = new long[numSecondDerivValueBuckets];
        Long lastFirstDerivativeValueCount = null;
        for (int i = 0; i < numFirstDerivValueBuckets; i++) {
            long thisFirstDerivativeValue = firstDerivValueCounts[i];
            if (lastFirstDerivativeValueCount != null) {
                long diff = thisFirstDerivativeValue - lastFirstDerivativeValueCount;
                secondDerivValueCounts[i - 1] = diff;
            }
            lastFirstDerivativeValueCount = thisFirstDerivativeValue;
        }

        // setup for index with empty buckets
        valueCounts_empty = new Long[] { 1L, 1L, 2L, 0L, 2L, 2L, 0L, 0L, 0L, 3L, 2L, 1L };
        firstDerivValueCounts_empty = new Double[] { null, 0d, 1d, -2d, 2d, 0d, -2d, 0d, 0d, 3d, -1d, -1d };

        // randomized setup for index with empty buckets
        numBuckets_empty_rnd = randomIntBetween(20, 100);
        valueCounts_empty_rnd = new Long[numBuckets_empty_rnd];
        firstDerivValueCounts_empty_rnd = new Double[numBuckets_empty_rnd];
        firstDerivValueCounts_empty_rnd[0] = null;
    }

    /**
     * test first and second derivative on the sing
     */
    public void testDocCountDerivative() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(interval);
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "_count"));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("2nd_deriv", "deriv"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram)histogram).getBuckets();
                assertThat(buckets.size(), equalTo(numValueBuckets));

                for (int i = 0; i < numValueBuckets; ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
                    SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
                    if (i > 0) {
                        assertThat(docCountDeriv, notNullValue());
                        assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i - 1]));
                    } else {
                        assertThat(docCountDeriv, nullValue());
                    }
                    SimpleValue docCount2ndDeriv = bucket.getAggregations().get("2nd_deriv");
                    if (i > 1) {
                        assertThat(docCount2ndDeriv, notNullValue());
                        assertThat(docCount2ndDeriv.value(), equalTo((double) secondDerivValueCounts[i - 2]));
                    } else {
                        assertThat(docCount2ndDeriv, nullValue());
                    }
                }
            });
    }

    /**
     * test first and second derivative on the sing
     */
    public void testSingleValuedField_normalised() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0);
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "_count").unit("1ms"));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("2nd_deriv", "deriv").unit("10ms"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram)histogram).getBuckets();
                assertThat(buckets.size(), equalTo(numValueBuckets));

                for (int i = 0; i < numValueBuckets; ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
                    Derivative docCountDeriv = bucket.getAggregations().get("deriv");
                    if (i > 0) {
                        assertThat(docCountDeriv, notNullValue());
                        assertThat(docCountDeriv.value(), closeTo((firstDerivValueCounts[i - 1]), 0.00001));
                        assertThat(docCountDeriv.normalizedValue(), closeTo((double) (firstDerivValueCounts[i - 1]) / 5, 0.00001));
                    } else {
                        assertThat(docCountDeriv, nullValue());
                    }
                    Derivative docCount2ndDeriv = bucket.getAggregations().get("2nd_deriv");
                    if (i > 1) {
                        assertThat(docCount2ndDeriv, notNullValue());
                        assertThat(docCount2ndDeriv.value(), closeTo((secondDerivValueCounts[i - 2]), 0.00001));
                        assertThat(docCount2ndDeriv.normalizedValue(), closeTo((double) (secondDerivValueCounts[i - 2]) * 2, 0.00001));
                    } else {
                        assertThat(docCount2ndDeriv, nullValue());
                    }
                }
            });
    }


    public void testSingleValueAggDerivative() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(interval);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "sum"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(numValueBuckets));

                Object[] propertiesKeys = (Object[]) ((InternalAggregation)histogram).getProperty("_key");
                Object[] propertiesDocCounts = (Object[]) ((InternalAggregation)histogram).getProperty("_count");
                Object[] propertiesSumCounts = (Object[]) ((InternalAggregation)histogram).getProperty("sum.value");

                Long expectedSumPreviousBucket = Long.MIN_VALUE; // start value, gets
                // overwritten
                for (int i = 0; i < numValueBuckets; ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    long expectedSum = valueCounts[i] * (i * interval);
                    assertThat(sum.getValue(), equalTo((double) expectedSum));
                    SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
                    if (i > 0) {
                        assertThat(sumDeriv, notNullValue());
                        long sumDerivValue = expectedSum - expectedSumPreviousBucket;
                        assertThat(sumDeriv.value(), equalTo((double) sumDerivValue));
                        assertThat(((InternalMultiBucketAggregation.InternalBucket)bucket).getProperty("histo",
                            AggregationPath.parse("deriv.value").getPathElementsAsStringList()),
                            equalTo((double) sumDerivValue));
                    } else {
                        assertThat(sumDeriv, nullValue());
                    }
                    expectedSumPreviousBucket = expectedSum;
                    assertThat(propertiesKeys[i], equalTo((double) i * interval));
                    assertThat((long) propertiesDocCounts[i], equalTo(valueCounts[i]));
                    assertThat((double) propertiesSumCounts[i], equalTo((double) expectedSum));
                }
            });
    }

    public void testMultiValueAggDerivative() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(interval);
        aggBuilder.subAggregation(new StatsAggregationBuilder("stats").field(SINGLE_VALUED_FIELD_NAME));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "stats.sum"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(numValueBuckets));

                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                Object[] propertiesKeys = (Object[]) ((InternalAggregation)histogram).getProperty("_key");
                Object[] propertiesDocCounts = (Object[]) ((InternalAggregation)histogram).getProperty("_count");
                Object[] propertiesSumCounts = (Object[]) ((InternalAggregation)histogram).getProperty("stats.sum");

                Long expectedSumPreviousBucket = Long.MIN_VALUE; // start value, gets
                // overwritten
                for (int i = 0; i < numValueBuckets; ++i) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
                    Stats stats = bucket.getAggregations().get("stats");
                    assertThat(stats, notNullValue());
                    long expectedSum = valueCounts[i] * (i * interval);
                    assertThat(stats.getSum(), equalTo((double) expectedSum));
                    SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
                    if (i > 0) {
                        assertThat(sumDeriv, notNullValue());
                        long sumDerivValue = expectedSum - expectedSumPreviousBucket;
                        assertThat(sumDeriv.value(), equalTo((double) sumDerivValue));
                        assertThat(((InternalMultiBucketAggregation.InternalBucket)bucket).getProperty("histo",
                            AggregationPath.parse("deriv.value").getPathElementsAsStringList()),
                            equalTo((double) sumDerivValue));
                    } else {
                        assertThat(sumDeriv, nullValue());
                    }
                    expectedSumPreviousBucket = expectedSum;
                    assertThat(propertiesKeys[i], equalTo((double) i * interval));
                    assertThat((long) propertiesDocCounts[i], equalTo(valueCounts[i]));
                    assertThat((double) propertiesSumCounts[i], equalTo((double) expectedSum));
                }
            });
    }

    public void testUnmapped() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(interval);
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "_count"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(0));
            },
            indexWriter -> {
                Document document = new Document();
                indexWriter.addDocument(document);
                indexWriter.commit();
            });
    }

    public void testDocCountDerivativeWithGaps() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(1);
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "_count"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(valueCounts_empty.length));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx));

                for (int i = 0; i < valueCounts_empty.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
                    SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
                    if (firstDerivValueCounts_empty[i] == null) {
                        assertThat(docCountDeriv, nullValue());
                    } else {
                        assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty[i]));
                    }
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    for (int docs = 0; docs < valueCounts_empty[i]; docs++) {
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx++;
                    }
                }
                indexWriter.commit();
            });

    }

    public void testDocCountDerivativeWithGaps_random() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME)
            .interval(1)
            .extendedBounds(0L, numBuckets_empty_rnd - 1);
        aggBuilder.subAggregation(
            new DerivativePipelineAggregationBuilder("deriv", "_count")
                .gapPolicy(randomFrom(BucketHelpers.GapPolicy.values()))
        );

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(numBuckets_empty_rnd));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx_rnd));

                for (int i = 0; i < valueCounts_empty_rnd.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty_rnd[i]);
                    SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
                    if (firstDerivValueCounts_empty_rnd[i] == null) {
                        assertThat(docCountDeriv, nullValue());
                    } else {
                        assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty_rnd[i]));
                    }
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < numBuckets_empty_rnd; i++) {
                    valueCounts_empty_rnd[i] = (long) randomIntBetween(1, 10);
                    // make approximately half of the buckets empty
                    if (randomBoolean())
                        valueCounts_empty_rnd[i] = 0L;
                    for (int docs = 0; docs < valueCounts_empty_rnd[i]; docs++) {

                        if (frequently()) {
                            indexWriter.commit();
                        }
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx_rnd++;
                    }
                    if (i > 0) {
                        firstDerivValueCounts_empty_rnd[i] = (double) valueCounts_empty_rnd[i] - valueCounts_empty_rnd[i - 1];
                    }
                    indexWriter.commit();
                }
            });
    }

    public void testDocCountDerivativeWithGaps_insertZeros() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME)
            .interval(1);
        aggBuilder.subAggregation(
            new DerivativePipelineAggregationBuilder("deriv", "_count")
                .gapPolicy(BucketHelpers.GapPolicy.INSERT_ZEROS)
        );

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(valueCounts_empty.length));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx));

                for (int i = 0; i < valueCounts_empty.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i + ": ", bucket, i, valueCounts_empty[i]);
                    SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
                    if (firstDerivValueCounts_empty[i] == null) {
                        assertThat(docCountDeriv, nullValue());
                    } else {
                        assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty[i]));
                    }
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    for (int docs = 0; docs < valueCounts_empty[i]; docs++) {
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx++;
                    }
                }
                indexWriter.commit();
            });
    }

    public void testSingleValueAggDerivativeWithGaps() throws Exception {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(1);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME));
        aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv", "sum"));

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(valueCounts_empty.length));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx));

                double lastSumValue = Double.NaN;
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
                    Sum sum = bucket.getAggregations().get("sum");
                    double thisSumValue = sum.value();
                    if (bucket.getDocCount() == 0) {
                        thisSumValue = Double.NaN;
                    }
                    SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
                    if (i == 0) {
                        assertThat(sumDeriv, nullValue());
                    } else {
                        double expectedDerivative = thisSumValue - lastSumValue;
                        if (Double.isNaN(expectedDerivative)) {
                            assertThat(sumDeriv.value(), equalTo(expectedDerivative));
                        } else {
                            assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
                        }
                    }
                    lastSumValue = thisSumValue;
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    for (int docs = 0; docs < valueCounts_empty[i]; docs++) {
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx++;
                    }
                }
                indexWriter.commit();
            });

    }

    public void testSingleValueAggDerivativeWithGaps_insertZeros() throws IOException {
        setupValueCounts();
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(1);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME));
        aggBuilder.subAggregation(
            new DerivativePipelineAggregationBuilder("deriv", "sum")
                    .gapPolicy(GapPolicy.INSERT_ZEROS)
        );

        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(valueCounts_empty.length));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx));

                double lastSumValue = Double.NaN;
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
                    Sum sum = bucket.getAggregations().get("sum");
                    double thisSumValue = sum.value();
                    if (bucket.getDocCount() == 0) {
                        thisSumValue = 0;
                    }
                    SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
                    if (i == 0) {
                        assertThat(sumDeriv, nullValue());
                    } else {
                        double expectedDerivative = thisSumValue - lastSumValue;
                        assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
                    }
                    lastSumValue = thisSumValue;
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < valueCounts_empty.length; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    for (int docs = 0; docs < valueCounts_empty[i]; docs++) {
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx++;
                    }
                }
                indexWriter.commit();
            });
    }

    public void testSingleValueAggDerivativeWithGaps_random() throws IOException {
        setupValueCounts();
        BucketHelpers.GapPolicy gapPolicy = randomFrom(GapPolicy.values());
        Query query = new MatchAllDocsQuery();
        HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
            .field(SINGLE_VALUED_FIELD_NAME).interval(1)
            .extendedBounds(0L, (long) numBuckets_empty_rnd - 1);
        aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME));
        aggBuilder.subAggregation(
            new DerivativePipelineAggregationBuilder("deriv", "sum").gapPolicy(gapPolicy)
        );
        executeTestCase(query, aggBuilder,
            histogram -> {
                assertThat(histogram, notNullValue());
                assertThat(histogram.getName(), equalTo("histo"));
                List<? extends Histogram.Bucket> buckets = ((Histogram) histogram).getBuckets();
                assertThat(buckets.size(), equalTo(valueCounts_empty_rnd.length));
                assertThat(getTotalDocCountAcrossBuckets(buckets), equalTo(numDocsEmptyIdx_rnd));

                double lastSumValue = Double.NaN;
                for (int i = 0; i < valueCounts_empty_rnd.length; i++) {
                    Histogram.Bucket bucket = buckets.get(i);
                    checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty_rnd[i]);
                    Sum sum = bucket.getAggregations().get("sum");
                    double thisSumValue = sum.value();
                    if (bucket.getDocCount() == 0) {
                        thisSumValue = gapPolicy == GapPolicy.INSERT_ZEROS ? 0 : Double.NaN;
                    }
                    SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
                    if (i == 0) {
                        assertThat(sumDeriv, nullValue());
                    } else {
                        double expectedDerivative = thisSumValue - lastSumValue;
                        if (Double.isNaN(expectedDerivative)) {
                            assertThat(sumDeriv.value(), equalTo(expectedDerivative));
                        } else {
                            assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
                        }
                    }
                    lastSumValue = thisSumValue;
                }
            },
            indexWriter -> {
                Document document = new Document();
                for (int i = 0; i < numBuckets_empty_rnd; i++) {
                    valueCounts_empty_rnd[i] = (long) randomIntBetween(1, 10);
                    // make approximately half of the buckets empty
                    if (randomBoolean())
                        valueCounts_empty_rnd[i] = 0L;
                    for (int docs = 0; docs < valueCounts_empty_rnd[i]; docs++) {

                        if (frequently()) {
                            indexWriter.commit();
                        }
                        document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i));
                        indexWriter.addDocument(document);
                        document.clear();
                        numDocsEmptyIdx_rnd++;
                    }
                    if (i > 0) {
                        firstDerivValueCounts_empty_rnd[i] = (double) valueCounts_empty_rnd[i] - valueCounts_empty_rnd[i - 1];
                    }
                    indexWriter.commit();
                }
            });
    }

    public void testSingleValueAggDerivative_invalidPath() throws IOException {
        try {
            Query query = new MatchAllDocsQuery();
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
                .field(SINGLE_VALUED_FIELD_NAME).interval(1);
            aggBuilder.subAggregation(
                new FiltersAggregationBuilder("filters", QueryBuilders.termQuery("tag", "foo"))
                    .subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME))
            );
            aggBuilder.subAggregation(new SumAggregationBuilder("sum").field(SINGLE_VALUED_FIELD_NAME));
            aggBuilder.subAggregation(
                new DerivativePipelineAggregationBuilder("deriv", "filters>get>sum")
            );
            executeTestCase(query, aggBuilder, history -> {});
            fail("Expected an Exception but didn't get one");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if (!(rootCause instanceof IllegalArgumentException)) {
                    throw e;
                }
            } else if (!(cause instanceof IllegalArgumentException)) {
                throw e;
            }
        }
    }

    public void testDerivDerivNPE() throws IOException {
        try (Directory directory = newDirectory()) {
            Query query = new MatchAllDocsQuery();
            HistogramAggregationBuilder aggBuilder = new HistogramAggregationBuilder("histo")
                .field("tick").interval(1);
            aggBuilder.subAggregation(new AvgAggregationBuilder("avg").field("value"));
            aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv1", "avg"));
            aggBuilder.subAggregation(new DerivativePipelineAggregationBuilder("deriv2", "deriv1"));
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (int i = 0; i < 10; i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    if (i != 1 || i != 3) {
                        document.add(new NumericDocValuesField("value", i));
                    }
                    document.add(new NumericDocValuesField("tick", i));

                    indexWriter.addDocument(document);
                    document.clear();
                }
                indexWriter.commit();
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                searchAndReduce(indexSearcher, query, aggBuilder);
            }
        }
    }

    private Long getTotalDocCountAcrossBuckets(List<? extends Histogram.Bucket> buckets) {
        Long count = 0L;
        for (Histogram.Bucket bucket : buckets) {
            count += bucket.getDocCount();
        }
        return count;
    }

    private void checkBucketKeyAndDocCount(final String msg, final Histogram.Bucket bucket, final long expectedKey,
                                           final long expectedDocCount) {
        assertThat(msg, bucket, notNullValue());
        assertThat(msg + " key", ((Number) bucket.getKey()).longValue(), equalTo(expectedKey));
        assertThat(msg + " docCount", bucket.getDocCount(), equalTo(expectedDocCount));
    }

    private void executeTestCase(Query query, AggregationBuilder aggBuilder, Consumer<InternalAggregation> verify) throws IOException {
        setupValueCounts();
        executeTestCase(query, aggBuilder, verify, indexWriter -> {
            Document document = new Document();
            for (int i = 0; i < numValueBuckets; i++) {
                if (frequently()) {
                    indexWriter.commit();
                }
                for (int docs = 0; docs < valueCounts[i]; docs++) {
                    document.add(new NumericDocValuesField(SINGLE_VALUED_FIELD_NAME, i * interval));
                    indexWriter.addDocument(document);
                    document.clear();
                }
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
                fieldType.setName(SINGLE_VALUED_FIELD_NAME);

                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                valueFieldType.setHasDocValues(true);
                valueFieldType.setName("value_field");

                InternalAggregation histogram;
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, new MappedFieldType[]{fieldType, valueFieldType});

                verify.accept(histogram);
            }
        }
    }

}

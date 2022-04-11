/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.normalize;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class NormalizeAggregatorTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "date";
    private static final String TERM_FIELD = "term";
    private static final String VALUE_FIELD = "value_field";

    private static final List<String> datasetTimes = Arrays.asList(
        "2017-01-01T01:07:45", // 1
        "2017-01-01T03:43:34", // 1
        "2017-01-03T04:11:00", // 3
        "2017-01-03T05:11:31", // 3
        "2017-01-05T08:24:05", // 5
        "2017-01-05T13:09:32", // 5
        "2017-01-07T13:47:43", // 7
        "2017-01-08T16:14:34", // 8
        "2017-01-09T17:09:50", // 9
        "2017-01-09T22:55:46"
    );// 9
    private static final List<String> datasetTerms = Arrays.asList(
        "a", // 1
        "a", // 1
        "b", // 2
        "b", // 2
        "c", // 3
        "c", // 3
        "d", // 4
        "e", // 5
        "f", // 6
        "f"
    );// 6

    private static final List<Integer> datasetValues = Arrays.asList(1, 1, 42, 6, 5, 0, 2, 8, 30, 13);
    private static final List<Double> datePercentOfSum = Arrays.asList(0.2, 0.0, 0.2, 0.0, 0.2, 0.0, 0.1, 0.1, 0.2);
    private static final List<Double> termPercentOfSum = Arrays.asList(0.2, 0.2, 0.2, 0.2, 0.1, 0.1);
    private static final List<Double> rescaleOneHundred = Arrays.asList(
        0.0,
        Double.NaN,
        100.0,
        Double.NaN,
        6.521739130434782,
        Double.NaN,
        0.0,
        13.043478260869565,
        89.1304347826087
    );

    public void testPercentOfTotalDocCount() throws IOException {
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(DATE_FIELD);
        aggBuilder.subAggregation(new NormalizePipelineAggregationBuilder("normalized", null, "percent_of_sum", List.of("_count")));

        testCase(aggBuilder, (agg) -> {
            assertEquals(9, ((Histogram) agg).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) agg).getBuckets();
            for (int i = 0; i < buckets.size(); i++) {
                Histogram.Bucket bucket = buckets.get(i);
                assertThat(((InternalSimpleValue) (bucket.getAggregations().get("normalized"))).value(), equalTo(datePercentOfSum.get(i)));
            }
        });
    }

    public void testValueMean() throws IOException {
        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.calendarInterval(DateHistogramInterval.DAY).field(DATE_FIELD);
        aggBuilder.subAggregation(new StatsAggregationBuilder("stats").field(VALUE_FIELD));
        aggBuilder.subAggregation(new NormalizePipelineAggregationBuilder("normalized", null, "rescale_0_100", List.of("stats.sum")));

        testCase(aggBuilder, (agg) -> {
            assertEquals(9, ((Histogram) agg).getBuckets().size());
            List<? extends Histogram.Bucket> buckets = ((Histogram) agg).getBuckets();
            for (int i = 0; i < buckets.size(); i++) {
                Histogram.Bucket bucket = buckets.get(i);
                assertThat(((InternalSimpleValue) (bucket.getAggregations().get("normalized"))).value(), equalTo(rescaleOneHundred.get(i)));
            }
        });
    }

    public void testTermsAggParent() throws IOException {
        TermsAggregationBuilder aggBuilder = new TermsAggregationBuilder("terms").field(TERM_FIELD);
        aggBuilder.subAggregation(new NormalizePipelineAggregationBuilder("normalized", null, "percent_of_sum", List.of("_count")));

        testCase(aggBuilder, (agg) -> {
            assertEquals(6, ((Terms) agg).getBuckets().size());
            List<? extends Terms.Bucket> buckets = ((Terms) agg).getBuckets();
            for (int i = 0; i < buckets.size(); i++) {
                Terms.Bucket bucket = buckets.get(i);
                assertThat(((InternalSimpleValue) (bucket.getAggregations().get("normalized"))).value(), equalTo(termPercentOfSum.get(i)));
            }
        });

    }

    private void testCase(ValuesSourceAggregationBuilder<?> aggBuilder, Consumer<InternalAggregation> aggAssertion) throws IOException {
        Query query = new MatchAllDocsQuery();
        // index date data
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (int i = 0; i < datasetValues.size(); i++) {
                    if (frequently()) {
                        indexWriter.commit();
                    }
                    long instant = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(datasetTimes.get(i)))
                        .toInstant()
                        .toEpochMilli();
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new NumericDocValuesField(VALUE_FIELD, datasetValues.get(i)));
                    document.add(new SortedSetDocValuesField(TERM_FIELD, new BytesRef(datasetTerms.get(i))));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            // setup mapping
            DateFieldMapper.DateFieldType dateFieldType = new DateFieldMapper.DateFieldType(DATE_FIELD);
            MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD, NumberFieldMapper.NumberType.LONG);
            MappedFieldType termFieldType = new KeywordFieldMapper.KeywordFieldType(TERM_FIELD, false, true, Collections.emptyMap());

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                InternalAggregation internalAggregation = searchAndReduce(
                    indexSearcher,
                    query,
                    aggBuilder,
                    dateFieldType,
                    valueFieldType,
                    termFieldType
                );
                aggAssertion.accept(internalAggregation);
            }
        }
    }
}

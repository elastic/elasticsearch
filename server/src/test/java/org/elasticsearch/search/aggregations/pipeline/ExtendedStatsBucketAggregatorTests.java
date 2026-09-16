/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats.Bounds;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the {@code extended_stats_bucket} pipeline aggregation, migrated from
 * {@code ExtendedStatsBucketIT} so that the {@code sigma} behaviour is covered without
 * standing up a cluster (https://github.com/elastic/elasticsearch/issues/36015).
 * <p>
 * The aggregations under test are wrapped in a single-bucket {@code filters} placeholder because
 * {@link AggregatorTestCase} reduces to exactly one top level aggregation, so the histogram and the
 * sibling pipeline aggregation cannot be declared side by side at the top level as they are in a search request.
 */
public class ExtendedStatsBucketAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD = "value_field";

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/17701
     */
    public void testGappyIndexWithSigma() throws IOException {
        double sigma = randomDoubleBetween(1.0, 6.0, true);
        MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD, NumberFieldMapper.NumberType.INTEGER);

        FiltersAggregationBuilder placeholder = new FiltersAggregationBuilder("placeholder", new MatchAllQueryBuilder()).subAggregation(
            new HistogramAggregationBuilder("histo").field(VALUE_FIELD).interval(1L)
        ).subAggregation(new ExtendedStatsBucketPipelineAggregationBuilder("extended_stats_bucket", "histo>_count").sigma(sigma));

        testCase(placeholder, iw -> {
            for (int i = 0; i < 6; i++) {
                // creates 6 documents where the value of the field is 0, 1, 2, 3,
                // 3, 5
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField(VALUE_FIELD, i == 4 ? 3 : i));
                iw.addDocument(doc);
            }
        }, filters -> {
            Histogram histo = filters.getBuckets().get(0).getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();
            assertThat(buckets.size(), equalTo(6));

            for (int i = 0; i < 6; ++i) {
                long expectedDocCount;
                if (i == 3) {
                    expectedDocCount = 2;
                } else if (i == 4) {
                    expectedDocCount = 0;
                } else {
                    expectedDocCount = 1;
                }
                Bucket bucket = buckets.get(i);
                assertThat("i: " + i, bucket, notNullValue());
                assertThat("i: " + i, ((Number) bucket.getKey()).longValue(), equalTo((long) i));
                assertThat("i: " + i, bucket.getDocCount(), equalTo(expectedDocCount));
            }

            ExtendedStatsBucket extendedStatsBucketValue = filters.getBuckets().get(0).getAggregations().get("extended_stats_bucket");
            long count = 6L;
            double sum = 1.0 + 1.0 + 1.0 + 2.0 + 0.0 + 1.0;
            double sumOfSqrs = 1.0 + 1.0 + 1.0 + 4.0 + 0.0 + 1.0;
            double avg = sum / count;
            double var = (sumOfSqrs - ((sum * sum) / count)) / count;
            var = var < 0 ? 0 : var;
            double stdDev = Math.sqrt(var);
            assertThat(extendedStatsBucketValue, notNullValue());
            assertThat(extendedStatsBucketValue.getName(), equalTo("extended_stats_bucket"));
            assertThat(extendedStatsBucketValue.getMin(), equalTo(0.0));
            assertThat(extendedStatsBucketValue.getMax(), equalTo(2.0));
            assertThat(extendedStatsBucketValue.getCount(), equalTo(count));
            assertThat(extendedStatsBucketValue.getSum(), equalTo(sum));
            assertThat(extendedStatsBucketValue.getAvg(), equalTo(avg));
            assertThat(extendedStatsBucketValue.getSumOfSquares(), equalTo(sumOfSqrs));
            assertThat(extendedStatsBucketValue.getVariance(), equalTo(var));
            assertThat(extendedStatsBucketValue.getStdDeviation(), equalTo(stdDev));
            assertThat(extendedStatsBucketValue.getStdDeviationBound(Bounds.LOWER), equalTo(avg - (sigma * stdDev)));
            assertThat(extendedStatsBucketValue.getStdDeviationBound(Bounds.UPPER), equalTo(avg + (sigma * stdDev)));
        }, valueFieldType);
    }

    public void testBadSigma() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ExtendedStatsBucketPipelineAggregationBuilder("extended_stats_bucket", "histo>sum").sigma(-1.0)
        );
        assertThat(e.getMessage(), equalTo("sigma must be a non-negative double"));
    }

    private void testCase(
        AggregationBuilder aggregationBuilder,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalFilters> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader indexReader = DirectoryReader.open(directory)) {
                verify.accept(searchAndReduce(indexReader, new AggTestConfig(aggregationBuilder, fieldTypes)));
            }
        }
    }
}

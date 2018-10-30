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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class BucketHelperTests extends AggregatorTestCase {

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
        "2017-01-10T22:55:46");

    private static final List<Integer> datasetValues = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

    public void testPathingThroughMultiBucket() {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new DateHistogramAggregationBuilder("histo2")
            .dateHistogramInterval(DateHistogramInterval.HOUR)
            .field(HISTO_FIELD).subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD)));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "histo2>the_avg"));

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Should have thrown exception because of multi-bucket agg")));

        assertThat(e.getMessage(),
            equalTo("[histo2] is a [date_histogram], but only number value or a single value numeric metric aggregations are allowed."));
    }

    public void testhMultiBucketBucketCount() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new DateHistogramAggregationBuilder("histo2")
            .dateHistogramInterval(DateHistogramInterval.HOUR)
            .field(HISTO_FIELD).subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD)));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "histo2._bucket_count"));

        executeTestCase(query, aggBuilder, histogram -> assertThat(((InternalDateHistogram)histogram).getBuckets().size(), equalTo(10)));
    }

    public void testCountOnMultiBucket() {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new DateHistogramAggregationBuilder("histo2")
            .dateHistogramInterval(DateHistogramInterval.HOUR)
            .field(HISTO_FIELD).subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD)));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "histo2._count"));

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Should have thrown exception because of multi-bucket agg")));

        assertThat(e.getMessage(),
            equalTo("[histo2] is a [date_histogram], but only number value or a single value numeric metric aggregations are allowed."));
    }

    public void testPathingThroughSingleBuckets() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new FilterAggregationBuilder("the_filter", new MatchAllQueryBuilder())
            .subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD)));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_filter>the_avg"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertThat(((InternalDateHistogram)histogram).getBuckets().size(), equalTo(10));
        });
    }

    public void testPathingThroughSingleThenMulti() {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new FilterAggregationBuilder("the_filter", new MatchAllQueryBuilder())
            .subAggregation(new DateHistogramAggregationBuilder("histo2")
                .dateHistogramInterval(DateHistogramInterval.HOUR)
                .field(HISTO_FIELD).subAggregation(new AvgAggregationBuilder("the_avg").field(VALUE_FIELD))));

        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_filter>histo2>the_avg"));

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Should have thrown exception because of multi-bucket agg")));

        assertThat(e.getMessage(),
            equalTo("[histo2] is a [date_histogram], but only number value or a single value numeric metric aggregations are allowed."));
    }

    public void testPercentilesWithoutSpecificValue() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new PercentilesAggregationBuilder("the_percentiles").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_percentiles"));

        AggregationExecutionException e = expectThrows(AggregationExecutionException.class,
            () -> executeTestCase(query, aggBuilder, histogram -> fail("Should have thrown exception because of multi-bucket agg")));

        assertThat(e.getMessage(),
            equalTo("[the_percentiles] is a [tdigest_percentiles] which contains multiple values, but only number value or a " +
                "single value numeric metric aggregation.  Please specify which value to return."));
    }

    public void testPercentilesWithSpecificValue() throws IOException {
        Query query = new MatchAllDocsQuery();

        DateHistogramAggregationBuilder aggBuilder = new DateHistogramAggregationBuilder("histo");
        aggBuilder.dateHistogramInterval(DateHistogramInterval.DAY).field(HISTO_FIELD);
        aggBuilder.subAggregation(new PercentilesAggregationBuilder("the_percentiles").field(VALUE_FIELD));
        aggBuilder.subAggregation(new CumulativeSumPipelineAggregationBuilder("cusum", "the_percentiles.99"));

        executeTestCase(query, aggBuilder, histogram -> {
            assertThat(((InternalDateHistogram)histogram).getBuckets().size(), equalTo(10));
        });
    }

    @SuppressWarnings("unchecked")
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

    @SuppressWarnings("unchecked")
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
                histogram = searchAndReduce(indexSearcher, query, aggBuilder, new MappedFieldType[]{fieldType, valueFieldType});
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parser().parseDateTime(dateTime).getMillis();
    }
}

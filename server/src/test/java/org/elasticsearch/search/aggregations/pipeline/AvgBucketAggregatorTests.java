/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalAvg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AvgBucketAggregatorTests extends AggregatorTestCase {
    private static final String DATE_FIELD = "date";
    private static final String VALUE_FIELD = "value";

    private static final List<String> dataset = Arrays.asList(
        "2010-03-12T01:07:45",
        "2010-04-27T03:43:34",
        "2012-05-18T04:11:00",
        "2013-05-29T05:11:31",
        "2013-10-31T08:24:05",
        "2015-02-13T13:09:32",
        "2015-06-24T13:47:43",
        "2015-11-13T16:14:34",
        "2016-03-04T17:09:50",
        "2017-12-12T22:55:46"
    );

    /**
     * Test for issue #30608.  Under the following circumstances:
     *
     * A. Multi-bucket agg in the first entry of our internal list
     * B. Regular agg as the immediate child of the multi-bucket in A
     * C. Regular agg with the same name as B at the top level, listed as the second entry in our internal list
     * D. Finally, a pipeline agg with the path down to B
     *
     * BucketMetrics reduction would throw a class cast exception due to bad subpathing.  This test ensures
     * it is fixed.
     *
     * Note: we have this test inside of the `avg_bucket` package so that we can get access to the package-private
     * `reduce()` needed for testing this
     */
    public void testSameAggNames() throws IOException {
        Query query = new MatchAllDocsQuery();

        AvgAggregationBuilder avgBuilder = new AvgAggregationBuilder("foo").field(VALUE_FIELD);
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("histo").calendarInterval(DateHistogramInterval.YEAR)
            .field(DATE_FIELD)
            .subAggregation(new AvgAggregationBuilder("foo").field(VALUE_FIELD));

        AvgBucketPipelineAggregationBuilder avgBucketBuilder = new AvgBucketPipelineAggregationBuilder("the_avg_bucket", "histo>foo");

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    document.add(new SortedNumericDocValuesField(DATE_FIELD, asLong(date)));
                    document.add(new SortedNumericDocValuesField(VALUE_FIELD, randomInt()));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            InternalAvg avgResult;
            InternalDateHistogram histogramResult;
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                DateFieldMapper.DateFieldType fieldType = new DateFieldMapper.DateFieldType(DATE_FIELD);

                MappedFieldType valueFieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD, NumberFieldMapper.NumberType.LONG);

                avgResult = searchAndReduce(indexSearcher, query, avgBuilder, 10000, new MappedFieldType[] { fieldType, valueFieldType });
                histogramResult = searchAndReduce(indexSearcher, query, histo, 10000, new MappedFieldType[] { fieldType, valueFieldType });
            }

            // Finally, reduce the pipeline agg
            PipelineAggregator avgBucketAgg = avgBucketBuilder.createInternal(Collections.emptyMap());
            List<Aggregation> reducedAggs = new ArrayList<>(2);

            // Histo has to go first to exercise the bug
            reducedAggs.add(histogramResult);
            reducedAggs.add(avgResult);
            Aggregations aggregations = new Aggregations(reducedAggs);
            InternalAggregation pipelineResult = ((AvgBucketPipelineAggregator) avgBucketAgg).doReduce(aggregations, null);
            assertNotNull(pipelineResult);
        }
    }

    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}

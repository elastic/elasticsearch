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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.function.Consumer;
import static java.util.Collections.singleton;

public class DateRangeHistogramAggregatorTests extends AggregatorTestCase {

    public static final String FIELD_NAME = "fieldName";

    public void testBasics() throws Exception {
        RangeFieldMapper.Range range = new RangeFieldMapper.Range(RangeType.DATE, asLong("2019-08-01T12:14:36"),
            asLong("2019-08-01T15:07:22"), true, true);
        testCase(
            new MatchAllDocsQuery(),
            builder -> builder.calendarInterval(DateHistogramInterval.DAY),
            writer -> writer.addDocument(singleton(new BinaryDocValuesField(FIELD_NAME, RangeType.DATE.encodeRanges(singleton(range))))),
            histo -> {
                assertEquals(1, histo.getBuckets().size());
                assertTrue(AggregationInspectionHelper.hasValue(histo));
            }
        );
    }

    private void testCase(Query query,
                          Consumer<DateHistogramAggregationBuilder> configure,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalDateHistogram> verify) throws IOException {
        MappedFieldType fieldType = new RangeFieldMapper.Builder(FIELD_NAME, RangeType.DATE).fieldType();
        fieldType.setName(FIELD_NAME);
        final DateHistogramAggregationBuilder aggregationBuilder = new DateHistogramAggregationBuilder("_name").field(FIELD_NAME);
        if (configure != null) {
            configure.accept(aggregationBuilder);
        }
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testCase(DateHistogramAggregationBuilder aggregationBuilder, Query query,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalDateHistogram> verify,
                          MappedFieldType fieldType) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        DateRangeHistogramAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher,
            fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalDateHistogram) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
    private static long asLong(String dateTime) {
        return DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(dateTime)).toInstant().toEpochMilli();
    }
}

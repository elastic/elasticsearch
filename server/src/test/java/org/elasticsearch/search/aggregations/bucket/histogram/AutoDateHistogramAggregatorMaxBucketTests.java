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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class AutoDateHistogramAggregatorMaxBucketTests extends AggregatorTestCase {

    private static final String DATE_FIELD = "date";
    private static final String INSTANT_FIELD = "instant";

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
            "2017-12-12T22:55:46");

    private void testSearchCase(Query query, List<String> dataset,
                                Consumer<AutoDateHistogramAggregationBuilder> configure,
                                Consumer<Histogram> verify) throws IOException {
        executeTestCase(false, query, dataset, configure, verify);
    }

    public void testWarningIsLoggedWithLargeNumberOfBuckets() throws IOException {
        Query query = new MatchAllDocsQuery();
        testSearchCase(query, dataset,
            aggregation -> aggregation.setNumBuckets(400).field(DATE_FIELD),
                histogram -> assertWarnings("This request will fail in 7.x, because number of buckets," +
                    " 400, is greater than default max buckets (10000) divided by the max rounding interval (30).")
            );
    }

    @Override
    protected IndexSettings createIndexSettings() {
        Settings nodeSettings = Settings.builder()
            .put("search.max_buckets", -1).build();
        return new IndexSettings(
            IndexMetaData.builder("_index").settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            nodeSettings
        );
    }

    private void executeTestCase(boolean reduced, Query query, List<String> dataset,
                                 Consumer<AutoDateHistogramAggregationBuilder> configure,
                                 Consumer<Histogram> verify) throws IOException {

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                for (String date : dataset) {
                    if (frequently()) {
                        indexWriter.commit();
                    }

                    long instant = asLong(date);
                    document.add(new SortedNumericDocValuesField(DATE_FIELD, instant));
                    document.add(new LongPoint(INSTANT_FIELD, instant));
                    indexWriter.addDocument(document);
                    document.clear();
                }
            }

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                AutoDateHistogramAggregationBuilder aggregationBuilder = new AutoDateHistogramAggregationBuilder("_name");
                if (configure != null) {
                    configure.accept(aggregationBuilder);
                }

                DateFieldMapper.Builder builder = new DateFieldMapper.Builder("_name");
                DateFieldMapper.DateFieldType fieldType = builder.fieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName(aggregationBuilder.field());

                InternalAutoDateHistogram histogram;
                if (reduced) {
                    histogram = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
                } else {
                    histogram = search(indexSearcher, query, aggregationBuilder, fieldType);
                }
                verify.accept(histogram);
            }
        }
    }

    private static long asLong(String dateTime) {
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseJoda(dateTime).getMillis();
    }
}

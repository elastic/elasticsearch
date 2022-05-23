/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregator.BucketComparator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class FilterAggregatorTests extends AggregatorTestCase {
    private MappedFieldType fieldType;

    @Before
    public void setUpTest() throws Exception {
        super.setUp();
        fieldType = new KeywordFieldMapper.KeywordFieldType("field");
    }

    public void testEmpty() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
        QueryBuilder filter = QueryBuilders.termQuery("field", randomAlphaOfLength(5));
        FilterAggregationBuilder builder = new FilterAggregationBuilder("test", filter);
        InternalFilter response = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, fieldType);
        assertEquals(response.getDocCount(), 0);
        assertFalse(AggregationInspectionHelper.hasValue(response));
        indexReader.close();
        directory.close();
    }

    public void testRandom() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        int numDocs = randomIntBetween(100, 200);
        int maxTerm = randomIntBetween(10, 50);
        int[] expectedBucketCount = new int[maxTerm];
        Document document = new Document();
        for (int i = 0; i < numDocs; i++) {
            if (frequently()) {
                // make sure we have more than one segment to test the merge
                indexWriter.commit();
            }
            int value = randomInt(maxTerm - 1);
            expectedBucketCount[value] += 1;
            document.add(new Field("field", Integer.toString(value), KeywordFieldMapper.Defaults.FIELD_TYPE));
            indexWriter.addDocument(document);
            document.clear();
        }
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
        try {

            int value = randomInt(maxTerm - 1);
            QueryBuilder filter = QueryBuilders.termQuery("field", Integer.toString(value));
            FilterAggregationBuilder builder = new FilterAggregationBuilder("test", filter);

            final InternalFilter response = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, fieldType);
            assertEquals(response.getDocCount(), (long) expectedBucketCount[value]);
            if (expectedBucketCount[value] > 0) {
                assertTrue(AggregationInspectionHelper.hasValue(response));
            } else {
                assertFalse(AggregationInspectionHelper.hasValue(response));
            }
        } finally {
            indexReader.close();
            directory.close();
        }
    }

    public void testBucketComparator() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexWriter.addDocument(singleton(new Field("field", "1", KeywordFieldMapper.Defaults.FIELD_TYPE)));
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);
                FilterAggregationBuilder builder = new FilterAggregationBuilder("test", new MatchAllQueryBuilder());
                FilterAggregator agg = createAggregator(builder, indexSearcher, fieldType);
                agg.preCollection();
                LeafBucketCollector collector = agg.getLeafCollector(indexReader.leaves().get(0));
                collector.collect(0, 0);
                collector.collect(0, 0);
                collector.collect(0, 1);
                BucketComparator c = agg.bucketComparator(null, SortOrder.ASC);
                assertThat(c.compare(0, 1), greaterThan(0));
                assertThat(c.compare(1, 0), lessThan(0));
                c = agg.bucketComparator("doc_count", SortOrder.ASC);
                assertThat(c.compare(0, 1), greaterThan(0));
                assertThat(c.compare(1, 0), lessThan(0));
                Exception e = expectThrows(
                    IllegalArgumentException.class,
                    () -> agg.bucketComparator("garbage", randomFrom(SortOrder.values()))
                );
                assertThat(
                    e.getMessage(),
                    equalTo(
                        "Ordering on a single-bucket aggregation can only be done on its doc_count. "
                            + "Either drop the key (a la \"test\") or change it to \"doc_count\" (a la \"test.doc_count\") or \"key\"."
                    )
                );
            }
        }
    }
}

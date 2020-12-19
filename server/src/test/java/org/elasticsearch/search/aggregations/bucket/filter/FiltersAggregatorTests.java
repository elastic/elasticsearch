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
package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class FiltersAggregatorTests extends AggregatorTestCase {
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
        int numFilters = randomIntBetween(1, 10);
        QueryBuilder[] filters = new QueryBuilder[numFilters];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = QueryBuilders.termQuery("field", randomAlphaOfLength(5));
        }
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder("test", filters);
        builder.otherBucketKey("other");
        InternalFilters response = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, fieldType);
        assertEquals(response.getBuckets().size(), numFilters);
        for (InternalFilters.InternalBucket filter : response.getBuckets()) {
            assertEquals(filter.getDocCount(), 0);
        }
        assertFalse(AggregationInspectionHelper.hasValue(response));
        indexReader.close();
        directory.close();
    }

    public void testKeyedFilter() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new Field("field", "foo", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", "else", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        // make sure we have more than one segment to test the merge
        indexWriter.commit();
        document.add(new Field("field", "foo", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", "bar", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", "foobar", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.commit();
        document.clear();
        document.add(new Field("field", "something", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.commit();
        document.clear();
        document.add(new Field("field", "foobar", KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        FiltersAggregator.KeyedFilter[] keys = new FiltersAggregator.KeyedFilter[6];
        keys[0] = new FiltersAggregator.KeyedFilter("foobar", QueryBuilders.termQuery("field", "foobar"));
        keys[1] = new FiltersAggregator.KeyedFilter("bar", QueryBuilders.termQuery("field", "bar"));
        keys[2] = new FiltersAggregator.KeyedFilter("foo", QueryBuilders.termQuery("field", "foo"));
        keys[3] = new FiltersAggregator.KeyedFilter("foo2", QueryBuilders.termQuery("field", "foo"));
        keys[4] = new FiltersAggregator.KeyedFilter("same", QueryBuilders.termQuery("field", "foo"));
        // filter name already present so it should be merge with the previous one ?
        keys[5] = new FiltersAggregator.KeyedFilter("same", QueryBuilders.termQuery("field", "bar"));
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder("test", keys);
        builder.otherBucket(true);
        builder.otherBucketKey("other");
        final InternalFilters filters = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, fieldType);
        assertEquals(filters.getBuckets().size(), 7);
        assertEquals(filters.getBucketByKey("foobar").getDocCount(), 2);
        assertEquals(filters.getBucketByKey("foo").getDocCount(), 2);
        assertEquals(filters.getBucketByKey("foo2").getDocCount(), 2);
        assertEquals(filters.getBucketByKey("bar").getDocCount(), 1);
        assertEquals(filters.getBucketByKey("same").getDocCount(), 1);
        assertEquals(filters.getBucketByKey("other").getDocCount(), 2);
        assertTrue(AggregationInspectionHelper.hasValue(filters));

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
            int numFilters = randomIntBetween(1, 10);
            QueryBuilder[] filters = new QueryBuilder[numFilters];
            int[] filterTerms = new int[numFilters];
            int expectedOtherCount = numDocs;
            Set<Integer> filterSet = new HashSet<>();
            for (int i = 0; i < filters.length; i++) {
                int value = randomInt(maxTerm - 1);
                filters[i] = QueryBuilders.termQuery("field", Integer.toString(value));
                filterTerms[i] = value;
                if (filterSet.contains(value) == false) {
                    expectedOtherCount -= expectedBucketCount[value];
                    filterSet.add(value);
                }
            }
            FiltersAggregationBuilder builder = new FiltersAggregationBuilder("test", filters);
            builder.otherBucket(true);
            builder.otherBucketKey("other");

            final InternalFilters response = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), builder, fieldType);
            List<InternalFilters.InternalBucket> buckets = response.getBuckets();
            assertEquals(buckets.size(), filters.length + 1);

            for (InternalFilters.InternalBucket bucket : buckets) {
                if ("other".equals(bucket.getKey())) {
                    assertEquals(bucket.getDocCount(), expectedOtherCount);
                } else {
                    int index = Integer.parseInt(bucket.getKey());
                    assertEquals(bucket.getDocCount(), (long) expectedBucketCount[filterTerms[index]]);
                }
            }

            // Always true because we include 'other' in the agg
            assertTrue(AggregationInspectionHelper.hasValue(response));
        } finally {
            indexReader.close();
            directory.close();
        }
    }

    /**
     * Test that we perform the appropriate unwrapping to merged queries.
     */
    public void testFilterMatchingBoth() throws IOException {
        Query topLevelQuery = LongPoint.newRangeQuery(
            "test",
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-02-01")
        );
        Query filterQuery = LongPoint.newRangeQuery(
            "test",
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-02-01")
        );
        Query matchingBoth = FiltersAggregator.filterMatchingBoth(new IndexOrDocValuesQuery(topLevelQuery, mock(Query.class)), filterQuery);
        /*
         * The topLevelQuery is entirely contained within the filter query so
         * it is good enough to match that. See MergedPointRangeQueryTests for
         * tons more tests around this. Really in this test we're just excited
         * to prove that we unwrapped the IndexOrDocValuesQuery above. 
         */
        assertThat(matchingBoth, equalTo(topLevelQuery));
    }

    public void testWithMergedPointRangeQueries() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType("test", Resolution.MILLISECONDS);
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2020-01-01").to("2020-03-01").includeUpper(false))
        );
        Query query = LongPoint.newRangeQuery(
            "test",
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-02-01")
        );
        testCase(builder, query, iw -> {
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02"))));
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02"))));
        }, result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(1));
            assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));
        }, ft);
    }

    public void testFilterByFilterCost() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType(
            "test",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null
        );
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2020-01-01").to("2020-03-01").includeUpper(false))
        );
        withAggregator(
            builder,
            new MatchAllDocsQuery(),
            iw -> {
                iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02"))));
                iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02"))));
            },
            (searcher, agg) -> {
                assertThat(agg, instanceOf(FiltersAggregator.FilterByFilter.class));
                FiltersAggregator.FilterByFilter filterByFilter = (FiltersAggregator.FilterByFilter) agg;
                int maxDoc = searcher.getIndexReader().maxDoc();
                assertThat(filterByFilter.estimateCost(maxDoc), equalTo(1L));
                assertThat(filterByFilter.scorersCached(), equalTo(true));
                Map<String, Object> debug = new HashMap<>();
                filterByFilter.collectDebugInfo(debug::put);
                assertThat(debug, hasEntry("segments_with_deleted_docs", 0));
                assertThat(debug, hasEntry("estimated_cost", 1L));
                assertThat(debug, hasEntry("max_cost", (long) maxDoc));
                assertThat(debug, hasEntry("estimate_cost_time", 0L));
            },
            ft
        );
    }
}

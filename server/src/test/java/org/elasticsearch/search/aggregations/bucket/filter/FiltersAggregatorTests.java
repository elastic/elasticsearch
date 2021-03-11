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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.CustomTermFreqField;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregatorTests;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.internal.ContextIndexSearcherTests.DocumentSubsetDirectoryReader;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
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

    public void testNoFilters() throws IOException {
        testCase(new FiltersAggregationBuilder("test", new KeyedFilter[0]), new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of());
        }, (InternalFilters result) -> {
            assertThat(result.getBuckets(), hasSize(0));
        });
    }

    public void testNoFiltersWithSubAggs() throws IOException {
        testCase(
            new FiltersAggregationBuilder("test", new KeyedFilter[0]).subAggregation(new MaxAggregationBuilder("m").field("i")),
            new MatchAllDocsQuery(),
            iw -> { iw.addDocument(List.of(new SortedNumericDocValuesField("i", 1))); },
            (InternalFilters result) -> { assertThat(result.getBuckets(), hasSize(0)); },
            new NumberFieldMapper.NumberFieldType("m", NumberFieldMapper.NumberType.INTEGER)
        );
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
     * Test that we perform the appropriate unwrapping to merge queries.
     */
    public void testMergingQueries() throws IOException {
        DateFieldMapper.DateFieldType ft = new DateFieldMapper.DateFieldType("test");
        Query topLevelQuery = ft.rangeQuery("2020-01-01", "2020-02-01", true, true, null, null, null, mock(SearchExecutionContext.class));
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder(
            "t",
            // The range query will be wrapped in IndexOrDocValuesQuery by the date field type
            new KeyedFilter("k", new RangeQueryBuilder("test").from("2020-01-01").to("2020-02-01"))
        );
        withAggregator(builder, topLevelQuery, iw -> {
            /*
             * There has to be a document inside the query and one outside
             * the query or we'll end up with MatchAll or MathNone.
             */
            long time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02");
            iw.addDocument(List.of(new LongPoint("test", time), new SortedNumericDocValuesField("test", time)));
            time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02");
            iw.addDocument(List.of(new LongPoint("test", time), new SortedNumericDocValuesField("test", time)));
            time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01");
            iw.addDocument(List.of(new LongPoint("test", time), new SortedNumericDocValuesField("test", time)));
            time = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-02-01");
            iw.addDocument(List.of(new LongPoint("test", time), new SortedNumericDocValuesField("test", time)));
        }, (searcher, aggregator) -> {
            /*
             * The topLevelQuery is entirely contained within the filter query so
             * it is good enough to match that. See MergedPointRangeQueryTests for
             * tons more tests around this. Really in this test we're just excited
             * to prove that we unwrapped the IndexOrDocValuesQuery that the date
             * field mapper adds
             */
            QueryToFilterAdapter<?> filter = ((FiltersAggregator) aggregator).filters().get(0);
            assertThat(filter.query(), equalTo(((IndexOrDocValuesQuery) topLevelQuery).getIndexQuery()));
            Map<String, Object> debug = new HashMap<>();
            filter.collectDebugInfo(debug::put);
            assertThat(debug, hasEntry("query", ((IndexOrDocValuesQuery) topLevelQuery).getIndexQuery().toString()));
        }, ft);
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
            Collections.emptyMap()
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
                Map<String, Object> debug = new HashMap<>();
                filterByFilter.collectDebugInfo(debug::put);
                assertThat(debug, hasEntry("segments_with_deleted_docs", 0));
                assertThat(debug, hasEntry("estimated_cost", 1L));
                assertThat(debug, hasEntry("max_cost", (long) maxDoc));
                assertThat(debug, hasEntry("estimate_cost_time", 0L));
                List<?> filtersDebug = (List<?>) debug.get("filters");
                for (int i = 0; i < filterByFilter.filters().size(); i++) {
                    Map<?, ?> filterDebug = (Map<?, ?>) filtersDebug.get(i);
                    assertThat((int) filterDebug.get("scorers_prepared_while_estimating_cost"), greaterThan(0));
                }
            },
            ft
        );
    }

    /**
     * Check that we don't accidentally find nested documents when the filter
     * matches it.
     */
    public void testNested() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("author");
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> iw.addDocuments(
            NestedAggregatorTests.generateBook("test", new String[] { "foo", "bar" }, new int[] { 5, 10, 15, 20 })
        );
        testCase(
            new FiltersAggregationBuilder("test", new KeyedFilter("q1", new TermQueryBuilder("author", "foo"))),
            Queries.newNonNestedFilter(),
            buildIndex,
            result -> {
                InternalFilters filters = (InternalFilters) result;
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));
            },
            ft
        );
        testCase(
            new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder())),
            Queries.newNonNestedFilter(),
            buildIndex,
            result -> {
                InternalFilters filters = (InternalFilters) result;
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));
            },
            ft
        );
    }

    public void testMatchAll() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of());
            }
        };
        withAggregator(builder, new MatchAllDocsQuery(), buildIndex, (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            // The estimated cost is 0 because we're going to read from metadata
            assertThat(((FiltersAggregator.FilterByFilter) aggregator).estimateCost(Long.MAX_VALUE), equalTo(0L));
            Map<String, Object> debug = collectAndGetFilterDebugInfo(searcher, aggregator);
            assertThat(debug, hasEntry("specialized_for", "match_all"));
            assertThat((int) debug.get("results_from_metadata"), greaterThan(0));
        });
        testCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters result) -> {
                assertThat(result.getBuckets(), hasSize(1));
                assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(10L));
            }
        );
    }

    public void testMatchAllWithDocCount() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of(new CustomTermFreqField(DocCountFieldMapper.NAME, DocCountFieldMapper.NAME, i + 1)));
            }
        };
        withAggregator(builder, new MatchAllDocsQuery(), buildIndex, (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            // The estimated cost is 0 because we're going to read from metadata
            assertThat(((FiltersAggregator.FilterByFilter) aggregator).estimateCost(Long.MAX_VALUE), equalTo(10L));
            Map<String, Object> debug = collectAndGetFilterDebugInfo(searcher, aggregator);
            assertThat(debug, hasEntry("specialized_for", "match_all"));
            assertThat(debug, hasEntry("results_from_metadata", 0));
        });
        testCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters result) -> {
                assertThat(result.getBuckets(), hasSize(1));
                assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(55L));
            }
        );
    }

    /**
     * This runs {@code filters} with a single {@code match_all} filter with
     * the index set up kind of like document level security. As a bonus, this
     * "looks" to the agg just like an index with deleted documents.
     */
    public void testMatchAllOnFilteredIndex() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < 10; i++) {
                indexWriter.addDocument(List.of(new LongPoint("t", i)));
            }
            indexWriter.close();

            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(createIndexSettings(), new BitsetFilterCache.Listener() {
                    @Override
                    public void onRemoval(ShardId shardId, Accountable accountable) {}

                    @Override
                    public void onCache(ShardId shardId, Accountable accountable) {}
                });
                IndexReader limitedReader = new DocumentSubsetDirectoryReader(
                    ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(bitsetFilterCache.index(), 0)),
                    bitsetFilterCache,
                    LongPoint.newRangeQuery("t", 5, Long.MAX_VALUE)
                );
                IndexSearcher searcher = newIndexSearcher(limitedReader);
                AggregationContext context = createAggregationContext(searcher, new MatchAllDocsQuery());
                FiltersAggregator.FilterByFilter aggregator = createAggregator(builder, context);
                // The estimated cost is 0 because we're going to read from metadata
                assertThat(((FiltersAggregator.FilterByFilter) aggregator).estimateCost(Long.MAX_VALUE), equalTo(10L));
                aggregator.preCollection();
                searcher.search(context.query(), aggregator);
                aggregator.postCollection();
                InternalAggregation result = aggregator.buildTopLevel();
                result = result.reduce(
                    List.of(result),
                    InternalAggregation.ReduceContext.forFinalReduction(
                        context.bigArrays(),
                        getMockScriptService(),
                        b -> {},
                        PipelineTree.EMPTY
                    )
                );
                InternalFilters filters = (InternalFilters) result;
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(5L));
                Map<String, Object> debug = new HashMap<>();
                ((FiltersAggregator.FilterByFilter) aggregator).filters().get(0).collectDebugInfo(debug::put);
                assertThat(debug, hasEntry("specialized_for", "match_all"));
                assertThat(debug, hasEntry("results_from_metadata", 0));
            }
        }
    }

    public void testMatchNone() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new RangeQueryBuilder("missing").gte(0)));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of(new LongPoint("t", i)));
            }
        };
        withAggregator(builder, new MatchAllDocsQuery(), buildIndex, (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            // The estimated cost is 0 because we're going to read from metadata
            assertThat(((FiltersAggregator.FilterByFilter) aggregator).estimateCost(Long.MAX_VALUE), equalTo(0L));
            Map<String, Object> debug = collectAndGetFilterDebugInfo(searcher, aggregator);
            assertThat(debug, hasEntry("specialized_for", "match_none"));
        });
        testCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters result) -> {
                assertThat(result.getBuckets(), hasSize(1));
                assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(0L));
            }
        );
    }

    public void testTermQuery() throws IOException {
        KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType("f", true, false, Collections.emptyMap());
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchQueryBuilder("f", "0")));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                BytesRef bytes = new BytesRef(Integer.toString(i % 3));
                iw.addDocument(List.of(new Field("f", bytes, KeywordFieldMapper.Defaults.FIELD_TYPE)));
            }
        };
        withAggregator(builder, new MatchAllDocsQuery(), buildIndex, (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            // The estimated cost is 0 because we're going to read from metadata
            assertThat(((FiltersAggregator.FilterByFilter) aggregator).estimateCost(Long.MAX_VALUE), equalTo(0L));
            Map<String, Object> debug = collectAndGetFilterDebugInfo(searcher, aggregator);
            assertThat(debug, hasEntry("specialized_for", "term"));
            assertThat((int) debug.get("results_from_metadata"), greaterThan(0));
            assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), equalTo(0));
        }, ft);
        testCase(builder, new MatchAllDocsQuery(), buildIndex, (InternalFilters result) -> {
            assertThat(result.getBuckets(), hasSize(1));
            assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(4L));
        }, ft);
    }

    public void testSubAggs() throws IOException {
        MappedFieldType dateFt = new DateFieldMapper.DateFieldType(
            "test",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
        MappedFieldType intFt = new NumberFieldMapper.NumberFieldType("int", NumberType.INTEGER);
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2010-01-01").to("2010-03-01").includeUpper(false)),
            new KeyedFilter("q2", new RangeQueryBuilder("test").from("2020-01-01").to("2020-03-01").includeUpper(false))
        ).subAggregation(new MaxAggregationBuilder("m").field("int")).subAggregation(new SumAggregationBuilder("s").field("int"));
        List<List<IndexableField>> docs = new ArrayList<>();
        docs.add(
            List.of(
                new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02")),
                new SortedNumericDocValuesField("int", 100)
            )
        );
        docs.add(
            List.of(
                new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02")),
                new SortedNumericDocValuesField("int", 5)
            )
        );
        docs.add(
            List.of(
                new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-03")),
                new SortedNumericDocValuesField("int", 10)
            )
        );
         /*
          * Shuffle the docs so we collect them in a random order which causes
          * bad implementations of filter-by-filter aggregation to fail with
          * assertion errors while executing.
          */
        Collections.shuffle(docs, random());
        testCase(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(2));

            InternalFilters.InternalBucket b = filters.getBucketByKey("q1");
            assertThat(b.getDocCount(), equalTo(1L));
            InternalMax max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(100.0));
            InternalSum sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(100.0));

            b = filters.getBucketByKey("q2");
            assertThat(b.getDocCount(), equalTo(2L));
            max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(10.0));
            sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(15.0));
        }, dateFt, intFt);
        withAggregator(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            FiltersAggregator.FilterByFilter filterByFilter = (FiltersAggregator.FilterByFilter) aggregator;
            int maxDoc = searcher.getIndexReader().maxDoc();
            assertThat(filterByFilter.estimateCost(maxDoc), equalTo(3L));
            Map<String, Object> debug = new HashMap<>();
            filterByFilter.filters().get(0).collectDebugInfo(debug::put);
            assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), greaterThanOrEqualTo(1));
            debug = new HashMap<>();
            filterByFilter.filters().get(1).collectDebugInfo(debug::put);
            assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), greaterThanOrEqualTo(1));
        }, dateFt, intFt);
    }

    public void testSubAggsManyDocs() throws IOException {
        MappedFieldType dateFt = new DateFieldMapper.DateFieldType(
            "test",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
        MappedFieldType intFt = new NumberFieldMapper.NumberFieldType("int", NumberType.INTEGER);
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2010-01-01").to("2010-03-01").includeUpper(false)),
            new KeyedFilter("q2", new RangeQueryBuilder("test").from("2020-01-01").to("2020-03-01").includeUpper(false))
        ).subAggregation(new MaxAggregationBuilder("m").field("int")).subAggregation(new SumAggregationBuilder("s").field("int"));
        List<List<IndexableField>> docs = new ArrayList<>();
        long[] times = new long[] {
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-03"),
        };
        for (int i = 0; i < 10000; i++) {
            docs.add(List.of(new LongPoint("test", times[i % 3]), new SortedNumericDocValuesField("int", i)));
        }
         /*
          * Shuffle the docs so we collect them in a random order which causes
          * bad implementations of filter-by-filter aggregation to fail with
          * assertion errors while executing.
          */
        Collections.shuffle(docs, random());
        testCase(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(2));

            InternalFilters.InternalBucket b = filters.getBucketByKey("q1");
            assertThat(b.getDocCount(), equalTo(3334L));
            InternalMax max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(9999.0));
            InternalSum sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(16668333.0));

            b = filters.getBucketByKey("q2");
            assertThat(b.getDocCount(), equalTo(6666L));
            max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(9998.0));
            sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(33326667.0));
        }, dateFt, intFt);
        withAggregator(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            FiltersAggregator.FilterByFilter filterByFilter = (FiltersAggregator.FilterByFilter) aggregator;
            int maxDoc = searcher.getIndexReader().maxDoc();
            assertThat(filterByFilter.estimateCost(maxDoc), both(greaterThanOrEqualTo(10000L)).and(lessThan(20000L)));
            Map<String, Object> debug = new HashMap<>();
            filterByFilter.filters().get(0).collectDebugInfo(debug::put);
            assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), greaterThanOrEqualTo(1));
            debug = new HashMap<>();
            filterByFilter.filters().get(1).collectDebugInfo(debug::put);
            assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), greaterThanOrEqualTo(1));
        }, dateFt, intFt);
    }

    public void testSubAggsManyFilters() throws IOException {
        MappedFieldType dateFt = new DateFieldMapper.DateFieldType(
            "test",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
        MappedFieldType intFt = new NumberFieldMapper.NumberFieldType("int", NumberType.INTEGER);
        List<KeyedFilter> buckets = new ArrayList<>();
        DateFormatter formatter = DateFormatter.forPattern("strict_date");
        long start = formatter.parseMillis("2010-01-01");
        long lastRange = formatter.parseMillis("2020-03-01");
        while (start < lastRange) {
            long end = start + TimeUnit.DAYS.toMillis(30);
            String key = formatter.formatMillis(start) + " to " + formatter.formatMillis(end);
            buckets.add(new KeyedFilter(key, new RangeQueryBuilder("test").from(start).to(end).includeUpper(false)));
            start = end;
        }
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            buckets.toArray(KeyedFilter[]::new)
        ).subAggregation(new MaxAggregationBuilder("m").field("int")).subAggregation(new SumAggregationBuilder("s").field("int"));
        List<List<IndexableField>> docs = new ArrayList<>();
        long[] times = new long[] {
            formatter.parseMillis("2010-01-02"),
            formatter.parseMillis("2020-01-02"),
            formatter.parseMillis("2020-01-03"), };
        for (int i = 0; i < 10000; i++) {
            docs.add(List.of(new LongPoint("test", times[i % 3]), new SortedNumericDocValuesField("int", i)));
        }
         /*
          * Shuffle the docs so we collect them in a random order which causes
          * bad implementations of filter-by-filter aggregation to fail with
          * assertion errors while executing.
          */
        Collections.shuffle(docs, random());
        testCase(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(buckets.size()));

            InternalFilters.InternalBucket b = filters.getBucketByKey("2010-01-01 to 2010-01-31");
            assertThat(b.getDocCount(), equalTo(3334L));
            InternalMax max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(9999.0));
            InternalSum sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(16668333.0));

            b = filters.getBucketByKey("2019-12-10 to 2020-01-09");
            assertThat(b.getDocCount(), equalTo(6666L));
            max = b.getAggregations().get("m");
            assertThat(max.getValue(), equalTo(9998.0));
            sum = b.getAggregations().get("s");
            assertThat(sum.getValue(), equalTo(33326667.0));
        }, dateFt, intFt);
        withAggregator(builder, new MatchAllDocsQuery(), iw -> iw.addDocuments(docs), (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FiltersAggregator.FilterByFilter.class));
            FiltersAggregator.FilterByFilter filterByFilter = (FiltersAggregator.FilterByFilter) aggregator;
            int maxDoc = searcher.getIndexReader().maxDoc();
            assertThat(filterByFilter.estimateCost(maxDoc), both(greaterThanOrEqualTo(10000L)).and(lessThan(20000L)));
            for (int b = 0; b < buckets.size(); b++) {
                Map<String, Object> debug = new HashMap<>();
                filterByFilter.filters().get(0).collectDebugInfo(debug::put);
                assertThat((int) debug.get("scorers_prepared_while_estimating_cost"), greaterThanOrEqualTo(1));
            }
        }, dateFt, intFt);
    }



    @Override
    protected List<ObjectMapper> objectMappers() {
        return MOCK_OBJECT_MAPPERS;
    }

    private Map<String, Object> collectAndGetFilterDebugInfo(IndexSearcher searcher, Aggregator aggregator) throws IOException {
        aggregator.preCollection();
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            expectThrows(CollectionTerminatedException.class, () -> aggregator.getLeafCollector(ctx));
        }
        Map<String, Object> debug = new HashMap<>();
        ((FiltersAggregator.FilterByFilter) aggregator).filters().get(0).collectDebugInfo(debug::put);
        return debug;
    }

    static final List<ObjectMapper> MOCK_OBJECT_MAPPERS = List.of(NestedAggregatorTests.nestedObject("nested_chapters"));
}

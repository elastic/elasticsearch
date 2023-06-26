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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator.KeyedFilter;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregatorTests;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.internal.ContextIndexSearcherTests.DocumentSubsetDirectoryReader;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.test.MapMatcher;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class FiltersAggregatorTests extends AggregatorTestCase {
    public void testEmpty() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();
        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);
        int numFilters = randomIntBetween(1, 10);
        QueryBuilder[] filters = new QueryBuilder[numFilters];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = QueryBuilders.termQuery("field", randomAlphaOfLength(5));
        }
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder("test", filters);
        builder.otherBucketKey("other");
        InternalFilters response = searchAndReduce(indexSearcher, new AggTestConfig(builder, new KeywordFieldType("field")));
        assertEquals(response.getBuckets().size(), numFilters);
        for (InternalFilters.InternalBucket filter : response.getBuckets()) {
            assertEquals(filter.getDocCount(), 0);
        }
        assertFalse(AggregationInspectionHelper.hasValue(response));
        indexReader.close();
        directory.close();
    }

    public void testBuildEmpty() throws IOException {
        int numFilters = randomIntBetween(1, 10);
        QueryBuilder[] filters = new QueryBuilder[numFilters];
        for (int i = 0; i < filters.length; i++) {
            filters[i] = QueryBuilders.termQuery("field", randomAlphaOfLength(5));
        }
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder("test", filters);
        boolean askForOtherBucket = true;
        if (askForOtherBucket) {
            builder.otherBucket(true).otherBucketKey("other");
        }
        withAggregator(builder, new MatchAllDocsQuery(), iw -> {}, (searcher, aggregator) -> {
            InternalFilters result = (InternalFilters) aggregator.buildEmptyAggregation();
            for (int i = 0; i < filters.length; i++) {
                assertThat(result.getBucketByKey(String.valueOf(i)).getDocCount(), equalTo(0L));
            }
            if (askForOtherBucket) {
                assertThat(result.getBucketByKey("other").getDocCount(), equalTo(0L));
            } else {
                assertThat(result.getBucketByKey("other"), nullValue());
            }
        });
    }

    public void testNoFilters() throws IOException {
        FiltersAggregationBuilder aggregationBuilder = new FiltersAggregationBuilder("test", new KeyedFilter[0]);
        testCase(
            iw -> { iw.addDocument(List.of()); },
            (InternalFilters result) -> { assertThat(result.getBuckets(), hasSize(0)); },
            new AggTestConfig(aggregationBuilder)
        );
    }

    public void testNoFiltersWithSubAggs() throws IOException {
        testCase(iw -> { iw.addDocument(List.of(new SortedNumericDocValuesField("i", 1))); }, (InternalFilters result) -> {
            assertThat(result.getBuckets(), hasSize(0));
        },
            new AggTestConfig(
                new FiltersAggregationBuilder("test", new KeyedFilter[0]).subAggregation(new MaxAggregationBuilder("m").field("i")),
                new NumberFieldMapper.NumberFieldType("m", NumberType.INTEGER)
            )
        );
    }

    public void testKeyedFilter() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new Field("field", new BytesRef("foo"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", new BytesRef("else"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        // make sure we have more than one segment to test the merge
        indexWriter.commit();
        document.add(new Field("field", new BytesRef("foo"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", new BytesRef("bar"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        document.clear();
        document.add(new Field("field", new BytesRef("foobar"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.commit();
        document.clear();
        document.add(new Field("field", new BytesRef("something"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.commit();
        document.clear();
        document.add(new Field("field", new BytesRef("foobar"), KeywordFieldMapper.Defaults.FIELD_TYPE));
        indexWriter.addDocument(document);
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

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
        final InternalFilters filters = searchAndReduce(indexSearcher, new AggTestConfig(builder, new KeywordFieldType("field")));
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
            document.add(new Field("field", new BytesRef(Integer.toString(value)), KeywordFieldMapper.Defaults.FIELD_TYPE));
            indexWriter.addDocument(document);
            document.clear();
        }
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);
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

            final InternalFilters response = searchAndReduce(indexSearcher, new AggTestConfig(builder, new KeywordFieldType("field")));
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
            QueryToFilterAdapter filter = ((FiltersAggregator) aggregator).filters().get(0);
            assertThat(filter.query(), equalTo(((IndexOrDocValuesQuery) topLevelQuery).getIndexQuery()));
            Map<String, Object> debug = new HashMap<>();
            filter.collectDebugInfo(debug::put);
            assertMap(debug, matchesMap().extraOk().entry("query", ((IndexOrDocValuesQuery) topLevelQuery).getIndexQuery().toString()));
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
        testCase(iw -> {
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02"))));
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02"))));
        }, result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(1));
            assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));
        }, new AggTestConfig(builder, ft).withQuery(query));
    }

    public void testWithEmptyMergedPointRangeQueries() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType("test", Resolution.MILLISECONDS);
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2020-01-01").to("2020-02-01").includeUpper(false))
        );
        Query query = LongPoint.newRangeQuery(
            "test",
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-01"),
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2015-02-01")
        );
        testCase(iw -> {
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02"))));
            iw.addDocument(List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02"))));
        }, result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(1));
            assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(0L));
        }, new AggTestConfig(builder, ft).withQuery(query));
    }

    public void testRangeFilter() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType(
            "test",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from("2020-01-01").to("2020-03-01").includeUpper(false))
        );
        withIndex(iw -> {
            iw.addDocuments(
                List.of(
                    List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("1999-01-02"))),
                    List.of(new LongPoint("test", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02")))
                )
            );
        }, searcher -> {
            debugTestCase(
                builder,
                new MatchAllDocsQuery(),
                searcher,
                (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                    assertThat(filters.getBuckets(), hasSize(1));
                    assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));

                    assertThat(impl, equalTo(FilterByFilterAggregator.class));
                    assertMap(
                        debug,
                        matchesMap().entry(
                            "test",
                            matchesMap().entry("segments_with_doc_count_field", 0)
                                .entry("segments_with_deleted_docs", 0)
                                .entry("segments_collected", 0)
                                .entry("segments_counted", greaterThanOrEqualTo(1))
                                .entry(
                                    "filters",
                                    matchesList().item(
                                        matchesMap().entry("query", "test:[1577836800000 TO 1583020799999]")
                                            .entry("segments_counted_in_constant_time", 1)
                                    )
                                )
                        )
                    );
                },
                ft
            );
        });
    }

    /**
     * Tests a filter that needs the cache to be fast.
     */
    public void testPhraseFilter() throws IOException {
        MappedFieldType ft = new TextFieldMapper.TextFieldType("test", randomBoolean());
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new MatchPhraseQueryBuilder("test", "will find me").slop(0))
        );
        withIndex(iw -> {
            iw.addDocuments(
                List.of(
                    List.of(new Field("test", "will not find me", TextFieldMapper.Defaults.FIELD_TYPE)),
                    List.of(new Field("test", "will find me", TextFieldMapper.Defaults.FIELD_TYPE))
                )
            );
        }, searcher -> {
            searcher.setQueryCachingPolicy(new QueryCachingPolicy() {
                @Override
                public boolean shouldCache(Query query) throws IOException {
                    return true;
                }

                @Override
                public void onUse(Query query) {}
            });
            for (Matcher<Integer> segmentsCountedInConstantTime : List.of(equalTo(0), greaterThanOrEqualTo(1))) {
                debugTestCase(
                    builder,
                    new MatchAllDocsQuery(),
                    searcher,
                    (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                        assertThat(filters.getBuckets(), hasSize(1));
                        assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));

                        assertThat(impl, equalTo(FilterByFilterAggregator.class));
                        assertMap(
                            debug,
                            matchesMap().entry(
                                "test",
                                matchesMap().entry("segments_with_doc_count_field", 0)
                                    .entry("segments_with_deleted_docs", 0)
                                    .entry("segments_collected", 0)
                                    .entry("segments_counted", greaterThanOrEqualTo(1))
                                    .entry(
                                        "filters",
                                        matchesList().item(
                                            matchesMap().entry("query", "test:\"will find me\"")
                                                .entry("segments_counted_in_constant_time", segmentsCountedInConstantTime)
                                        )
                                    )
                            )
                        );
                    },
                    ft
                );
            }
        });
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
        testCase(buildIndex, result1 -> {
            InternalFilters filters1 = (InternalFilters) result1;
            assertThat(filters1.getBuckets(), hasSize(1));
            assertThat(filters1.getBucketByKey("q1").getDocCount(), equalTo(1L));
        },
            new AggTestConfig(new FiltersAggregationBuilder("test", new KeyedFilter("q1", new TermQueryBuilder("author", "foo"))), ft)
                .withQuery(Queries.newNonNestedFilter(IndexVersion.CURRENT))
        );
        testCase(buildIndex, result -> {
            InternalFilters filters = (InternalFilters) result;
            assertThat(filters.getBuckets(), hasSize(1));
            assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));
        },
            new AggTestConfig(new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder())), ft).withQuery(
                Queries.newNonNestedFilter(IndexVersion.CURRENT)
            )
        );
    }

    public void testMatchAll() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of());
            }
        };
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(10L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry("segments_collected", 0)
                            .entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry("query", "*:*").entry("segments_counted_in_constant_time", greaterThan(0))
                                )
                            )
                    )
                );
            }
        );
    }

    public void testMatchAllWithDocCount() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of(DocCountFieldMapper.field(i + 1)));
            }
        };
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(55L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry("segments_collected", 0)
                            .entry("segments_with_doc_count_field", greaterThanOrEqualTo(1))
                            .entry("segments_with_deleted_docs", 0)
                            .entry(
                                "filters",
                                matchesList().item(matchesMap().entry("query", "*:*").entry("segments_counted_in_constant_time", 0))
                            )
                    )
                );
            }
        );
    }

    /**
     * When there is more than one filter and the docs use the {@code _doc_count} field
     * we disable filter-by-filter mode because decoding the {@code _doc_count} is so
     * expensive.
     */
    public void testTwoTermsWithDocCount() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q0", new TermQueryBuilder("a", "0")),
            new KeyedFilter("q1", new TermQueryBuilder("a", "1"))
        );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(
                    List.of(
                        new Field("a", new BytesRef(Integer.toString(i % 2)), KeywordFieldMapper.Defaults.FIELD_TYPE),
                        DocCountFieldMapper.field(i + 1)
                    )
                );
            }
        };
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(2));
                assertThat(filters.getBucketByKey("q0").getDocCount(), equalTo(25L));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(30L));

                assertThat(impl, equalTo(FiltersAggregator.Compatible.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry(
                            "filters",
                            matchesList().item(matchesMap().entry("segments_counted_in_constant_time", 0).entry("query", "a:0"))
                                .item(matchesMap().entry("segments_counted_in_constant_time", 0).entry("query", "a:1"))
                        )
                    )
                );
            },
            new KeywordFieldType("a")
        );
    }

    /**
     * This runs {@code filters} with a single {@code match_all} filter with
     * the index set up kind of like document level security. As a bonus, this
     * "looks" to the agg just like an index with deleted documents.
     * <p>
     * We actually <strong>do</strong> get to use the constant time counting
     * here because {@code match_all} knows how to run constant time counts
     * on filtered indices - that what {@link IndexReader#numDocs()} does.
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
                final IndexSettings indexSettings = createIndexSettings();
                BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
                    @Override
                    public void onRemoval(ShardId shardId, Accountable accountable) {}

                    @Override
                    public void onCache(ShardId shardId, Accountable accountable) {}
                });
                DirectoryReader limitedReader = new DocumentSubsetDirectoryReader(
                    ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(indexSettings.getIndex(), 0)),
                    bitsetFilterCache,
                    LongPoint.newRangeQuery("t", 5, Long.MAX_VALUE)
                );
                IndexSearcher searcher = newIndexSearcher(limitedReader);
                int segmentsWithLiveDocs = (int) searcher.getIndexReader()
                    .leaves()
                    .stream()
                    .map(LeafReaderContext::reader)
                    .filter(leafReader -> leafReader.getLiveDocs() != null)
                    .count();
                debugTestCase(
                    builder,
                    new MatchAllDocsQuery(),
                    searcher,
                    (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                        assertThat(filters.getBuckets(), hasSize(1));
                        assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(5L));
                        assertMap(
                            debug,
                            matchesMap().entry(
                                "test",
                                matchesMap().entry("segments_counted", greaterThanOrEqualTo(1))
                                    .entry("segments_collected", 0)
                                    .entry("segments_with_doc_count_field", 0)
                                    .entry("segments_with_deleted_docs", segmentsWithLiveDocs)
                                    .entry(
                                        "filters",
                                        matchesList().item(
                                            matchesMap().entry("query", "*:*")
                                                .entry("segments_counted_in_constant_time", searcher.getLeafContexts().size())
                                        )
                                    )
                            )
                        );
                    }
                );
            }
        }
    }

    /**
     * This runs {@code filters} with a single {@code term} filter with
     * the index set up kind of like document level security. As a bonus, this
     * "looks" to the agg just like an index with deleted documents.
     * <p>
     * Segments with a filter that doesn't rewrite to {@code match_all} can't
     * take the fast path. But segments who's filter rewrites to {@code match_all}
     * can use the fast path - thus the assertion at the bottom of this:
     * {@code "segments_counted_in_constant_time", lessThan(searcher.getLeafContexts().size())}.
     */
    public void testTermOnFilteredIndex() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("foo");
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new TermQueryBuilder("foo", "bar")));
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < 10; i++) {
                indexWriter.addDocument(
                    List.of(new Field("foo", new BytesRef("bar"), KeywordFieldMapper.Defaults.FIELD_TYPE), new LongPoint("t", i))
                );
            }
            indexWriter.close();

            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                final IndexSettings indexSettings = createIndexSettings();
                BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
                    @Override
                    public void onRemoval(ShardId shardId, Accountable accountable) {}

                    @Override
                    public void onCache(ShardId shardId, Accountable accountable) {}
                });
                DirectoryReader limitedReader = new DocumentSubsetDirectoryReader(
                    ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(indexSettings.getIndex(), 0)),
                    bitsetFilterCache,
                    LongPoint.newRangeQuery("t", 5, Long.MAX_VALUE)
                );
                IndexSearcher searcher = newIndexSearcher(limitedReader);
                int segmentsWithLiveDocs = (int) searcher.getIndexReader()
                    .leaves()
                    .stream()
                    .map(LeafReaderContext::reader)
                    .filter(leafReader -> leafReader.getLiveDocs() != null)
                    .count();
                debugTestCase(
                    builder,
                    new MatchAllDocsQuery(),
                    searcher,
                    (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                        assertThat(filters.getBuckets(), hasSize(1));
                        assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(5L));
                        assertMap(
                            debug,
                            matchesMap().entry(
                                "test",
                                matchesMap().entry("segments_counted", greaterThanOrEqualTo(1))
                                    .entry("segments_collected", 0)
                                    .entry("segments_with_doc_count_field", 0)
                                    .entry("segments_with_deleted_docs", segmentsWithLiveDocs)
                                    .entry(
                                        "filters",
                                        matchesList().item(
                                            matchesMap().entry("query", "foo:bar")
                                                .entry("segments_counted_in_constant_time", lessThan(searcher.getLeafContexts().size()))
                                        )
                                    )
                            )
                        );
                    },
                    ft
                );
            }
        }
    }

    /**
     * This runs {@code filters} with a single {@code term} filter with
     * the index set up kind of like document level security where the
     * document level security query matches all documents. These can
     * always take the fast path in filter-by-filter.
     */
    public void testTermOnFilterWithMatchAll() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("foo");
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new TermQueryBuilder("foo", "bar")));
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            for (int i = 0; i < 10; i++) {
                indexWriter.addDocument(
                    List.of(new Field("foo", new BytesRef("bar"), KeywordFieldMapper.Defaults.FIELD_TYPE), new LongPoint("t", i))
                );
            }
            indexWriter.close();

            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                final IndexSettings indexSettings = createIndexSettings();
                BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
                    @Override
                    public void onRemoval(ShardId shardId, Accountable accountable) {}

                    @Override
                    public void onCache(ShardId shardId, Accountable accountable) {}
                });
                DirectoryReader limitedReader = new DocumentSubsetDirectoryReader(
                    ElasticsearchDirectoryReader.wrap(directoryReader, new ShardId(indexSettings.getIndex(), 0)),
                    bitsetFilterCache,
                    LongPoint.newRangeQuery("t", Long.MIN_VALUE, Long.MAX_VALUE)
                );
                IndexSearcher searcher = newIndexSearcher(limitedReader);
                int segmentsWithLiveDocs = (int) searcher.getIndexReader()
                    .leaves()
                    .stream()
                    .map(LeafReaderContext::reader)
                    .filter(leafReader -> leafReader.getLiveDocs() != null)
                    .count();

                debugTestCase(
                    builder,
                    new MatchAllDocsQuery(),
                    searcher,
                    (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                        assertThat(filters.getBuckets(), hasSize(1));
                        assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(10L));
                        assertMap(
                            debug,
                            matchesMap().entry(
                                "test",
                                matchesMap().entry("segments_counted", greaterThanOrEqualTo(1))
                                    .entry("segments_collected", 0)
                                    .entry("segments_with_doc_count_field", 0)
                                    .entry("segments_with_deleted_docs", segmentsWithLiveDocs)
                                    .entry(
                                        "filters",
                                        matchesList().item(
                                            matchesMap().entry("query", "foo:bar")
                                                .entry("segments_counted_in_constant_time", searcher.getLeafContexts().size())
                                        )
                                    )
                            )
                        );
                    },
                    ft
                );

            }
        }
    }

    public void testComplexUnionDisabledFilterByFilter() throws IOException {
        MappedFieldType dft = new DateFieldMapper.DateFieldType(
            "date",
            true,
            false,
            false,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            Resolution.MILLISECONDS,
            null,
            null,
            Collections.emptyMap()
        );
        MappedFieldType kft = new KeywordFieldType("kwd");
        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("date").from("2020-01-01").to("2020-03-01").includeUpper(false))
        );
        debugTestCase(builder, new TermQuery(new Term("kwd", "a")), iw -> {
            iw.addDocument(
                List.of(
                    new LongPoint("date", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2010-01-02")),
                    new Field("kwd", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE)
                )
            );
            iw.addDocument(
                List.of(
                    new LongPoint("date", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-02")),
                    new Field("kwd", new BytesRef("a"), KeywordFieldMapper.Defaults.FIELD_TYPE)
                )
            );
        }, (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
            assertThat(filters.getBuckets(), hasSize(1));
            assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(1L));

            assertThat(impl, equalTo(FiltersAggregator.Compatible.class));
            assertMap(
                debug,
                matchesMap().entry(
                    "test",
                    matchesMap().entry(
                        "filters",
                        matchesList().item(
                            matchesMap().entry("query", "date:[1577836800000 TO 1583020799999]")
                                .entry("segments_counted_in_constant_time", 0)
                        )
                    )
                )
            );
        }, dft, kft);
    }

    public void testMatchNoneFilter() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new RangeQueryBuilder("missing").gte(0)));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of(new LongPoint("t", i)));
            }
        };
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(0L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry("query", "MatchNoDocsQuery(\"User requested \"match_none\" query.\")")
                                        .entry("segments_counted_in_constant_time", greaterThan(0))
                                )
                            )
                    )
                );
            }
        );
    }

    public void testMatchNoneTopLevel() throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new RangeQueryBuilder("t").gte(0)));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of(new LongPoint("t", i)));
            }
        };
        debugTestCase(
            builder,
            new MatchNoDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(0L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry("query", "MatchNoDocsQuery(\"User requested \"match_none\" query.\")")
                                        .entry("segments_counted_in_constant_time", greaterThan(0))
                                )
                            )
                    )
                );
            }
        );
    }

    public void testTermFilter() throws IOException {
        KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType("f", true, false, Collections.emptyMap());
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchQueryBuilder("f", "0")));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                BytesRef bytes = new BytesRef(Integer.toString(i % 3));
                iw.addDocument(List.of(new Field("f", bytes, KeywordFieldMapper.Defaults.FIELD_TYPE)));
            }
        };
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(4L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry("query", "f:0").entry("segments_counted_in_constant_time", greaterThan(0))
                                )
                            )
                    )
                );
            },
            ft
        );
    }

    public void testTermTopLevel() throws IOException {
        KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType("f", true, false, Collections.emptyMap());
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", new MatchAllQueryBuilder()));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                BytesRef bytes = new BytesRef(Integer.toString(i % 3));
                iw.addDocument(List.of(new Field("f", bytes, KeywordFieldMapper.Defaults.FIELD_TYPE)));
            }
        };
        debugTestCase(
            builder,
            new TermQuery(new Term("f", "0")),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(4L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry("query", "f:0").entry("segments_counted_in_constant_time", greaterThan(0))
                                )
                            )
                    )
                );
            },
            ft
        );
    }

    public void testBoolThenDateTopLevel() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType("test");
        FieldNamesFieldMapper.FieldNamesFieldType fnft = FieldNamesFieldMapper.FieldNamesFieldType.get(false);

        String start = "2010-01-02T00:00:00.000Z";
        String middle = "2010-01-02T00:00:05.000Z";
        String mostly = "2010-01-02T00:00:09.000Z";
        String end = "2010-01-02T00:00:10.000Z";
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start);
            long endDate = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(end);
            while (date < endDate) {
                iw.addDocument(List.of(new LongPoint("test", date), new SortedNumericDocValuesField("test", date)));
                date += 100;
            }
        };

        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from(start).to(middle))
        );
        debugTestCase(
            builder,
            new BooleanQuery.Builder().add(
                LongPoint.newRangeQuery(
                    "test",
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start),
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(mostly)
                ),
                Occur.FILTER
            ).build(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(51L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry(
                                        "query",
                                        "MergedPointRange[+test:[1262390400000 TO 1262390405000] +test:[1262390400000 TO 1262390409000]]"
                                    ).entry("segments_counted_in_constant_time", greaterThanOrEqualTo(1))
                                )
                            )
                    )
                );
            },
            ft,
            fnft
        );
    }

    public void testBoolThenDateFilter() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType("test");
        FieldNamesFieldMapper.FieldNamesFieldType fnft = FieldNamesFieldMapper.FieldNamesFieldType.get(false);

        String start = "2010-01-02T00:00:00.000Z";
        String middle = "2010-01-02T00:00:05.000Z";
        String mostly = "2010-01-02T00:00:09.000Z";
        String end = "2010-01-02T00:00:10.000Z";
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start);
            long endDate = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(end);
            while (date < endDate) {
                iw.addDocument(List.of(new LongPoint("test", date), new SortedNumericDocValuesField("test", date)));
                date += 100;
            }
        };

        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new BoolQueryBuilder().filter(new RangeQueryBuilder("test").from(start).to(middle)))
        );
        debugTestCase(
            builder,
            LongPoint.newRangeQuery(
                "test",
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start),
                DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(mostly)
            ),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(51L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry(
                                        "query",
                                        "MergedPointRange[+test:[1262390400000 TO 1262390405000] +test:[1262390400000 TO 1262390409000]]"
                                    ).entry("segments_counted_in_constant_time", greaterThanOrEqualTo(1))
                                )
                            )
                    )
                );
            },
            ft,
            fnft
        );
    }

    public void testBoolWithMatchAllThenDateFilter() throws IOException {
        MappedFieldType ft = new DateFieldMapper.DateFieldType("test");
        FieldNamesFieldMapper.FieldNamesFieldType fnft = FieldNamesFieldMapper.FieldNamesFieldType.get(false);

        String start = "2010-01-02T00:00:00.000Z";
        String middle = "2010-01-02T00:00:05.000Z";
        String mostly = "2010-01-02T00:00:09.000Z";
        String end = "2010-01-02T00:00:10.000Z";
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            long date = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start);
            long endDate = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(end);
            while (date < endDate) {
                iw.addDocument(List.of(new LongPoint("test", date), new SortedNumericDocValuesField("test", date)));
                date += 100;
            }
        };

        AggregationBuilder builder = new FiltersAggregationBuilder(
            "test",
            new KeyedFilter("q1", new RangeQueryBuilder("test").from(start).to(middle))
        );
        debugTestCase(
            builder,
            new BooleanQuery.Builder().add(
                LongPoint.newRangeQuery(
                    "test",
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(start),
                    DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis(mostly)
                ),
                Occur.FILTER
            ).add(new MatchAllDocsQuery(), Occur.FILTER).build(),
            buildIndex,
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(1));
                assertThat(filters.getBucketByKey("q1").getDocCount(), equalTo(51L));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", 0)
                            .entry("segments_counted", greaterThanOrEqualTo(1))
                            .entry(
                                "filters",
                                matchesList().item(
                                    matchesMap().entry(
                                        "query",
                                        "MergedPointRange[+test:[1262390400000 TO 1262390405000] +test:[1262390400000 TO 1262390409000]]"
                                    ).entry("segments_counted_in_constant_time", greaterThanOrEqualTo(1))
                                )
                            )
                    )
                );
            },
            ft,
            fnft
        );
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
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            iw -> iw.addDocuments(docs),
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(2));

                InternalFilters.InternalBucket b = filters.getBucketByKey("q1");
                assertThat(b.getDocCount(), equalTo(1L));
                Max max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(100.0));
                Sum sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(100.0));

                b = filters.getBucketByKey("q2");
                assertThat(b.getDocCount(), equalTo(2L));
                max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(10.0));
                sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(15.0));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                ListMatcher filtersMatcher = matchesList().item(
                    matchesMap().entry("query", "test:[1262304000000 TO 1267401599999]").entry("segments_counted_in_constant_time", 0)
                ).item(matchesMap().entry("query", "test:[1577836800000 TO 1583020799999]").entry("segments_counted_in_constant_time", 0));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", greaterThanOrEqualTo(1))
                            .entry("segments_counted", 0)
                            .entry("filters", filtersMatcher)
                    ).entry("test.s", matchesMap()).entry("test.m", matchesMap())
                );
            },
            dateFt,
            intFt
        );
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
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-03"), };
        for (int i = 0; i < 10000; i++) {
            docs.add(List.of(new LongPoint("test", times[i % 3]), new SortedNumericDocValuesField("int", i)));
        }
        /*
         * Shuffle the docs so we collect them in a random order which causes
         * bad implementations of filter-by-filter aggregation to fail with
         * assertion errors while executing.
         */
        Collections.shuffle(docs, random());
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            iw -> iw.addDocuments(docs),
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(2));

                InternalFilters.InternalBucket b = filters.getBucketByKey("q1");
                assertThat(b.getDocCount(), equalTo(3334L));
                Max max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(9999.0));
                Sum sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(16668333.0));

                b = filters.getBucketByKey("q2");
                assertThat(b.getDocCount(), equalTo(6666L));
                max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(9998.0));
                sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(33326667.0));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", greaterThanOrEqualTo(1))
                            .entry("segments_counted", 0)
                            .entry("filters", hasSize(2))
                    ).entry("test.s", matchesMap()).entry("test.m", matchesMap())
                );
            },
            dateFt,
            intFt
        );
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
        AggregationBuilder builder = new FiltersAggregationBuilder("test", buckets.toArray(KeyedFilter[]::new)).subAggregation(
            new MaxAggregationBuilder("m").field("int")
        ).subAggregation(new SumAggregationBuilder("s").field("int"));
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
        debugTestCase(
            builder,
            new MatchAllDocsQuery(),
            iw -> iw.addDocuments(docs),
            (InternalFilters filters, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                assertThat(filters.getBuckets(), hasSize(buckets.size()));
                InternalFilters.InternalBucket b = filters.getBucketByKey("2010-01-01 to 2010-01-31");
                assertThat(b.getDocCount(), equalTo(3334L));
                Max max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(9999.0));
                Sum sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(16668333.0));

                b = filters.getBucketByKey("2019-12-10 to 2020-01-09");
                assertThat(b.getDocCount(), equalTo(6666L));
                max = b.getAggregations().get("m");
                assertThat(max.value(), equalTo(9998.0));
                sum = b.getAggregations().get("s");
                assertThat(sum.value(), equalTo(33326667.0));

                assertThat(impl, equalTo(FilterByFilterAggregator.class));
                assertMap(
                    debug,
                    matchesMap().entry(
                        "test",
                        matchesMap().entry("segments_with_doc_count_field", 0)
                            .entry("segments_with_deleted_docs", 0)
                            .entry("segments_collected", greaterThanOrEqualTo(1))
                            .entry("segments_counted", 0)
                            .entry("filters", hasSize(buckets.size()))
                    ).entry("test.s", matchesMap()).entry("test.m", matchesMap())
                );
            },
            dateFt,
            intFt
        );
    }

    public void testDocValuesFieldExistsForDate() throws IOException {
        DateFieldMapper.DateFieldType ft = new DateFieldMapper.DateFieldType("f");
        QueryBuilder exists;
        if (randomBoolean()) {
            exists = new ExistsQueryBuilder("f");
        } else {
            // Range query covering all values in the index is rewritten to exists
            exists = new RangeQueryBuilder("f").gte("2020-01-01").lt("2020-01-02");
        }
        long start = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01T00:00:01");
        docValuesFieldExistsTestCase(exists, ft, true, i -> {
            long date = start + TimeUnit.HOURS.toMillis(i);
            return List.of(new LongPoint("f", date), new NumericDocValuesField("f", date));
        });
    }

    public void testDocValuesFieldExistsForDateWithMultiValuedFields() throws IOException {
        DateFieldMapper.DateFieldType ft = new DateFieldMapper.DateFieldType("f");
        long start = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2020-01-01T00:00:01");
        docValuesFieldExistsTestCase(new ExistsQueryBuilder("f"), ft, true, i -> {
            long date = start + TimeUnit.HOURS.toMillis(i);
            return List.of(
                new LongPoint("f", date),
                new LongPoint("f", date + 10),
                new SortedNumericDocValuesField("f", date),
                new SortedNumericDocValuesField("f", date + 10)
            );
        });
    }

    public void testDocValuesFieldExistsForDateWithoutData() throws IOException {
        docValuesFieldExistsNoDataTestCase(new DateFieldMapper.DateFieldType("f"));
    }

    public void testDocValuesFieldExistsForNumber() throws IOException {
        NumberFieldMapper.NumberType numberType = randomFrom(NumberFieldMapper.NumberType.values());
        NumberFieldMapper.NumberFieldType ft = new NumberFieldMapper.NumberFieldType(
            "f",
            numberType,
            true,
            false,
            true,
            true,
            null,
            Map.of(),
            null,
            false,
            null,
            null
        );
        docValuesFieldExistsTestCase(new ExistsQueryBuilder("f"), ft, true, i -> {
            final LuceneDocument document = new LuceneDocument();
            numberType.addFields(document, "f", i, true, true, false);
            return document;
        });
    }

    public void testDocValuesFieldExistsForNumberWithoutData() throws IOException {
        docValuesFieldExistsNoDataTestCase(
            new NumberFieldMapper.NumberFieldType(
                "f",
                randomFrom(NumberFieldMapper.NumberType.values()),
                true,
                false,
                true,
                true,
                null,
                Map.of(),
                null,
                false,
                null,
                null
            )
        );
    }

    public void testDocValuesFieldExistsForKeyword() throws IOException {
        KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType("f", true, true, Map.of());
        docValuesFieldExistsTestCase(new ExistsQueryBuilder("f"), ft, true, i -> {
            BytesRef text = new BytesRef(randomAlphaOfLength(5));
            return List.of(new Field("f", text, KeywordFieldMapper.Defaults.FIELD_TYPE));
        });
    }

    public void testDocValuesFieldExistsForKeywordWithoutData() throws IOException {
        docValuesFieldExistsNoDataTestCase(new KeywordFieldMapper.KeywordFieldType("f", true, true, Map.of()));
    }

    private void docValuesFieldExistsTestCase(
        QueryBuilder exists,
        MappedFieldType fieldType,
        boolean countsResultsInConstantTime,
        IntFunction<Iterable<? extends IndexableField>> buildDocWithField
    ) throws IOException {
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", exists));
        // Exists queries convert to MatchNone if this isn't defined
        FieldNamesFieldMapper.FieldNamesFieldType fnft = FieldNamesFieldMapper.FieldNamesFieldType.get(true);
        debugTestCase(builder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocuments(
                    List.of(
                        buildDocWithField.apply(i),
                        // Create a document without the field to prevent DocValueFieldExists from being rewritten to MatchAll
                        List.of()
                    )
                );
            }
        }, (InternalFilters result, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
            assertThat(result.getBuckets(), hasSize(1));
            assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(10L));

            assertThat(impl, equalTo(FilterByFilterAggregator.class));
            MapMatcher expectedFilterDebug = matchesMap().extraOk()
                .entry("segments_counted_in_constant_time", countsResultsInConstantTime ? greaterThan(0) : equalTo(0));
            assertMap(debug, matchesMap().entry("test", matchesMap().extraOk().entry("filters", matchesList().item(expectedFilterDebug))));
        }, fieldType, fnft);
    }

    private void docValuesFieldExistsNoDataTestCase(MappedFieldType fieldType) throws IOException {
        QueryBuilder exists = new ExistsQueryBuilder(fieldType.name());
        AggregationBuilder builder = new FiltersAggregationBuilder("test", new KeyedFilter("q1", exists));
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(List.of());
            }
        };
        // Exists queries convert to MatchNone if this isn't defined
        FieldNamesFieldMapper.FieldNamesFieldType fnft = FieldNamesFieldMapper.FieldNamesFieldType.get(true);
        withAggregator(builder, new MatchAllDocsQuery(), buildIndex, (searcher, aggregator) -> {
            assertThat(aggregator, instanceOf(FilterByFilterAggregator.class));

            Map<String, Object> debug = collectAndGetFilterDebugInfo(searcher, aggregator);
            assertMap(debug, matchesMap().extraOk().entry("segments_counted_in_constant_time", greaterThan(0)));
        }, fieldType, fnft);
        testCase(buildIndex, (InternalFilters result) -> {
            assertThat(result.getBuckets(), hasSize(1));
            assertThat(result.getBucketByKey("q1").getDocCount(), equalTo(0L));
        }, new AggTestConfig(builder, fieldType, fnft));
    }

    @Override
    protected List<ObjectMapper> objectMappers() {
        return MOCK_OBJECT_MAPPERS;
    }

    private Map<String, Object> collectAndGetFilterDebugInfo(IndexSearcher searcher, Aggregator aggregator) throws IOException {
        aggregator.preCollection();
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            LeafBucketCollector leafCollector = aggregator.getLeafCollector(new AggregationExecutionContext(ctx, null, null, null));
            assertTrue(leafCollector.isNoop());
        }
        Map<String, Object> debug = new HashMap<>();
        ((FilterByFilterAggregator) aggregator).filters().get(0).collectDebugInfo(debug::put);
        return debug;
    }

    static final List<ObjectMapper> MOCK_OBJECT_MAPPERS = List.of(NestedAggregatorTests.nestedObject("nested_chapters"));
}

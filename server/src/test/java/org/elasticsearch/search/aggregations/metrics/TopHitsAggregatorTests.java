/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregatorTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class TopHitsAggregatorTests extends AggregatorTestCase {
    public void testTopLevel() throws Exception {
        Aggregation result;
        if (randomBoolean()) {
            result = testCase(Queries.ALL_DOCS_INSTANCE, topHits("_name").sort("string", SortOrder.DESC));
        } else {
            Query query = new QueryParser("string", new KeywordAnalyzer()).parse("d^1000 c^100 b^10 a^1");
            result = testCase(query, topHits("_name"));
        }
        SearchHits searchHits = ((TopHits) result).getHits();
        assertEquals(3L, searchHits.getTotalHits().value());
        assertEquals("3", searchHits.getAt(0).getId());
        assertEquals("2", searchHits.getAt(1).getId());
        assertEquals("1", searchHits.getAt(2).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) result)));
    }

    public void testNoResults() throws Exception {
        TopHits result = (TopHits) testCase(Queries.NO_DOCS_INSTANCE, topHits("_name").sort("string", SortOrder.DESC));
        SearchHits searchHits = result.getHits();
        assertEquals(0L, searchHits.getTotalHits().value());
        assertFalse(AggregationInspectionHelper.hasValue(((InternalTopHits) result)));
    }

    /**
     * Tests {@code top_hits} inside of {@code terms}. While not strictly a unit test this is a fairly common way to run {@code top_hits}
     * and serves as a good example of running {@code top_hits} inside of another aggregation.
     */
    public void testInsideTerms() throws Exception {
        Aggregation result;
        if (randomBoolean()) {
            result = testCase(
                Queries.ALL_DOCS_INSTANCE,
                terms("term").field("string").subAggregation(topHits("top").sort("string", SortOrder.DESC))
            );
        } else {
            Query query = new QueryParser("string", new KeywordAnalyzer()).parse("d^1000 c^100 b^10 a^1");
            result = testCase(query, terms("term").field("string").subAggregation(topHits("top")));
        }
        Terms terms = (Terms) result;

        // The "a" bucket
        TopHits hits = (TopHits) terms.getBucketByKey("a").getAggregations().get("top");
        SearchHits searchHits = (hits).getHits();
        assertEquals(2L, searchHits.getTotalHits().value());
        assertEquals("2", searchHits.getAt(0).getId());
        assertEquals("1", searchHits.getAt(1).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("a").getAggregations().get("top"))));

        // The "b" bucket
        searchHits = ((TopHits) terms.getBucketByKey("b").getAggregations().get("top")).getHits();
        assertEquals(2L, searchHits.getTotalHits().value());
        assertEquals("3", searchHits.getAt(0).getId());
        assertEquals("1", searchHits.getAt(1).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("b").getAggregations().get("top"))));

        // The "c" bucket
        searchHits = ((TopHits) terms.getBucketByKey("c").getAggregations().get("top")).getHits();
        assertEquals(1L, searchHits.getTotalHits().value());
        assertEquals("2", searchHits.getAt(0).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("c").getAggregations().get("top"))));

        // The "d" bucket
        searchHits = ((TopHits) terms.getBucketByKey("d").getAggregations().get("top")).getHits();
        assertEquals(1L, searchHits.getTotalHits().value());
        assertEquals("3", searchHits.getAt(0).getId());
        assertTrue(AggregationInspectionHelper.hasValue(((InternalTopHits) terms.getBucketByKey("d").getAggregations().get("top"))));
    }

    /**
     * Every bucket fetches through one shared {@link org.elasticsearch.index.query.SearchExecutionContext}, whose field
     * lookup provider holds the fields of the last hit it loaded. Each bucket therefore asserts a value of its own and
     * not just a hit count: the id, which the fetch phase reads through that stored field lookup, and a doc value
     * fetched through the shared context. A value carried over from the previous bucket fails one of the two.
     */
    public void testSharedFetchContextKeepsBucketsIsolated() throws Exception {
        int buckets = 5;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < buckets; i++) {
                    iw.addDocument(document(Integer.toString(i), "term" + i));
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                Terms terms = searchAndReduce(
                    indexReader,
                    new AggTestConfig(
                        terms("term").field("string").size(buckets).subAggregation(topHits("top").docValueField("string")),
                        STRING_FIELD_TYPE
                    )
                        // Force a single aggregator, so one forked fetch context serves every bucket. Otherwise, a seed can
                        // give each segment its own aggregator with one bucket each, and the test passes proving nothing.
                        .withSplitLeavesIntoSeperateAggregators(false)
                );
                assertEquals(buckets, terms.getBuckets().size());
                for (int i = 0; i < buckets; i++) {
                    assertOnlyHitIs(i, ((TopHits) terms.getBucketByKey("term" + i).getAggregations().get("top")).getHits());
                }
            }
        }
    }

    /**
     * The same guarantee as {@link #testSharedFetchContextKeepsBucketsIsolated}, with the buckets spread over separate
     * segments so the shared context is reused across leaf readers too.
     */
    public void testSharedFetchContextSpansSegments() throws Exception {
        int buckets = 5;
        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < buckets; i++) {
                    iw.addDocument(document(Integer.toString(i), "term" + i));
                    iw.commit();
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                assertThat(indexReader.leaves().size(), greaterThan(1));
                // debugTestCase always builds a single aggregator over every segment, where searchAndReduce may
                // randomly build one per leaf, so the forked fetch context is reused across segments here.
                debugTestCase(
                    terms("term").field("string").size(buckets).subAggregation(topHits("top").docValueField("string")),
                    Queries.ALL_DOCS_INSTANCE,
                    indexReader,
                    (StringTerms result, Class<? extends Aggregator> impl, Map<String, Map<String, Object>> debug) -> {
                        assertEquals(buckets, result.getBuckets().size());
                        for (int i = 0; i < buckets; i++) {
                            assertOnlyHitIs(i, ((TopHits) result.getBucketByKey("term" + i).getAggregations().get("top")).getHits());
                        }
                    },
                    null,
                    STRING_FIELD_TYPE
                );
            }
        }
    }

    private static void assertOnlyHitIs(int id, SearchHits hits) {
        assertEquals(1L, hits.getTotalHits().value());
        assertEquals(Integer.toString(id), hits.getAt(0).getId());
        assertEquals("term" + id, hits.getAt(0).field("string").getValue());
    }

    // top_hits with a single numeric sort is collected with a BucketedSort. Everything else uses Lucene collectors.

    private static final String NUMBER_FIELD = "number";

    private static final List<NumberFieldMapper.NumberType> NUMERIC_TYPES = List.of(
        NumberFieldMapper.NumberType.BYTE,
        NumberFieldMapper.NumberType.SHORT,
        NumberFieldMapper.NumberType.INTEGER,
        NumberFieldMapper.NumberType.LONG,
        NumberFieldMapper.NumberType.HALF_FLOAT,
        NumberFieldMapper.NumberType.FLOAT,
        NumberFieldMapper.NumberType.DOUBLE
    );

    /**
     * Indexes with {@code indexer} and hands a reader of the result to {@code test}. Segments are never merged, so doc ids follow
     * the order the documents were added, and a test that wants several segments commits in between.
     */
    private void withUnmergedIndex(CheckedConsumer<IndexWriter, IOException> indexer, CheckedConsumer<IndexReader, IOException> test)
        throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                indexer.accept(iw);
            }
            try (DirectoryReader reader = wrapDirectoryReader(DirectoryReader.open(directory))) {
                test.accept(reader);
            }
        }
    }

    private static MappedFieldType numberFieldType(NumberFieldMapper.NumberType type) {
        return new NumberFieldMapper.NumberFieldType(NUMBER_FIELD, type);
    }

    private static Number numberValue(NumberFieldMapper.NumberType type, long value) {
        return switch (type) {
            case BYTE -> (byte) value;
            case SHORT -> (short) value;
            case INTEGER -> (int) value;
            case LONG -> value;
            case HALF_FLOAT, FLOAT -> (float) value;
            case DOUBLE -> (double) value;
            default -> throw new AssertionError("unexpected number type [" + type + "]");
        };
    }

    private static LuceneDocument numberDocument(String id, String term, NumberFieldMapper.NumberType type, @Nullable Number number) {
        return numberDocument(id, term, type, number == null ? List.of() : List.of(number));
    }

    /**
     * A document with any number of values for the number field, including none.
     */
    private static LuceneDocument numberDocument(String id, String term, NumberFieldMapper.NumberType type, List<Number> numbers) {
        LuceneDocument document = new LuceneDocument();
        document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Store.YES));
        document.add(new Field("string", new BytesRef(term), KeywordFieldMapper.Defaults.FIELD_TYPE));
        for (Number number : numbers) {
            type.addFields(document, NUMBER_FIELD, number, IndexType.points(false, true), false);
        }
        return document;
    }

    /**
     * What the result of a bucket says: the exact total, and for each hit its id, its raw sort value and its score, and the
     * highest score.
     */
    private record HitsSummary(long total, List<String> ids, List<Object> sortValues, List<Float> scores, float maxScore) {}

    private static Query boostedTermsQuery(int termCount) {
        BooleanQuery.Builder query = new BooleanQuery.Builder();
        for (int t = 0; t < termCount; t++) {
            query.add(new BoostQuery(new TermQuery(new Term("string", "t" + t)), t + 1), Occur.SHOULD);
        }
        return query.build();
    }

    private Terms termsWithTopHits(
        IndexReader reader,
        NumberFieldMapper.NumberType type,
        TopHitsAggregationBuilder topHits,
        int termCount,
        boolean splitLeaves,
        boolean expectBucketedSort
    ) throws IOException {
        return searchAndReduce(
            reader,
            new AggTestConfig(
                terms("term").field("string").size(termCount).subAggregation(topHits),
                STRING_FIELD_TYPE,
                numberFieldType(type)
            ).withQuery(boostedTermsQuery(termCount))
                .withSplitLeavesIntoSeperateAggregators(splitLeaves)
                // Only run when the leaves are split, and then once for every aggregator that is built.
                .withCheckAggregator(aggregator -> assertUsesBucketedSort(aggregator, expectBucketedSort))
        );
    }

    private static HitsSummary summarize(SearchHit[] hits, TotalHits totalHits, float maxScore) {
        assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation());
        List<String> ids = new ArrayList<>();
        List<Object> sortValues = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
            sortValues.add(hit.getRawSortValues()[0]);
            scores.add(hit.getScore());
        }
        return new HitsSummary(totalHits.value(), ids, sortValues, scores, maxScore);
    }

    /**
     * The result after the reduce, where the hits carry the total and the highest score.
     */
    private static HitsSummary summarize(SearchHits hits) {
        return summarize(hits.getHits(), hits.getTotalHits(), hits.getMaxScore());
    }

    /**
     * The result of one aggregator before the reduce, where the total and the highest score are in its top docs, and the hits
     * are all of the {@code from + size} the aggregator keeps.
     */
    private static HitsSummary summarize(InternalTopHits partial) {
        TopDocsAndMaxScore topDocs = partial.getTopDocs();
        return summarize(partial.getHits().getHits(), topDocs.topDocs.totalHits, topDocs.maxScore);
    }

    /**
     * The summary of every bucket after the reduce, over separate aggregators for the segments, the order of which is not
     * deterministic, so the sort values have to be unique for the results of two runs to be comparable.
     */
    private Map<String, HitsSummary> summarizeReducedBuckets(
        IndexReader reader,
        NumberFieldMapper.NumberType type,
        TopHitsAggregationBuilder topHits,
        int termCount,
        boolean expectBucketedSort
    ) throws IOException {
        Map<String, HitsSummary> summaries = new HashMap<>();
        for (Terms.Bucket bucket : termsWithTopHits(reader, type, topHits, termCount, true, expectBucketedSort).getBuckets()) {
            summaries.put(bucket.getKeyAsString(), summarize(((TopHits) bucket.getAggregations().get("top")).getHits()));
        }
        return summaries;
    }

    /**
     * The summary of every bucket of what one aggregator returns before any reduce. One sequential search over every segment
     * feeds one aggregator, so ties are broken by doc id and nothing else.
     */
    private Map<String, HitsSummary> summarizeBuckets(
        IndexReader reader,
        Query query,
        MappedFieldType numberFieldType,
        TopHitsAggregationBuilder topHits,
        int termCount,
        Aggregator.SubAggCollectionMode collectMode,
        boolean expectBucketedSort
    ) throws IOException {
        Map<String, HitsSummary> summaries = new HashMap<>();
        collectAndBuild(
            reader,
            query,
            terms("term").field("string").size(termCount).collectMode(collectMode).subAggregation(topHits),
            expectBucketedSort,
            built -> {
                for (Terms.Bucket bucket : ((Terms) built).getBuckets()) {
                    summaries.put(bucket.getKeyAsString(), summarize((InternalTopHits) bucket.getAggregations().get("top")));
                }
            },
            STRING_FIELD_TYPE,
            numberFieldType
        );
        return summaries;
    }

    /**
     * Runs the same {@code top_hits} twice, as it is and with a second sort on {@code _doc}, which cannot use the
     * {@code BucketedSort}, and asserts the two return the same: the old way of collecting is what the new one has to match, and
     * its tie-break by doc id is the one the {@code BucketedSort} reproduces. Returns what the {@code BucketedSort} returned.
     */
    private Map<String, HitsSummary> assertTheSameAsTheLuceneCollectors(
        IndexReader reader,
        Query query,
        MappedFieldType numberFieldType,
        FieldSortBuilder sort,
        int termCount,
        int from,
        int size,
        boolean trackScores,
        Aggregator.SubAggCollectionMode collectMode
    ) throws IOException {
        Map<String, HitsSummary> bucketed = summarizeBuckets(
            reader,
            query,
            numberFieldType,
            topHits("top").sort(sort).from(from).size(size).trackScores(trackScores),
            termCount,
            collectMode,
            true
        );
        Map<String, HitsSummary> fallback = summarizeBuckets(
            reader,
            query,
            numberFieldType,
            topHits("top").sort(sort).sort("_doc", SortOrder.ASC).from(from).size(size).trackScores(trackScores),
            termCount,
            collectMode,
            false
        );
        assertThat(bucketed.size(), greaterThan(0));
        assertEquals(fallback, bucketed);
        return bucketed;
    }

    /**
     * The same as the Lucene collectors, for every numeric type, with heavy ties, both orders, documents with no value, several
     * values per document with each sort mode, the {@code missing} option, {@code numeric_type}, {@code from}, {@code track_scores},
     * several segments, and either way of collecting the {@code terms}.
     */
    public void testBucketedSortMatchesTheLuceneCollectors() throws Exception {
        NumberFieldMapper.NumberType type = randomFrom(NUMERIC_TYPES);
        boolean multiValued = randomBoolean();
        FieldSortBuilder sort = SortBuilders.fieldSort(NUMBER_FIELD).order(randomFrom(SortOrder.values()));
        if (multiValued) {
            sort.sortMode(randomFrom(SortMode.values()));
        }
        if (randomBoolean()) {
            sort.missing(randomFrom("_first", "_last", 3));
        }
        if (randomBoolean()) {
            sort.setNumericType(randomFrom("long", "double"));
        }
        int termCount = between(1, 4);
        withUnmergedIndex(iw -> {
            int docs = between(1, 150);
            for (int i = 0; i < docs; i++) {
                List<Number> numbers = new ArrayList<>();
                // About one document in six has no value at all.
                int count = rarely() ? 0 : multiValued ? between(1, 3) : 1;
                for (int n = 0; n < count; n++) {
                    numbers.add(numberValue(type, between(0, 5)));
                }
                iw.addDocument(numberDocument(Integer.toString(i), "t" + between(0, termCount - 1), type, numbers));
                if (rarely()) {
                    iw.commit();
                }
            }
        },
            reader -> assertTheSameAsTheLuceneCollectors(
                reader,
                boostedTermsQuery(termCount),
                numberFieldType(type),
                sort,
                termCount,
                between(0, 2),
                between(1, 4),
                randomBoolean(),
                randomFrom(Aggregator.SubAggCollectionMode.values())
            )
        );
    }

    /**
     * One bucket holding the given values, in doc id order, sorted with the {@code BucketedSort} and the Lucene collectors, which
     * have to agree. Returns what the bucket kept.
     */
    private HitsSummary assertSameForValues(NumberFieldMapper.NumberType type, SortOrder order, int size, Number... values)
        throws IOException {
        HitsSummary[] kept = new HitsSummary[1];
        withUnmergedIndex(iw -> {
            for (int i = 0; i < values.length; i++) {
                iw.addDocument(numberDocument(Integer.toString(i), "t0", type, values[i]));
            }
        },
            reader -> kept[0] = assertTheSameAsTheLuceneCollectors(
                reader,
                boostedTermsQuery(1),
                numberFieldType(type),
                SortBuilders.fieldSort(NUMBER_FIELD).order(order),
                1,
                0,
                size,
                false,
                Aggregator.SubAggCollectionMode.DEPTH_FIRST
            ).get("t0")
        );
        return kept[0];
    }

    /**
     * Among equal sort values the lower doc id is kept, as the Lucene collectors do, and the heap cannot recover from evicting the
     * wrong tied document. Values 6, 5, 5, 4, 3 in doc id order, keeping the best three ascending, have to keep doc 1 and not
     * doc 2, and the mirror image descending. Random data reaches that pattern too rarely to be relied on, so this one is fixed.
     */
    public void testBucketedSortKeepsTheLowerDocIdOnTies() throws Exception {
        for (SortOrder order : SortOrder.values()) {
            Number[] values = order == SortOrder.ASC ? new Number[] { 6L, 5L, 5L, 4L, 3L } : new Number[] { 3L, 4L, 4L, 5L, 6L };
            HitsSummary kept = assertSameForValues(NumberFieldMapper.NumberType.LONG, order, 3, values);
            assertEquals(order.toString(), List.of("4", "3", "1"), kept.ids());
        }
    }

    /**
     * Separate aggregators for the segments, whose results are merged in the reduce, which compares the raw sort values. The
     * values are unique so that the order of the merge, which is shuffled by the test, cannot change the result.
     */
    public void testBucketedSortMergesAcrossAggregators() throws Exception {
        NumberFieldMapper.NumberType type = randomFrom(NUMERIC_TYPES);
        int termCount = between(1, 4);
        int docs = between(2, 100);
        List<Long> values = new ArrayList<>();
        for (long v = 0; v < docs; v++) {
            values.add(v);
        }
        Collections.shuffle(values, random());
        withUnmergedIndex(iw -> {
            for (int i = 0; i < docs; i++) {
                iw.addDocument(
                    numberDocument(Integer.toString(i), "t" + between(0, termCount - 1), type, numberValue(type, values.get(i)))
                );
                if (i % 10 == 9) {
                    iw.commit();
                }
            }
        }, reader -> {
            FieldSortBuilder sort = SortBuilders.fieldSort(NUMBER_FIELD).order(randomFrom(SortOrder.values()));
            int from = between(0, 3);
            int size = between(1, 5);
            boolean trackScores = randomBoolean();
            Map<String, HitsSummary> bucketed = summarizeReducedBuckets(
                reader,
                type,
                topHits("top").sort(sort).from(from).size(size).trackScores(trackScores),
                termCount,
                true
            );
            Map<String, HitsSummary> fallback = summarizeReducedBuckets(
                reader,
                type,
                topHits("top").sort(sort).sort("_doc", SortOrder.ASC).from(from).size(size).trackScores(trackScores),
                termCount,
                false
            );
            assertThat(bucketed.size(), greaterThan(0));
            assertEquals(fallback, bucketed);
        });
    }

    /**
     * A {@code NaN} sorts after every other value, in both orders, and is reported as {@code NaN}. The {@code BucketedSort}
     * has no number to report for it, so this checks it is turned back into one of the type the Lucene comparator gives.
     */
    public void testBucketedSortReportsNaN() throws Exception {
        for (NumberFieldMapper.NumberType type : List.of(NumberFieldMapper.NumberType.DOUBLE, NumberFieldMapper.NumberType.FLOAT)) {
            Number nan = type == NumberFieldMapper.NumberType.DOUBLE ? (Number) Double.NaN : (Number) Float.NaN;
            for (SortOrder order : SortOrder.values()) {
                HitsSummary kept = assertSameForValues(type, order, 4, nan, numberValue(type, 1), nan, numberValue(type, 2));
                assertThat(type + " " + order, kept.sortValues(), hasItem(nan));
            }
        }
    }

    /**
     * A {@code date_nanos} sort uses the long comparator over the values as nanoseconds, so the raw sort values have to come out
     * as the same {@link Long}s the Lucene collectors give, with the same ties.
     */
    public void testBucketedSortOnDateNanos() throws Exception {
        MappedFieldType dateNanos = new DateFieldMapper.DateFieldType(NUMBER_FIELD, DateFieldMapper.Resolution.NANOSECONDS);
        int termCount = between(1, 4);
        withUnmergedIndex(iw -> {
            int docs = between(1, 100);
            for (int i = 0; i < docs; i++) {
                LuceneDocument document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Store.YES));
                document.add(new Field("string", new BytesRef("t" + between(0, termCount - 1)), KeywordFieldMapper.Defaults.FIELD_TYPE));
                // A few distinct instants, so that there are ties.
                document.add(new SortedNumericDocValuesField(NUMBER_FIELD, 1_700_000_000_000_000_000L + between(0, 5) * 1_000_000_007L));
                iw.addDocument(document);
                if (rarely()) {
                    iw.commit();
                }
            }
        },
            reader -> assertTheSameAsTheLuceneCollectors(
                reader,
                boostedTermsQuery(termCount),
                dateNanos,
                SortBuilders.fieldSort(NUMBER_FIELD).order(randomFrom(SortOrder.values())),
                termCount,
                0,
                between(1, 4),
                false,
                randomFrom(Aggregator.SubAggCollectionMode.values())
            )
        );
    }

    /**
     * Only a single sort on a numeric field is collected with the {@code BucketedSort}, and the decision is made when the
     * aggregator is created.
     */
    public void testWhichSortsUseBucketedSort() throws Exception {
        Map<TopHitsAggregationBuilder, Boolean> expected = new LinkedHashMap<>();
        expected.put(topHits("top").sort(NUMBER_FIELD, SortOrder.ASC), true);
        expected.put(topHits("top").sort(NUMBER_FIELD, SortOrder.DESC).trackScores(true).from(1).size(2), true);
        expected.put(topHits("top"), false);
        expected.put(topHits("top").sort("string", SortOrder.ASC), false);
        expected.put(topHits("top").sort("_doc", SortOrder.ASC), false);
        expected.put(topHits("top").sort(NUMBER_FIELD, SortOrder.ASC).sort("_doc", SortOrder.ASC), false);
        expected.put(topHits("top").sort(SortBuilders.scoreSort()), false);
        withUnmergedIndex(iw -> {
            for (int i = 0; i < 5; i++) {
                iw.addDocument(numberDocument(Integer.toString(i), "t" + i, NumberFieldMapper.NumberType.LONG, i));
            }
        }, reader -> {
            // The failure message of a mismatch names the aggregation, and so the sort.
            for (Map.Entry<TopHitsAggregationBuilder, Boolean> entry : expected.entrySet()) {
                collectAndBuild(
                    reader,
                    Queries.ALL_DOCS_INSTANCE,
                    terms("term").field("string").size(10).subAggregation(entry.getKey()),
                    entry.getValue(),
                    built -> {},
                    STRING_FIELD_TYPE,
                    numberFieldType(NumberFieldMapper.NumberType.LONG)
                );
            }
        });

        // A sort on a field of nested documents is only valid in the nested scope that the nested aggregation enters while it builds
        // its sub-aggregations, so the sort of a top_hits under it has to be built from there. Only the decision is checked, because
        // fetching nested documents would need more of the search context than these tests set up.
        objectMappers = List.of(NestedAggregatorTests.nestedObject(NESTED_PATH));
        withUnmergedIndex(iw -> {
            for (int parent = 0; parent < 3; parent++) {
                List<LuceneDocument> block = new ArrayList<>();
                for (int comment = 0; comment < 2; comment++) {
                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(NestedPathFieldMapper.NAME, NESTED_PATH, Store.NO));
                    document.add(new Field("string", new BytesRef("t" + comment), KeywordFieldMapper.Defaults.FIELD_TYPE));
                    NumberFieldMapper.NumberType.LONG.addFields(
                        document,
                        NESTED_NUMBER_FIELD,
                        (long) parent + comment,
                        IndexType.points(false, true),
                        false
                    );
                    block.add(document);
                }
                LuceneDocument root = new LuceneDocument();
                root.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(parent)), Store.YES));
                root.add(new StringField(NestedPathFieldMapper.NAME, "_doc", Store.NO));
                block.add(root);
                iw.addDocuments(block);
            }
        },
            reader -> collect(
                reader,
                Queries.ALL_DOCS_INSTANCE,
                new NestedAggregationBuilder("nested", NESTED_PATH).subAggregation(
                    terms("term").field("string").size(10).subAggregation(topHits("top").sort(NESTED_NUMBER_FIELD, SortOrder.DESC))
                ),
                requestBreakers("1gb"),
                false,
                true,
                built -> {},
                STRING_FIELD_TYPE,
                new NumberFieldMapper.NumberFieldType(NESTED_NUMBER_FIELD, NumberFieldMapper.NumberType.LONG)
            )
        );
    }

    private static final String NESTED_PATH = "comments";
    private static final String NESTED_NUMBER_FIELD = NESTED_PATH + ".stars";

    /**
     * Left empty except for the test that needs a nested mapping, because the fetch phase of every other test would then expect
     * nested documents in its index.
     */
    private List<ObjectMapper> objectMappers = List.of();

    @Override
    protected List<ObjectMapper> objectMappers() {
        return objectMappers;
    }

    /**
     * Nested aggregations need the {@linkplain DirectoryReader} wrapped.
     */
    @Override
    protected DirectoryReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return wrapInMockESDirectoryReader(reader);
    }

    private static HierarchyCircuitBreakerService requestBreakers(String requestLimit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestLimit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    /**
     * Builds the aggregation with the given breakers and runs one sequential search over every segment into it, which is a single
     * aggregator that sees the documents in doc id order. Hands what is built, or {@code null} if {@code buildResults} is not
     * set, to {@code check} before everything the aggregation holds is released. If {@code expectBucketedSort} is not
     * {@code null}, asserts that the {@code top_hits} it built do, or do not, collect with a {@code BucketedSort}.
     */
    private void collect(
        IndexReader reader,
        Query query,
        AggregationBuilder builder,
        CircuitBreakerService breakers,
        boolean buildResults,
        @Nullable Boolean expectBucketedSort,
        CheckedConsumer<InternalAggregation, IOException> check,
        MappedFieldType... fieldTypes
    ) throws IOException {
        AggregationContext context = createAggregationContext(
            reader,
            createIndexSettings(),
            query,
            breakers,
            0,
            Integer.MAX_VALUE,
            false,
            false,
            fieldTypes
        );
        try {
            Aggregator aggregator = createAggregator(builder, context);
            if (expectBucketedSort != null) {
                assertUsesBucketedSort(aggregator, expectBucketedSort);
            }
            aggregator.preCollection();
            ContextIndexSearcher searcher = (ContextIndexSearcher) context.searcher();
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1f);
            searcher.search(
                searcher.getIndexReader()
                    .leaves()
                    .stream()
                    .map(IndexSearcher.LeafReaderContextPartition::createForEntireSegment)
                    .toArray(IndexSearcher.LeafReaderContextPartition[]::new),
                weight,
                aggregator.asCollector()
            );
            aggregator.postCollection();
            check.accept(buildResults ? aggregator.buildTopLevel() : null);
        } finally {
            Releasables.close(context);
        }
    }

    private void collectAndBuild(
        IndexReader reader,
        Query query,
        AggregationBuilder builder,
        boolean expectBucketedSort,
        CheckedConsumer<InternalAggregation, IOException> check,
        MappedFieldType... fieldTypes
    ) throws IOException {
        collect(reader, query, builder, requestBreakers("1gb"), true, expectBucketedSort, check, fieldTypes);
    }

    /**
     * A test that compares the results of two ways of collecting is only worth anything if it knows which one it ran, because
     * falling back to the Lucene collectors compares them with themselves.
     */
    private static void assertUsesBucketedSort(Aggregator aggregator, boolean expected) {
        List<TopHitsAggregator> topHits = topHitsAggregators(aggregator);
        assertThat("a top_hits was built", topHits, not(empty()));
        for (TopHitsAggregator topHitsAggregator : topHits) {
            assertEquals("collected with a BucketedSort", expected, topHitsAggregator.usesBucketedSort());
        }
    }

    private static List<TopHitsAggregator> topHitsAggregators(Aggregator aggregator) {
        List<TopHitsAggregator> topHits = new ArrayList<>();
        if (aggregator instanceof TopHitsAggregator topHitsAggregator) {
            topHits.add(topHitsAggregator);
        }
        for (Aggregator sub : aggregator.subAggregators()) {
            topHits.addAll(topHitsAggregators(sub));
        }
        return topHits;
    }

    /**
     * Collects every document and returns how many bytes the request breaker holds once collection, and optionally building the
     * results, is done. Everything is released afterwards.
     */
    private long breakerBytesAfterCollecting(
        IndexReader reader,
        AggregationBuilder builder,
        HierarchyCircuitBreakerService breakers,
        boolean buildResults
    ) throws IOException {
        long baseline = breakers.getBreaker(CircuitBreaker.REQUEST).getUsed();
        long[] held = new long[1];
        collect(
            reader,
            Queries.ALL_DOCS_INSTANCE,
            builder,
            breakers,
            buildResults,
            null,
            built -> held[0] = breakers.getBreaker(CircuitBreaker.REQUEST).getUsed() - baseline,
            STRING_FIELD_TYPE,
            numberFieldType(NumberFieldMapper.NumberType.LONG)
        );
        return held[0];
    }

    private static CheckedConsumer<RandomIndexWriter, IOException> uniqueBuckets(int buckets) {
        return iw -> {
            for (int i = 0; i < buckets; i++) {
                iw.addDocument(
                    numberDocument(Integer.toString(i), String.format(Locale.ROOT, "term%06d", i), NumberFieldMapper.NumberType.LONG, i)
                );
            }
        };
    }

    private static AggregationBuilder termsWithSizeOneTopHits(int buckets) {
        return termsWithTopHitsOfSize(buckets, 1);
    }

    /**
     * A {@code terms} that keeps {@code termsSize} buckets, with the default shard size, with a {@code top_hits} that keeps
     * {@code topHitsSize} documents in each of them.
     */
    private static TermsAggregationBuilder termsWithTopHitsOfSize(int termsSize, int topHitsSize) {
        return terms("term").field("string")
            .size(termsSize)
            .subAggregation(topHits("top").size(topHitsSize).sort(NUMBER_FIELD, SortOrder.ASC));
    }

    /**
     * The control for the cost of the top hits: a {@code terms} with the cheapest kind of sub-aggregation, a {@code max}, which
     * keeps one double per bucket. A {@code terms} costs more per bucket as soon as it has any sub-aggregation, so comparing with
     * a {@code terms} on its own would charge that to the top hits.
     */
    private static AggregationBuilder termsWithMax(int buckets) {
        return terms("term").field("string").size(buckets).subAggregation(AggregationBuilders.max("max").field(NUMBER_FIELD));
    }

    /**
     * What the top hits of a bucket cost the request breaker. A {@code BucketedSort} reserves {@code from + size} slots, 12 bytes
     * each, the first time it sees a bucket, whether the bucket ends up with that many documents or not, and charges all of it to
     * the request breaker, where the Lucene collectors held several kilobytes per bucket and charged none of it. So the cost grows
     * with the size, which is also why a large size over many buckets can now trip the breaker. Every bucket here has a single
     * document, so what is charged is the slots and not the documents. The cost of a {@code max} sub-aggregation, which is 8 bytes
     * a bucket, is taken off.
     */
    public void testBucketedSortChargesTheSlotsOfEveryBucketAndReleasesThem() throws Exception {
        int buckets = 2_000;
        withIndex(uniqueBuckets(buckets), reader -> {
            HierarchyCircuitBreakerService breakers = requestBreakers("1gb");
            long withMax = breakerBytesAfterCollecting(reader, termsWithMax(buckets), breakers, false);
            for (int size : new int[] { 1, 100 }) {
                long withTopHits = breakerBytesAfterCollecting(reader, termsWithTopHitsOfSize(buckets, size), breakers, false);
                assertThat("the top hits are charged to the breaker", withTopHits, greaterThan(withMax));
                // 12 bytes a slot, and up to about an eighth more for the room the arrays grow by.
                long perBucket = (withTopHits - withMax) / buckets;
                assertThat("bytes charged per bucket at size " + size, perBucket, greaterThanOrEqualTo(12L * size - 16));
                assertThat("bytes charged per bucket at size " + size, perBucket, lessThanOrEqualTo(16L * size + 16));
                assertEquals("the breaker is back to its baseline", 0L, breakers.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
        });
    }

    /**
     * The state of a {@code BucketedSort} is indexed by bucket ordinal, so it would grow with the highest ordinal it is given if a
     * parent aggregation handed it sparse ones, where the Lucene collectors only held the buckets they saw. A {@code terms} does not:
     * collecting depth first remaps the global ordinals as it sees them, and collecting breadth first defers the {@code top_hits}
     * and replays the documents of the buckets it keeps with their ordinals rebased to {@code 0..n}. So the state follows the
     * buckets the {@code top_hits} collects and not the number of terms. A {@code terms} over 20,000 values that keeps 10 of them
     * breadth first must cost about nothing, where depth first it has to collect all of them.
     */
    public void testBucketedSortStateFollowsTheBucketsItCollectsNotTheNumberOfTerms() throws Exception {
        int termCount = 20_000;
        int kept = 10;
        withIndex(uniqueBuckets(termCount), reader -> {
            HierarchyCircuitBreakerService breakers = requestBreakers("1gb");
            for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
                // The results are built because a deferred sub-aggregation only collects when they are.
                long withMax = breakerBytesAfterCollecting(
                    reader,
                    terms("term").field("string")
                        .size(kept)
                        .collectMode(mode)
                        .subAggregation(AggregationBuilders.max("max").field(NUMBER_FIELD)),
                    breakers,
                    true
                );
                long withTopHits = breakerBytesAfterCollecting(reader, termsWithTopHitsOfSize(kept, 1).collectMode(mode), breakers, true);
                long extra = withTopHits - withMax;
                switch (mode) {
                    // Only the buckets kept are replayed into the top_hits, 25 of them with the shard size of 10, which is
                    // nothing compared with the 20,000 terms, which would be about 240 KB if the state followed the ordinals. It is
                    // about 500 bytes.
                    case BREADTH_FIRST -> assertThat("breadth first, bytes more than a max", extra, lessThanOrEqualTo(4L * 1024));
                    // Every term is a bucket the top_hits collects before the terms picks the best ones.
                    case DEPTH_FIRST -> assertThat("depth first, bytes more than a max", extra, lessThanOrEqualTo(32L * termCount));
                    default -> throw new AssertionError("unexpected collect mode [" + mode + "]");
                }
                assertEquals("the breaker is back to its baseline", 0L, breakers.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
        });
    }

    /**
     * A breaker that has room for the {@code terms} and a cheap sub-aggregation but not for the top hits throws instead of the
     * node running out of heap, and everything is released.
     */
    public void testBucketedSortTripsTheBreakerAndReleasesEverything() throws Exception {
        int buckets = 5_000;
        withIndex(uniqueBuckets(buckets), reader -> {
            HierarchyCircuitBreakerService generous = requestBreakers("1gb");
            long withMax = breakerBytesAfterCollecting(reader, termsWithMax(buckets), generous, false);
            long withTopHits = breakerBytesAfterCollecting(reader, termsWithSizeOneTopHits(buckets), generous, false);
            // Room for the control, and for half of what the top hits need on top of it.
            HierarchyCircuitBreakerService tight = requestBreakers((withMax + (withTopHits - withMax) / 2) + "b");
            breakerBytesAfterCollecting(reader, termsWithMax(buckets), tight, false);
            assertEquals(0L, tight.getBreaker(CircuitBreaker.REQUEST).getUsed());
            expectThrows(
                CircuitBreakingException.class,
                () -> breakerBytesAfterCollecting(reader, termsWithSizeOneTopHits(buckets), tight, false)
            );
            assertEquals("the breaker is back to its baseline", 0L, tight.getBreaker(CircuitBreaker.REQUEST).getUsed());
        });
    }

    /**
     * A breaker that fails at random, wherever the aggregation allocates: creating it, collecting, and building the results. Each
     * failure has to release everything, which the test infrastructure checks for leaked arrays when the test ends.
     */
    public void testBucketedSortReleasesEverythingWhenTheBreakerFailsAtRandom() throws Exception {
        int buckets = 300;
        withIndex(uniqueBuckets(buckets), reader -> {
            int failures = 0;
            for (int i = 0; i < 40; i++) {
                try {
                    collect(
                        reader,
                        Queries.ALL_DOCS_INSTANCE,
                        termsWithSizeOneTopHits(buckets),
                        new CrankyCircuitBreakerService(),
                        true,
                        null,
                        built -> {},
                        STRING_FIELD_TYPE,
                        numberFieldType(NumberFieldMapper.NumberType.LONG)
                    );
                } catch (CircuitBreakingException e) {
                    failures++;
                    assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
                }
            }
            assertThat("the breaker was expected to fail in some of the runs", failures, greaterThan(0));
        });
    }

    private IndexVersion indexVersionCreated = IndexVersion.current();

    @Override
    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionCreated))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }

    /**
     * Int, short and byte fields declare a long sort, and not an int one, in indices created before
     * {@link IndexVersions#INDEX_INT_SORT_INT_TYPE}, except for the ones created from
     * {@link IndexVersions#INDEX_INT_SORT_INT_TYPE_8_19} to before {@link IndexVersions#UPGRADE_TO_LUCENE_10_0_0}. The Lucene collectors fill that sort with longs. The sort values of the shard results are
     * merged using the type the sort field declares, so a {@code BucketedSort} that reports integers for such a field would break
     * the merge with the shards of newer indices. Those fields keep the Lucene collectors, and the other types are not affected.
     */
    public void testIntegerFieldsOfOlderIndicesKeepTheLuceneCollectors() throws Exception {
        record Case(IndexVersion version, NumberFieldMapper.NumberType type, boolean bucketedSort) {}
        List<Case> cases = new ArrayList<>();
        for (NumberFieldMapper.NumberType type : NUMERIC_TYPES) {
            boolean declaresLong = type == NumberFieldMapper.NumberType.BYTE
                || type == NumberFieldMapper.NumberType.SHORT
                || type == NumberFieldMapper.NumberType.INTEGER;
            cases.add(new Case(IndexVersions.UPGRADE_TO_LUCENE_10_0_0, type, declaresLong == false));
            if (declaresLong) {
                cases.add(new Case(IndexVersions.INDEX_INT_SORT_INT_TYPE_8_19, type, true));
                cases.add(new Case(IndexVersions.INDEX_INT_SORT_INT_TYPE, type, true));
                cases.add(new Case(IndexVersion.current(), type, true));
            }
        }
        for (Case testCase : cases) {
            indexVersionCreated = testCase.version;
            withUnmergedIndex(iw -> iw.addDocument(numberDocument("0", "t0", testCase.type, numberValue(testCase.type, 1))), reader -> {
                try {
                    collectAndBuild(
                        reader,
                        Queries.ALL_DOCS_INSTANCE,
                        terms("term").field("string").size(10).subAggregation(topHits("top").sort(NUMBER_FIELD, SortOrder.ASC)),
                        testCase.bucketedSort,
                        built -> {},
                        STRING_FIELD_TYPE,
                        numberFieldType(testCase.type)
                    );
                } catch (AssertionError e) {
                    throw new AssertionError(testCase.toString(), e);
                }
            });
        }
    }

    private static final MappedFieldType STRING_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType("string");

    private Aggregation testCase(Query query, AggregationBuilder builder) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
        iw.addDocument(document("1", "a", "b"));
        iw.addDocument(document("2", "c", "a"));
        iw.addDocument(document("3", "b", "d"));
        iw.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        Aggregation result = searchAndReduce(indexReader, new AggTestConfig(builder, STRING_FIELD_TYPE).withQuery(query));
        indexReader.close();
        directory.close();
        return result;
    }

    private Document document(String id, String... stringValues) {
        Document document = new Document();
        document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Store.YES));
        for (String stringValue : stringValues) {
            document.add(new Field("string", new BytesRef(stringValue), KeywordFieldMapper.Defaults.FIELD_TYPE));
        }
        return document;
    }

    public void testSetScorer() throws Exception {
        Directory directory = newDirectory();
        IndexWriter w = new IndexWriter(
            directory,
            newIndexWriterConfig()
                // only merge adjacent segments
                .setMergePolicy(newLogMergePolicy())
        );
        // first window (see BooleanScorer) has matches on one clause only
        for (int i = 0; i < 2048; ++i) {
            Document doc = new Document();
            doc.add(new StringField("_id", Uid.encodeId(Integer.toString(i)), Store.YES));
            if (i == 1000) { // any doc in 0..2048
                doc.add(new StringField("string", "bar", Store.NO));
            }
            w.addDocument(doc);
        }
        // second window has matches in two clauses
        for (int i = 0; i < 2048; ++i) {
            Document doc = new Document();
            doc.add(new StringField("_id", Uid.encodeId(Integer.toString(2048 + i)), Store.YES));
            if (i == 500) { // any doc in 0..2048
                doc.add(new StringField("string", "baz", Store.NO));
            } else if (i == 1500) {
                doc.add(new StringField("string", "bar", Store.NO));
            }
            w.addDocument(doc);
        }

        w.forceMerge(1); // we need all docs to be in the same segment

        IndexReader reader = DirectoryReader.open(w);
        w.close();

        Query query = new BooleanQuery.Builder().add(new TermQuery(new Term("string", "bar")), Occur.SHOULD)
            .add(new TermQuery(new Term("string", "baz")), Occur.SHOULD)
            .build();
        AggregationBuilder agg = AggregationBuilders.topHits("top_hits");
        TopHits result = searchAndReduce(reader, new AggTestConfig(agg, STRING_FIELD_TYPE).withQuery(query));
        assertEquals(3, result.getHits().getTotalHits().value());
        reader.close();
        directory.close();
    }

    public void testSortByScore() throws Exception {
        // just check that it does not fail with exceptions
        testCase(Queries.ALL_DOCS_INSTANCE, topHits("_name").sort("_score", SortOrder.DESC));
        testCase(Queries.ALL_DOCS_INSTANCE, topHits("_name").sort("_score"));
    }
}

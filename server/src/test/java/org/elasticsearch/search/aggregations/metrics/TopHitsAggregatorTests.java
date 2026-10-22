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
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
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
import org.elasticsearch.index.mapper.NumberFieldMapper;
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
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortFieldValidation;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
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
     * The sort a plain Lucene collector would use for the same data. The raw sort values it produces are the ones the shard
     * results are merged with, so the types must match what the {@code BucketedSort} reports.
     */
    private static Sort luceneSort(NumberFieldMapper.NumberType type, SortOrder order) {
        SortField.Type sortType = switch (type) {
            case BYTE, SHORT, INTEGER -> SortField.Type.INT;
            case LONG -> SortField.Type.LONG;
            case FLOAT -> SortField.Type.FLOAT;
            case DOUBLE -> SortField.Type.DOUBLE;
            default -> throw new AssertionError("unexpected number type [" + type + "]");
        };
        boolean reverse = order == SortOrder.DESC;
        SortedNumericSelector.Type selector = reverse ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
        if (type == NumberFieldMapper.NumberType.LONG) {
            // Documents without a value sort last, as the default of top_hits does.
            return new Sort(
                new SortedNumericSortField(NUMBER_FIELD, sortType, reverse, selector, reverse ? Long.MIN_VALUE : Long.MAX_VALUE)
            );
        }
        return new Sort(new SortedNumericSortField(NUMBER_FIELD, sortType, reverse, selector));
    }

    private record ExpectedHits(long total, List<String> ids, List<Object> sortValues, List<Float> scores, float maxScore) {}

    /**
     * What plain Lucene collectors return for one bucket, which is the documents with the bucket's term.
     */
    private static ExpectedHits expectedHits(
        IndexSearcher searcher,
        Query query,
        String term,
        Sort sort,
        int from,
        int size,
        boolean trackScores
    ) throws IOException {
        Query bucketQuery = new BooleanQuery.Builder().add(query, Occur.MUST)
            .add(new TermQuery(new Term("string", term)), Occur.FILTER)
            .build();
        TopFieldDocs top = searcher.search(bucketQuery, new TopFieldCollectorManager(sort, from + size, null, Integer.MAX_VALUE));
        ScoreDoc[] window = Arrays.copyOfRange(
            top.scoreDocs,
            Math.min(from, top.scoreDocs.length),
            Math.min(from + size, top.scoreDocs.length)
        );
        if (trackScores) {
            TopFieldCollector.populateScores(window, searcher, bucketQuery);
        }
        List<String> ids = new ArrayList<>();
        List<Object> sortValues = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (ScoreDoc scoreDoc : window) {
            ids.add(Uid.decodeId(searcher.storedFields().document(scoreDoc.doc).getBinaryValue(IdFieldMapper.NAME)));
            sortValues.add(((FieldDoc) scoreDoc).fields[0]);
            scores.add(scoreDoc.score);
        }
        float maxScore = Float.NaN;
        if (trackScores && top.totalHits.value() > 0) {
            maxScore = searcher.search(bucketQuery, 1).scoreDocs[0].score;
        }
        return new ExpectedHits(top.totalHits.value(), ids, sortValues, scores, maxScore);
    }

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
                .withCheckAggregator(aggregator -> {
                    // Only run when the leaves are split, and then once for every aggregator that is built.
                    for (TopHitsAggregator topHitsAggregator : topHitsAggregators(aggregator)) {
                        assertEquals("collected with a BucketedSort", expectBucketedSort, topHitsAggregator.usesBucketedSort());
                    }
                })
        );
    }

    /**
     * What the result of a bucket says: the exact total, and for each hit its id, its raw sort value and its score, and the
     * highest score.
     */
    private static ExpectedHits summarize(SearchHit[] hits, TotalHits totalHits, float maxScore) {
        assertEquals(TotalHits.Relation.EQUAL_TO, totalHits.relation());
        List<String> ids = new ArrayList<>();
        List<Object> sortValues = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (SearchHit hit : hits) {
            ids.add(hit.getId());
            sortValues.add(hit.getRawSortValues()[0]);
            scores.add(hit.getScore());
        }
        return new ExpectedHits(totalHits.value(), ids, sortValues, scores, maxScore);
    }

    /**
     * The result after the reduce, where the hits carry the total and the highest score.
     */
    private static ExpectedHits summarize(SearchHits hits) {
        return summarize(hits.getHits(), hits.getTotalHits(), hits.getMaxScore());
    }

    /**
     * The result of one aggregator before the reduce, where the total and the highest score are in its top docs, and the hits
     * are all of the {@code from + size} the aggregator keeps.
     */
    private static ExpectedHits summarize(InternalTopHits partial) {
        TopDocsAndMaxScore topDocs = partial.getTopDocs();
        return summarize(partial.getHits().getHits(), topDocs.topDocs.totalHits, topDocs.maxScore);
    }

    private static void assertSummary(String description, ExpectedHits expected, ExpectedHits actual) {
        assertEquals(description + " total hits", expected.total, actual.total);
        assertEquals(description + " ids", expected.ids, actual.ids);
        assertEquals(description + " sort values", expected.sortValues, actual.sortValues);
        assertEquals(description + " scores", expected.scores, actual.scores);
        assertEquals(description + " max score", expected.maxScore, actual.maxScore, 0f);
    }

    /**
     * Compares the hits after the reduce with the ones plain Lucene collectors find. The reduce merges the results of separate
     * aggregators, the order of which is not deterministic, so this is only for sort values that are unique.
     */
    private void assertReducedMatchesLuceneCollectors(
        IndexReader reader,
        NumberFieldMapper.NumberType type,
        SortOrder order,
        int termCount,
        int from,
        int size,
        boolean trackScores
    ) throws IOException {
        Terms terms = termsWithTopHits(
            reader,
            type,
            topHits("top").sort(NUMBER_FIELD, order).from(from).size(size).trackScores(trackScores),
            termCount,
            true,
            true
        );
        IndexSearcher searcher = new IndexSearcher(reader);
        for (Terms.Bucket bucket : terms.getBuckets()) {
            ExpectedHits expected = expectedHits(
                searcher,
                boostedTermsQuery(termCount),
                bucket.getKeyAsString(),
                luceneSort(type, order),
                from,
                size,
                trackScores
            );
            assertSummary(
                "bucket [" + bucket.getKeyAsString() + "] type [" + type + "] order [" + order + "]",
                expected,
                summarize(((TopHits) bucket.getAggregations().get("top")).getHits())
            );
        }
    }

    /**
     * Compares what one aggregator returns, before any reduce, with the hits plain Lucene collectors find. One sequential
     * search over every segment feeds one aggregator, so ties are broken by doc id and nothing else, as they are by Lucene.
     * The aggregator keeps {@code from + size} hits per bucket, and the reduce is what drops the first {@code from}.
     */
    private void assertPartialMatchesLuceneCollectors(
        IndexReader reader,
        NumberFieldMapper.NumberType type,
        SortOrder order,
        int termCount,
        int from,
        int size,
        boolean trackScores
    ) throws IOException {
        Query query = boostedTermsQuery(termCount);
        IndexSearcher searcher = new IndexSearcher(reader);
        collectAndBuild(
            reader,
            query,
            terms("term").field("string")
                .size(termCount)
                // Breadth first collection defers the sub-aggregation and replays the documents of the buckets that are kept.
                .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
                .subAggregation(topHits("top").sort(NUMBER_FIELD, order).from(from).size(size).trackScores(trackScores)),
            built -> {
                for (Terms.Bucket bucket : ((Terms) built).getBuckets()) {
                    ExpectedHits expected = expectedHits(
                        searcher,
                        query,
                        bucket.getKeyAsString(),
                        luceneSort(type, order),
                        0,
                        from + size,
                        trackScores
                    );
                    assertSummary(
                        "bucket [" + bucket.getKeyAsString() + "] type [" + type + "] order [" + order + "]",
                        expected,
                        summarize((InternalTopHits) bucket.getAggregations().get("top"))
                    );
                }
            },
            STRING_FIELD_TYPE,
            numberFieldType(type)
        );
        assertBucketedSortUsed(true);
    }

    /**
     * The same documents and sort as plain Lucene collectors, for every numeric type, both orders, with many ties, with and
     * without missing values, {@code from}, and {@code track_scores}, over several segments and one aggregator.
     */
    public void testBucketedSortMatchesLuceneCollectors() throws Exception {
        NumberFieldMapper.NumberType type = randomFrom(
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.INTEGER,
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.FLOAT,
            NumberFieldMapper.NumberType.DOUBLE
        );
        boolean missingValues = type == NumberFieldMapper.NumberType.LONG && randomBoolean();
        int termCount = between(1, 6);
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                int docs = between(1, 200);
                for (int i = 0; i < docs; i++) {
                    // Values from a small range so that there are plenty of ties.
                    Number number = missingValues && rarely() ? null : numberValue(type, between(0, 9));
                    if (number != null && rarely()) {
                        // NaN sorts after every other value, and the sort has to report it as one.
                        if (type == NumberFieldMapper.NumberType.FLOAT) {
                            number = Float.NaN;
                        } else if (type == NumberFieldMapper.NumberType.DOUBLE) {
                            number = Double.NaN;
                        }
                    }
                    iw.addDocument(numberDocument(Integer.toString(i), "t" + between(0, termCount - 1), type, number));
                    if (rarely()) {
                        iw.commit();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertPartialMatchesLuceneCollectors(
                    reader,
                    type,
                    randomFrom(SortOrder.values()),
                    termCount,
                    between(0, 3),
                    between(1, 5),
                    randomBoolean()
                );
            }
        }
    }

    /**
     * Separate aggregators for the segments, whose results are merged in the reduce, which compares the raw sort values. The
     * values are unique so that the order of the merge, which is shuffled by the test, cannot change the result.
     */
    public void testBucketedSortMergesAcrossAggregators() throws Exception {
        NumberFieldMapper.NumberType type = randomFrom(
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.INTEGER,
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.FLOAT,
            NumberFieldMapper.NumberType.DOUBLE
        );
        int termCount = between(1, 4);
        int docs = between(2, 100);
        List<Long> values = new ArrayList<>();
        for (long v = 0; v < docs; v++) {
            values.add(v);
        }
        Collections.shuffle(values, random());
        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < docs; i++) {
                    iw.addDocument(
                        numberDocument(Integer.toString(i), "t" + between(0, termCount - 1), type, numberValue(type, values.get(i)))
                    );
                    if (i % 10 == 9) {
                        iw.commit();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertReducedMatchesLuceneCollectors(
                    reader,
                    type,
                    randomFrom(SortOrder.values()),
                    termCount,
                    between(0, 3),
                    between(1, 5),
                    randomBoolean()
                );
            }
        }
    }

    /**
     * Among equal sort values the lower doc id is kept, as a Lucene collector does. Values 6, 5, 5, 4, 3 in doc id order, keeping
     * the best three ascending, have to keep doc 1 and not doc 2, and the mirror image for descending.
     */
    public void testBucketedSortKeepsLowerDocIdOnTies() throws Exception {
        for (SortOrder order : SortOrder.values()) {
            long[] values = order == SortOrder.ASC ? new long[] { 6, 5, 5, 4, 3 } : new long[] { 3, 4, 4, 5, 6 };
            try (Directory directory = newDirectory()) {
                try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                    for (int i = 0; i < values.length; i++) {
                        iw.addDocument(numberDocument(Integer.toString(i), "t0", NumberFieldMapper.NumberType.LONG, values[i]));
                    }
                }
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    // Without merging, doc ids follow the order the documents were added, whatever the number of segments.
                    collectAndBuild(
                        reader,
                        boostedTermsQuery(1),
                        terms("term").field("string").size(1).subAggregation(topHits("top").sort(NUMBER_FIELD, order).size(3)),
                        built -> {
                            Terms.Bucket bucket = ((Terms) built).getBucketByKey("t0");
                            ExpectedHits actual = summarize((InternalTopHits) bucket.getAggregations().get("top"));
                            assertEquals(order.toString(), List.of("4", "3", "1"), actual.ids);
                            assertEquals(5L, actual.total);
                        },
                        STRING_FIELD_TYPE,
                        numberFieldType(NumberFieldMapper.NumberType.LONG)
                    );
                    assertBucketedSortUsed(true);
                }
            }
        }
    }

    /**
     * Sorting on the number and then on {@code _doc} cannot use the {@code BucketedSort}, and its tie-break by doc id is the
     * one the {@code BucketedSort} has to reproduce, so the two have to return the same thing. Unlike the tests against hand
     * made Lucene sorts, this compares with the comparators Elasticsearch builds, so it also covers what those do with the
     * options of a sort: every numeric type, missing documents, several values per document with each sort mode, the
     * {@code missing} option, and {@code numeric_type}.
     */
    public void testBucketedSortMatchesTheLuceneCollectorsOfTheFallback() throws Exception {
        NumberFieldMapper.NumberType type = randomFrom(
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.INTEGER,
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.HALF_FLOAT,
            NumberFieldMapper.NumberType.FLOAT,
            NumberFieldMapper.NumberType.DOUBLE
        );
        int termCount = between(1, 4);
        int from = between(0, 2);
        int size = between(1, 4);
        boolean trackScores = randomBoolean();
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
        Aggregator.SubAggCollectionMode collectMode = randomFrom(Aggregator.SubAggCollectionMode.values());
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                int docs = between(1, 150);
                for (int i = 0; i < docs; i++) {
                    List<Number> numbers = new ArrayList<>();
                    // About one document in six has no value at all.
                    int count = rarely() ? 0 : multiValued ? between(1, 3) : 1;
                    for (int n = 0; n < count; n++) {
                        numbers.add(numberValue(type, between(0, 5)));
                    }
                    iw.addDocument(numberDocument(Integer.toString(i), "t" + between(0, termCount - 1), type, numbers));
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                Query query = boostedTermsQuery(termCount);
                Map<String, ExpectedHits> bucketed = summarizeBuckets(
                    reader,
                    query,
                    type,
                    topHits("top").sort(sort).from(from).size(size).trackScores(trackScores),
                    termCount,
                    collectMode,
                    true
                );
                Map<String, ExpectedHits> fallback = summarizeBuckets(
                    reader,
                    query,
                    type,
                    topHits("top").sort(sort).sort("_doc", SortOrder.ASC).from(from).size(size).trackScores(trackScores),
                    termCount,
                    collectMode,
                    false
                );
                assertThat(bucketed.size(), greaterThan(0));
                assertEquals(fallback, bucketed);
            }
        }
    }

    private Map<String, ExpectedHits> summarizeBuckets(
        IndexReader reader,
        Query query,
        NumberFieldMapper.NumberType type,
        TopHitsAggregationBuilder topHits,
        int termCount,
        Aggregator.SubAggCollectionMode collectMode,
        boolean expectBucketedSort
    ) throws IOException {
        Map<String, ExpectedHits> summaries = new HashMap<>();
        collectAndBuild(
            reader,
            query,
            terms("term").field("string").size(termCount).collectMode(collectMode).subAggregation(topHits),
            built -> {
                for (Terms.Bucket bucket : ((Terms) built).getBuckets()) {
                    summaries.put(bucket.getKeyAsString(), summarize((InternalTopHits) bucket.getAggregations().get("top")));
                }
            },
            STRING_FIELD_TYPE,
            numberFieldType(type)
        );
        assertBucketedSortUsed(expectBucketedSort);
        return summaries;
    }

    /**
     * A {@code NaN} sorts after every other value, in both orders, and is reported as {@code NaN}. The {@code BucketedSort}
     * has no number to report for it, so this checks it is turned back into one of the type the Lucene comparator gives.
     */
    public void testBucketedSortReportsNaN() throws Exception {
        for (NumberFieldMapper.NumberType type : List.of(NumberFieldMapper.NumberType.DOUBLE, NumberFieldMapper.NumberType.FLOAT)) {
            for (SortOrder order : SortOrder.values()) {
                Number nan = type == NumberFieldMapper.NumberType.DOUBLE ? (Number) Double.NaN : (Number) Float.NaN;
                try (Directory directory = newDirectory()) {
                    try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                        Number[] values = new Number[] { nan, numberValue(type, 1), nan, numberValue(type, 2) };
                        for (int i = 0; i < values.length; i++) {
                            iw.addDocument(numberDocument(Integer.toString(i), "t0", type, values[i]));
                        }
                    }
                    try (IndexReader reader = DirectoryReader.open(directory)) {
                        Query query = boostedTermsQuery(1);
                        IndexSearcher searcher = new IndexSearcher(reader);
                        collectAndBuild(
                            reader,
                            query,
                            terms("term").field("string").size(1).subAggregation(topHits("top").sort(NUMBER_FIELD, order).size(4)),
                            built -> assertSummary(
                                type + " " + order,
                                expectedHits(searcher, query, "t0", luceneSort(type, order), 0, 4, false),
                                summarize((InternalTopHits) ((Terms) built).getBucketByKey("t0").getAggregations().get("top"))
                            ),
                            STRING_FIELD_TYPE,
                            numberFieldType(type)
                        );
                        assertBucketedSortUsed(true);
                    }
                }
            }
        }
    }

    /**
     * A {@code date_nanos} sort uses the long comparator over the values as nanoseconds, so the raw sort values have to come out
     * as the same {@link Long}s the Lucene collectors give, with the same ties.
     */
    public void testBucketedSortOnDateNanos() throws Exception {
        MappedFieldType dateNanos = new DateFieldMapper.DateFieldType(NUMBER_FIELD, DateFieldMapper.Resolution.NANOSECONDS);
        SortOrder order = randomFrom(SortOrder.values());
        int termCount = between(1, 4);
        int size = between(1, 4);
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                int docs = between(1, 100);
                for (int i = 0; i < docs; i++) {
                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Store.YES));
                    document.add(
                        new Field("string", new BytesRef("t" + between(0, termCount - 1)), KeywordFieldMapper.Defaults.FIELD_TYPE)
                    );
                    // A few distinct instants, so that there are ties.
                    document.add(
                        new SortedNumericDocValuesField(NUMBER_FIELD, 1_700_000_000_000_000_000L + between(0, 5) * 1_000_000_007L)
                    );
                    iw.addDocument(document);
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                Query query = boostedTermsQuery(termCount);
                IndexSearcher searcher = new IndexSearcher(reader);
                collectAndBuild(
                    reader,
                    query,
                    terms("term").field("string").size(termCount).subAggregation(topHits("top").sort(NUMBER_FIELD, order).size(size)),
                    built -> {
                        for (Terms.Bucket bucket : ((Terms) built).getBuckets()) {
                            assertSummary(
                                "bucket [" + bucket.getKeyAsString() + "] order [" + order + "]",
                                expectedHits(
                                    searcher,
                                    query,
                                    bucket.getKeyAsString(),
                                    luceneSort(NumberFieldMapper.NumberType.LONG, order),
                                    0,
                                    size,
                                    false
                                ),
                                summarize((InternalTopHits) bucket.getAggregations().get("top"))
                            );
                        }
                    },
                    STRING_FIELD_TYPE,
                    dateNanos
                );
                assertBucketedSortUsed(true);
            }
        }
    }

    /**
     * Only a single sort on a numeric field is collected with the {@code BucketedSort}, and the decision is made when the
     * aggregator is created.
     */
    public void testWhichSortsUseBucketedSort() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 5; i++) {
                    iw.addDocument(numberDocument(Integer.toString(i), "t" + i, NumberFieldMapper.NumberType.LONG, i));
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertTrue(usesBucketedSort(reader, topHits("top").sort(NUMBER_FIELD, SortOrder.ASC)));
                assertTrue(usesBucketedSort(reader, topHits("top").sort(NUMBER_FIELD, SortOrder.DESC).trackScores(true).from(1).size(2)));
                assertFalse("no sort", usesBucketedSort(reader, topHits("top")));
                assertFalse("not numeric", usesBucketedSort(reader, topHits("top").sort("string", SortOrder.ASC)));
                assertFalse("_doc", usesBucketedSort(reader, topHits("top").sort("_doc", SortOrder.ASC)));
                assertFalse(
                    "two sorts",
                    usesBucketedSort(reader, topHits("top").sort(NUMBER_FIELD, SortOrder.ASC).sort("_doc", SortOrder.ASC))
                );
                assertFalse("score", usesBucketedSort(reader, topHits("top").sort(SortBuilders.scoreSort())));
            }
        }
    }

    private boolean usesBucketedSort(IndexReader reader, TopHitsAggregationBuilder topHits) throws IOException {
        return usesBucketedSort(reader, NumberFieldMapper.NumberType.LONG, topHits);
    }

    private boolean usesBucketedSort(IndexReader reader, NumberFieldMapper.NumberType type, TopHitsAggregationBuilder topHits)
        throws IOException {
        AggregationContext context = createAggregationContext(
            reader,
            createIndexSettings(),
            Queries.ALL_DOCS_INSTANCE,
            requestBreakers("1gb"),
            0,
            Integer.MAX_VALUE,
            false,
            false,
            STRING_FIELD_TYPE,
            numberFieldType(type)
        );
        try {
            Aggregator terms = createAggregator(terms("term").field("string").size(10).subAggregation(topHits), context);
            return ((TopHitsAggregator) terms.subAggregators()[0]).usesBucketedSort();
        } finally {
            Releasables.close(context);
        }
    }

    /**
     * A bucket keeps at most as many entries as the shard has documents, whatever {@code from + size} says. An index can allow a
     * very large window, and a {@code BucketedSort} allocates that many slots for every bucket.
     */
    public void testBucketSizeIsCappedAtTheNumberOfDocuments() {
        assertEquals(15, BucketedTopHits.bucketSize(5, 10, 100));
        assertEquals(7, BucketedTopHits.bucketSize(0, 100, 7));
        assertEquals(7, BucketedTopHits.bucketSize(3, 10_000, 7));
        assertEquals(0, BucketedTopHits.bucketSize(0, 0, 10));
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
     * set, to {@code check} before everything the aggregation holds is released.
     */
    private void collect(
        IndexReader reader,
        Query query,
        AggregationBuilder builder,
        CircuitBreakerService breakers,
        boolean buildResults,
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
            usedBucketedSort.clear();
            for (TopHitsAggregator topHits : topHitsAggregators(aggregator)) {
                usedBucketedSort.add(topHits.usesBucketedSort());
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
        CheckedConsumer<InternalAggregation, IOException> check,
        MappedFieldType... fieldTypes
    ) throws IOException {
        collect(reader, query, builder, requestBreakers("1gb"), true, check, fieldTypes);
    }

    /**
     * For each {@code top_hits} the last {@link #collect} built, whether it collected with a {@code BucketedSort}. A test that
     * compares the results of two ways of collecting is only worth anything if it knows which one it ran, because falling back to
     * the Lucene collectors compares them with themselves.
     */
    private final List<Boolean> usedBucketedSort = new ArrayList<>();

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

    private void assertBucketedSortUsed(boolean expected) {
        assertThat("a top_hits was built", usedBucketedSort, not(empty()));
        for (boolean used : usedBucketedSort) {
            assertEquals("collected with a BucketedSort", expected, used);
        }
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
            built -> held[0] = breakers.getBreaker(CircuitBreaker.REQUEST).getUsed() - baseline,
            STRING_FIELD_TYPE,
            numberFieldType(NumberFieldMapper.NumberType.LONG)
        );
        return held[0];
    }

    private void indexUniqueBuckets(Directory directory, int buckets) throws IOException {
        try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            for (int i = 0; i < buckets; i++) {
                iw.addDocument(
                    numberDocument(Integer.toString(i), String.format(Locale.ROOT, "term%06d", i), NumberFieldMapper.NumberType.LONG, i)
                );
            }
        }
    }

    private static AggregationBuilder termsWithSizeOneTopHits(int buckets) {
        return terms("term").field("string").size(buckets).subAggregation(topHits("top").size(1).sort(NUMBER_FIELD, SortOrder.ASC));
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
     * What the top hits of a bucket cost the request breaker when there are many buckets. The Lucene collectors accounted for
     * none of it and held several kilobytes per bucket. Measured in a unit test it is about 16 bytes per bucket on top of a
     * {@code max}, and a {@code max} itself is 8, so about 24.
     */
    public void testBucketedSortChargesLittlePerBucketAndReleasesIt() throws Exception {
        int buckets = 5_000;
        try (Directory directory = newDirectory()) {
            indexUniqueBuckets(directory, buckets);
            try (IndexReader reader = DirectoryReader.open(directory)) {
                HierarchyCircuitBreakerService breakers = requestBreakers("1gb");
                long withMax = breakerBytesAfterCollecting(reader, termsWithMax(buckets), breakers, false);
                long withTopHits = breakerBytesAfterCollecting(reader, termsWithSizeOneTopHits(buckets), breakers, false);
                assertThat("the top hits are charged to the breaker", withTopHits, greaterThan(withMax));
                assertThat("bytes charged per bucket over a max", (withTopHits - withMax) / buckets, lessThanOrEqualTo(32L));
                assertEquals("the breaker is back to its baseline", 0L, breakers.getBreaker(CircuitBreaker.REQUEST).getUsed());
            }
        }
    }

    /**
     * A breaker that has room for the {@code terms} and a cheap sub-aggregation but not for the top hits throws instead of the
     * node running out of heap, and everything is released.
     */
    public void testBucketedSortTripsTheBreakerAndReleasesEverything() throws Exception {
        int buckets = 5_000;
        try (Directory directory = newDirectory()) {
            indexUniqueBuckets(directory, buckets);
            try (IndexReader reader = DirectoryReader.open(directory)) {
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
            }
        }
    }

    /**
     * A breaker that fails at random, wherever the aggregation allocates: creating it, collecting, and building the results. Each
     * failure has to release everything, which the test infrastructure checks for leaked arrays when the test ends.
     */
    public void testBucketedSortReleasesEverythingWhenTheBreakerFailsAtRandom() throws Exception {
        int buckets = 300;
        try (Directory directory = newDirectory()) {
            indexUniqueBuckets(directory, buckets);
            try (IndexReader reader = DirectoryReader.open(directory)) {
                int failures = 0;
                for (int i = 0; i < 40; i++) {
                    try {
                        collect(
                            reader,
                            Queries.ALL_DOCS_INSTANCE,
                            termsWithSizeOneTopHits(buckets),
                            new CrankyCircuitBreakerService(),
                            true,
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
            }
        }
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
     * Int, short and byte fields declare a long sort, and not an int one, in indices created before 9.28, except for the ones
     * created from 8.19 to before 9.0. The Lucene collectors fill that sort with longs. The sort values of the shard results are
     * merged using the type the sort field declares, so a {@code BucketedSort} that reports integers for such a field would break
     * the merge with the shards of newer indices. Those fields keep the Lucene collectors, and the other types are not affected.
     */
    public void testIntegerFieldsOfOlderIndicesKeepTheLuceneCollectors() throws Exception {
        record Case(IndexVersion version, NumberFieldMapper.NumberType type, boolean bucketedSort) {}
        List<Case> cases = new ArrayList<>();
        for (NumberFieldMapper.NumberType type : List.of(
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.INTEGER
        )) {
            cases.add(new Case(IndexVersions.UPGRADE_TO_LUCENE_10_0_0, type, false));
            cases.add(new Case(IndexVersions.INDEX_INT_SORT_INT_TYPE_8_19, type, true));
            cases.add(new Case(IndexVersions.INDEX_INT_SORT_INT_TYPE, type, true));
            cases.add(new Case(IndexVersion.current(), type, true));
        }
        for (NumberFieldMapper.NumberType type : List.of(
            NumberFieldMapper.NumberType.LONG,
            NumberFieldMapper.NumberType.FLOAT,
            NumberFieldMapper.NumberType.DOUBLE
        )) {
            cases.add(new Case(IndexVersions.UPGRADE_TO_LUCENE_10_0_0, type, true));
        }
        for (Case testCase : cases) {
            indexVersionCreated = testCase.version;
            try (Directory directory = newDirectory()) {
                try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                    iw.addDocument(numberDocument("0", "t0", testCase.type, numberValue(testCase.type, 1)));
                }
                try (IndexReader reader = DirectoryReader.open(directory)) {
                    assertEquals(
                        testCase.version + " " + testCase.type,
                        testCase.bucketedSort,
                        usesBucketedSort(reader, testCase.type, topHits("top").sort(NUMBER_FIELD, SortOrder.ASC))
                    );
                }
            }
        }
    }

    /**
     * What the Lucene collectors report for an integer field of an older index: a long sort declared, and longs, which is what
     * shards of the other indices are rewritten to when they are merged, with no exception, over several aggregators.
     */
    public void testIntegerFieldOfAnOlderIndexReportsLongSortValues() throws Exception {
        indexVersionCreated = IndexVersions.UPGRADE_TO_LUCENE_10_0_0;
        NumberFieldMapper.NumberType type = randomFrom(
            NumberFieldMapper.NumberType.BYTE,
            NumberFieldMapper.NumberType.SHORT,
            NumberFieldMapper.NumberType.INTEGER
        );
        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < 20; i++) {
                    iw.addDocument(numberDocument(Integer.toString(i), "t0", type, numberValue(type, i)));
                    if (i % 5 == 4) {
                        iw.commit();
                    }
                }
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                collectAndBuild(
                    reader,
                    boostedTermsQuery(1),
                    terms("term").field("string").size(1).subAggregation(topHits("top").sort(NUMBER_FIELD, SortOrder.ASC).size(3)),
                    built -> {
                        InternalTopHits partial = (InternalTopHits) ((Terms) built).getBucketByKey("t0").getAggregations().get("top");
                        TopFieldDocs topFieldDocs = (TopFieldDocs) partial.getTopDocs().topDocs;
                        assertEquals(SortField.Type.LONG, SortFieldValidation.getType(topFieldDocs.fields[0]));
                        for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
                            assertThat(((FieldDoc) scoreDoc).fields[0], instanceOf(Long.class));
                        }
                    },
                    STRING_FIELD_TYPE,
                    numberFieldType(type)
                );
                assertBucketedSortUsed(false);
                Terms terms = termsWithTopHits(reader, type, topHits("top").sort(NUMBER_FIELD, SortOrder.ASC).size(3), 1, true, false);
                List<String> ids = new ArrayList<>();
                for (SearchHit hit : ((TopHits) terms.getBucketByKey("t0").getAggregations().get("top")).getHits().getHits()) {
                    ids.add(hit.getId());
                }
                assertEquals(List.of("0", "1", "2"), ids);
            }
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

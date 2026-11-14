/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.DOC;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SEGMENT;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SHARD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests {@link LuceneSourceOperator.Factory#autoStrategy(long, int, boolean)}, the cost-aware source strategy.
 *
 * <p>A <b>no-limit</b> scan (e.g. {@code STATS}) visits every matching doc, so it shares the cost-threshold rule with
 * count and TopN: a scan-heavy doc-values filter → {@link LuceneSliceQueue.PartitioningStrategy#DOC}; a cheap indexed
 * lookup, an empty result, or the low-cost side of the threshold → {@link LuceneSliceQueue.PartitioningStrategy#SEGMENT};
 * a costly multi-term clause → SEGMENT; {@code MatchAll} → DOC. A point range or point set is SEGMENT while it matches fewer
 * than {@code minCostForDoc * taskConcurrency} docs in every segment, and DOC once one segment alone matches that many:
 * SEGMENT never cuts a segment, so a range that matches most of one big segment (a time window over recent data) would be
 * read by one driver. When the rows are wide, the plan loads many fields per doc, any costly clause is DOC whatever it
 * matches: SEGMENT sizes slices by segment docs, not by matches, so a selective filter leaves one driver with the work.
 *
 * <p>A <b>limited</b> scan (implicit {@code LIMIT}) keeps {@link LuceneSliceQueue.PartitioningStrategy#SHARD} for
 * costly-to-build clauses (BKD point ranges, multi-term), for indexed spatial queries (their scorers walk a BKD
 * tree, so a doc split repeats that walk), and for cheap / {@code MatchAll} queries that trivially early-terminate.
 * Only a scan-heavy non-costly filter (doc-values-only, cost ≥ threshold) is promoted to
 * {@link LuceneSliceQueue.PartitioningStrategy#DOC} because it visits ~maxDoc regardless of match density.
 * A no-limit indexed spatial query is not treated as costly and still follows the cost threshold.
 */
public class LuceneSourceOperatorCostAwareStrategyTests extends ESTestCase {

    private static final int NUM_DOCS = 200;
    private static final long MIN_COST_FOR_DOC = 10;
    /** A point range is promoted to DOC at {@code MIN_COST_FOR_DOC * TASK_CONCURRENCY} = 40 matching docs. */
    private static final int TASK_CONCURRENCY = 4;
    /** Puts the point range promotion threshold, {@code MIN_COST_FOR_DOC * 100}, above the {@link #NUM_DOCS} docs. */
    private static final int HIGH_TASK_CONCURRENCY = 100;

    private static final String GEO = "geo";
    private static final String GEO_DV = "geo_dv";
    /** Covers every indexed point, so a distance query estimates at or above {@link #MIN_COST_FOR_DOC}. */
    private static final double WORLD_RADIUS_METERS = 20_000_000d;

    private Directory directory;
    private IndexReader reader;
    private Directory geoDirectory;
    private IndexReader geoReader;

    @Before
    public void initIndex() throws Exception {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new KeywordField("kw", "v" + i, Field.Store.NO)); // unique -> a term matches a single doc
                doc.add(new SortedNumericDocValuesField("dv", i));         // doc-values only, no points -> scan
                doc.add(new LongPoint("pt", i));
                doc.add(new NumericDocValuesField("pt", i));
                writer.addDocument(doc);
            }
        }
        reader = DirectoryReader.open(directory);

        // Separate from the keyword/points index: one indexed point plus doc values, and a doc-values-only field.
        // Every point is the origin, inside a world-sized circle, so estimateDocCount is the doc count.
        geoDirectory = newDirectory();
        try (IndexWriter writer = new IndexWriter(geoDirectory, new IndexWriterConfig())) {
            for (int i = 0; i < NUM_DOCS; i++) {
                Document doc = new Document();
                doc.add(new LatLonPoint(GEO, 0, 0));
                doc.add(new LatLonDocValuesField(GEO, 0, 0));
                doc.add(new LatLonDocValuesField(GEO_DV, 0, 0));
                writer.addDocument(doc);
            }
        }
        geoReader = DirectoryReader.open(geoDirectory);
    }

    @After
    public void closeIndex() throws Exception {
        IOUtils.close(reader, directory, geoReader, geoDirectory);
    }

    // --- no-limit (STATS): the cost threshold applies, same as TopN/count ---

    public void testStatsDocValuesScanPicksDoc() throws IOException {
        assertThat(pick(SortedNumericDocValuesField.newSlowRangeQuery("dv", 0, NUM_DOCS / 2), NO_LIMIT), equalTo(DOC));
    }

    public void testStatsCheapIndexedTermPicksSegment() throws IOException {
        // one matching doc -> cost below the threshold -> not worth DOC's per-slice overhead.
        assertThat(pick(new TermQuery(new Term("kw", "v7")), NO_LIMIT), equalTo(SEGMENT));
    }

    public void testStatsMatchAllPicksDoc() throws IOException {
        assertThat(pick(Queries.ALL_DOCS_INSTANCE, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsMatchNonePicksSegment() throws IOException {
        assertThat(pick(Queries.NO_DOCS_INSTANCE, NO_LIMIT), equalTo(SEGMENT));
    }

    public void testStatsPointRangeMatchingEnoughDocsPicksDoc() throws IOException {
        Query query = nearlyAllDocsRange();
        assertCostlyAfterRewrite(reader, query);
        assertCostAtLeast(query, MIN_COST_FOR_DOC * TASK_CONCURRENCY);
        assertThat(pick(query, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsPointRangeMatchingFewDocsPicksSegment() throws IOException {
        // The same range, but with a task concurrency high enough that its matches cannot give every driver a minimum slice.
        Query query = nearlyAllDocsRange();
        assertCostlyAfterRewrite(reader, query);
        assertThat(pick(reader, query, NO_LIMIT, HIGH_TASK_CONCURRENCY), equalTo(SEGMENT));
    }

    public void testStatsPointRangeSpreadOverSegmentsPicksSegment() throws IOException {
        // Four segments of 50 docs, nearly all matching. The matches add up to 199, but no segment alone reaches the threshold of 60,
        // and SEGMENT can run the four segments in parallel.
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < NUM_DOCS; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint("pt", i));
                    doc.add(new NumericDocValuesField("pt", i));
                    writer.addDocument(doc);
                    if ((i + 1) % 50 == 0) {
                        writer.flush();
                    }
                }
            }
            try (IndexReader segmented = DirectoryReader.open(dir)) {
                assertThat(segmented.leaves().size(), equalTo(4));
                Query query = nearlyAllDocsRange();
                assertCostlyAfterRewrite(segmented, query);
                assertThat(pick(segmented, query, NO_LIMIT, 6), equalTo(SEGMENT));
                // With a threshold of 40, each segment has enough matches to be worth cutting.
                assertThat(pick(segmented, query, NO_LIMIT, 4), equalTo(DOC));
            }
        }
    }

    public void testStatsWideRowsWithFewMatchesPicksDoc() throws IOException {
        // A selective terms filter next to a point range, like a session filter over a time window: the matches are far
        // below the threshold, but each one is read with many fields, so a few matches are still a lot of work.
        Query query = new BooleanQuery.Builder().add(nearlyAllDocsRange(), BooleanClause.Occur.FILTER)
            .add(new WildcardQuery(new Term("kw", "v1?")), BooleanClause.Occur.FILTER)
            .build();
        assertThat(pick(reader, query, NO_LIMIT, HIGH_TASK_CONCURRENCY, false), equalTo(SEGMENT));
        assertThat(pick(reader, query, NO_LIMIT, HIGH_TASK_CONCURRENCY, true), equalTo(DOC));
    }

    public void testStatsWideRowsWithPointRangeFewMatchesPicksDoc() throws IOException {
        Query query = nearlyAllDocsRange();
        assertThat(pick(reader, query, NO_LIMIT, HIGH_TASK_CONCURRENCY, false), equalTo(SEGMENT));
        assertThat(pick(reader, query, NO_LIMIT, HIGH_TASK_CONCURRENCY, true), equalTo(DOC));
    }

    public void testStatsWideRowsWithoutCostlyClauseFollowsCostThreshold() throws IOException {
        // Wide rows only change what a costly clause does. A cheap indexed lookup still stays on SEGMENT.
        assertThat(pick(reader, new TermQuery(new Term("kw", "v7")), NO_LIMIT, TASK_CONCURRENCY, true), equalTo(SEGMENT));
        assertThat(pick(reader, Queries.NO_DOCS_INSTANCE, NO_LIMIT, TASK_CONCURRENCY, true), equalTo(SEGMENT));
    }

    public void testLimitedWideRowsPointRangeStillPicksShard() throws IOException {
        // A limit can fire early, so a wide-row scan keeps the low-overhead SHARD for a costly clause.
        assertThat(pick(reader, nearlyAllDocsRange(), between(1, 1000), TASK_CONCURRENCY, true), equalTo(SHARD));
    }

    public void testStatsPointSetMatchingEnoughDocsPicksDoc() throws IOException {
        Query query = LongPoint.newSetQuery("pt", LongStream.range(0, NUM_DOCS).toArray());
        assertCostlyAfterRewrite(reader, query);
        assertCostAtLeast(query, MIN_COST_FOR_DOC * TASK_CONCURRENCY);
        assertThat(pick(query, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsPointRangeWithMultiTermClausePicksSegment() throws IOException {
        // The point range alone matches enough docs for DOC, but a DOC slice would also rebuild the wildcard scorer.
        Query query = new BooleanQuery.Builder().add(nearlyAllDocsRange(), BooleanClause.Occur.FILTER)
            .add(new WildcardQuery(new Term("kw", "v*")), BooleanClause.Occur.FILTER)
            .build();
        assertThat(pick(query, NO_LIMIT), equalTo(SEGMENT));
    }

    // --- implicit limit: cost-aware, not always SHARD ---

    public void testLimitedDocValuesScanPicksDoc() throws IOException {
        // Doc-values-only filter (no BKD): cost ≈ maxDoc, so SHARD would scan ~maxDoc single-threaded.
        // The limit is unlikely to fire (very few matches), so parallelise with DOC instead.
        assertThat(pick(SortedNumericDocValuesField.newSlowRangeQuery("dv", 0, NUM_DOCS / 2), between(1, 1000)), equalTo(DOC));
    }

    public void testLimitedMatchAllPicksShard() throws IOException {
        // FROM foo | LIMIT N: MatchAll early-terminates immediately, keep low-overhead SHARD.
        assertThat(pick(Queries.ALL_DOCS_INSTANCE, between(1, 1000)), equalTo(SHARD));
    }

    public void testLimitedCheapTermPicksShard() throws IOException {
        // Term query: not costly to build, cost = 1 (below threshold) → SHARD.
        assertThat(pick(new TermQuery(new Term("kw", "v7")), between(1, 1000)), equalTo(SHARD));
    }

    public void testLimitedPointRangePicksShard() throws IOException {
        // BKD point range: costly to build, so keep SHARD — single shard-level BKD walk beats
        // one scorer-rebuild per segment when the limit fires early.
        assertThat(pick(LongPoint.newRangeQuery("pt", 0, NUM_DOCS / 2), between(1, 1000)), equalTo(SHARD));
    }

    // --- indexed spatial: limited scans stay SHARD; doc-values-only and no-limit do not ---

    public void testLimitedGeoDistancePicksShard() throws IOException {
        Query query = LatLonPoint.newDistanceQuery(GEO, 0, 0, WORLD_RADIUS_METERS);
        assertGeoCostAtLeastDocThreshold(query);
        assertThat(pick(geoReader, query, between(1, 1000)), equalTo(SHARD));
    }

    public void testLimitedGeoDistanceIndexOrDocValuesPicksShard() throws IOException {
        // Shape produced by GeoPointFieldMapper.geoShapeQuery when the field has points and doc values.
        Query query = new IndexOrDocValuesQuery(
            LatLonPoint.newDistanceQuery(GEO, 0, 0, WORLD_RADIUS_METERS),
            LatLonDocValuesField.newSlowDistanceQuery(GEO, 0, 0, WORLD_RADIUS_METERS)
        );
        assertGeoCostAtLeastDocThreshold(query);
        assertThat(pick(geoReader, query, between(1, 1000)), equalTo(SHARD));
    }

    public void testLimitedGeoDistanceAndDisjointPicksShard() throws IOException {
        // One indexed spatial clause is enough. The disjoint circle is elsewhere, so both clauses estimate
        // at or above the threshold; a low conjunction cost would stay SHARD even without the spatial check.
        Query query = new BooleanQuery.Builder().add(
            LatLonPoint.newDistanceQuery(GEO, 0, 0, WORLD_RADIUS_METERS),
            BooleanClause.Occur.FILTER
        )
            .add(
                LatLonPoint.newGeometryQuery(GEO, ShapeField.QueryRelation.DISJOINT, new Circle(50, 50, 1_000)),
                BooleanClause.Occur.FILTER
            )
            .build();
        assertGeoCostAtLeastDocThreshold(query);
        assertThat(pick(geoReader, query, between(1, 1000)), equalTo(SHARD));
    }

    public void testLimitedGeoDocValuesDistancePicksDoc() throws IOException {
        Query query = LatLonDocValuesField.newSlowDistanceQuery(GEO_DV, 0, 0, WORLD_RADIUS_METERS);
        assertGeoCostAtLeastDocThreshold(query);
        assertThat(pick(geoReader, query, between(1, 1000)), equalTo(DOC));
    }

    public void testStatsGeoDistancePicksDoc() throws IOException {
        // No limit: the spatial class check must not leak into isCostlyToBuildScorer (that would pick SEGMENT).
        Query query = LatLonPoint.newDistanceQuery(GEO, 0, 0, WORLD_RADIUS_METERS);
        assertGeoCostAtLeastDocThreshold(query);
        assertThat(pick(geoReader, query, NO_LIMIT), equalTo(DOC));
    }

    private static final int NO_LIMIT = LuceneOperator.NO_LIMIT;

    private LuceneSliceQueue.PartitioningStrategy pick(Query query, int limit) throws IOException {
        return pick(reader, query, limit);
    }

    private LuceneSliceQueue.PartitioningStrategy pick(IndexReader indexReader, Query query, int limit) throws IOException {
        return pick(indexReader, query, limit, TASK_CONCURRENCY);
    }

    private LuceneSliceQueue.PartitioningStrategy pick(IndexReader indexReader, Query query, int limit, int taskConcurrency)
        throws IOException {
        return pick(indexReader, query, limit, taskConcurrency, false);
    }

    private LuceneSliceQueue.PartitioningStrategy pick(IndexReader indexReader, Query query, int limit, int taskConcurrency, boolean wide)
        throws IOException {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(indexReader, 0);
        Query rewritten = new IndexSearcher(indexReader).rewrite(query);
        return LuceneSourceOperator.Factory.autoStrategy(MIN_COST_FOR_DOC, taskConcurrency, wide).pickStrategy(limit).apply(ctx, rewritten);
    }

    /**
     * A range over every doc but the first. Lucene rewrites a point range that covers every doc to match-all, which takes
     * DOC for a different reason than the one under test.
     */
    private static Query nearlyAllDocsRange() {
        return LongPoint.newRangeQuery("pt", 1, NUM_DOCS - 1);
    }

    private static void assertCostlyAfterRewrite(IndexReader indexReader, Query query) throws IOException {
        Query rewritten = new IndexSearcher(indexReader).rewrite(query);
        assertTrue("expected a point clause after rewrite: " + rewritten, LuceneSourceOperator.Factory.containsCostlyClause(rewritten));
    }

    private void assertCostAtLeast(Query query, long minCost) throws IOException {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Query rewritten = new IndexSearcher(reader).rewrite(query);
        assertThat(LuceneSourceOperator.Factory.queryCost(ctx, rewritten, Long.MAX_VALUE), greaterThanOrEqualTo(minCost));
    }

    private void assertGeoCostAtLeastDocThreshold(Query query) throws IOException {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(geoReader, 0);
        Query rewritten = new IndexSearcher(geoReader).rewrite(query);
        assertThat(LuceneSourceOperator.Factory.queryCost(ctx, rewritten, Long.MAX_VALUE), greaterThanOrEqualTo(MIN_COST_FOR_DOC));
    }
}

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
 * Tests {@link LuceneSourceOperator.Factory#autoStrategy(long, boolean, boolean)}, the cost-aware source strategy.
 *
 * <p>A <b>no-limit</b> scan (e.g. {@code STATS}) visits every matching doc, so it shares the cost-threshold rule with
 * count and TopN: a scan-heavy doc-values filter → {@link LuceneSliceQueue.PartitioningStrategy#DOC}; a cheap indexed
 * lookup, an empty result, or the low-cost side of the threshold → {@link LuceneSliceQueue.PartitioningStrategy#SEGMENT};
 * a costly multi-term clause → SEGMENT; {@code MatchAll} → DOC. A filter with a point range or point set is DOC once one
 * segment alone matches {@code minCostForDoc * POINT_RANGE_MIN_SLICES} docs for its required clauses, without counting
 * the multi-term ones, else SEGMENT, but only for narrow rows. With the opt-in for wide rows, any costly clause is DOC.
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
    private static final long MIN_COST_FOR_DOC = 5;
    /** A point range is DOC from this many matching docs in one segment. */
    private static final long POINT_RANGE_BAR = MIN_COST_FOR_DOC * LuceneSourceOperator.Factory.POINT_RANGE_MIN_SLICES;
    /** Puts the point range bar above the {@link #NUM_DOCS} docs. */
    private static final long HIGH_MIN_COST_FOR_DOC = 30;

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
        assertCostAtLeast(query, POINT_RANGE_BAR);
        assertThat(pick(query, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsPointRangeMatchingFewDocsPicksSegment() throws IOException {
        // The same range with a bar above the number of docs.
        Query query = nearlyAllDocsRange();
        assertCostlyAfterRewrite(reader, query);
        assertThat(pick(reader, query, NO_LIMIT, HIGH_MIN_COST_FOR_DOC), equalTo(SEGMENT));
    }

    public void testStatsPointRangeSpreadOverSegmentsPicksSegment() throws IOException {
        // Four segments of 50 docs, nearly all matching: 199 in total, but no segment alone reaches a bar of 60.
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
                // Bar of 60: no segment reaches it.
                assertThat(pick(segmented, query, NO_LIMIT, 6), equalTo(SEGMENT));
                // Bar of 40: each full segment reaches it.
                assertThat(pick(segmented, query, NO_LIMIT, 4), equalTo(DOC));
            }
        }
    }

    public void testStatsPointSetMatchingEnoughDocsPicksDoc() throws IOException {
        Query query = LongPoint.newSetQuery("pt", LongStream.range(0, NUM_DOCS).toArray());
        assertCostlyAfterRewrite(reader, query);
        assertCostAtLeast(query, POINT_RANGE_BAR);
        assertThat(pick(query, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsPointRangeWithMultiTermClausePicksDoc() throws IOException {
        // The wildcard is left out of the estimate, so the range that fills the segment decides.
        Query query = rangeAnd(new WildcardQuery(new Term("kw", "v*")));
        assertCostlyAfterRewrite(reader, query);
        assertThat(pick(query, NO_LIMIT), equalTo(DOC));
    }

    public void testStatsPointRangeWithMultiTermClauseBelowBarPicksSegment() throws IOException {
        Query query = rangeAnd(new WildcardQuery(new Term("kw", "v*")));
        assertThat(pick(reader, query, NO_LIMIT, HIGH_MIN_COST_FOR_DOC), equalTo(SEGMENT));
    }

    public void testStatsMultiTermWithoutPointClausePicksSegment() throws IOException {
        // No point clause to say where the matches are, so the costly multi-term clause keeps SEGMENT.
        Query query = new WildcardQuery(new Term("kw", "v*"));
        assertCostlyAfterRewrite(reader, query);
        assertThat(pick(query, NO_LIMIT), equalTo(SEGMENT));
    }

    public void testStatsPointRangeWithSelectiveTermPicksSegment() throws IOException {
        // The term matches one doc, so the required clauses together are far below the bar.
        Query query = rangeAnd(new TermQuery(new Term("kw", "v7")));
        assertCostlyAfterRewrite(reader, query);
        assertThat(pick(query, NO_LIMIT), equalTo(SEGMENT));
    }

    public void testStatsPointRangeThatIsNotRequiredPicksSegment() throws IOException {
        Query wildcard = new WildcardQuery(new Term("kw", "v*"));
        // an optional range: the wildcard alone can match, so the range says nothing about where the matches are
        Query should = new BooleanQuery.Builder().add(nearlyAllDocsRange(), BooleanClause.Occur.SHOULD)
            .add(wildcard, BooleanClause.Occur.SHOULD)
            .build();
        assertThat(pick(should, NO_LIMIT), equalTo(SEGMENT));
        // a negated range selects the docs outside of it
        Query mustNot = new BooleanQuery.Builder().add(nearlyAllDocsRange(), BooleanClause.Occur.MUST_NOT)
            .add(wildcard, BooleanClause.Occur.FILTER)
            .build();
        assertThat(pick(mustNot, NO_LIMIT), equalTo(SEGMENT));
    }

    public void testStatsWideRowsKeepSegmentUnlessOptedIn() throws IOException {
        // A range that fills the segment takes DOC for narrow rows, but wide rows pay too much CPU for it.
        Query range = nearlyAllDocsRange();
        assertThat(pick(reader, range, NO_LIMIT, MIN_COST_FOR_DOC, true, false), equalTo(DOC));
        assertThat(pick(reader, range, NO_LIMIT, MIN_COST_FOR_DOC, false, false), equalTo(SEGMENT));
        assertThat(
            pick(reader, rangeAnd(new WildcardQuery(new Term("kw", "v*"))), NO_LIMIT, MIN_COST_FOR_DOC, false, false),
            equalTo(SEGMENT)
        );
    }

    public void testStatsWideRowsOptInPicksDocForAnyCostlyClause() throws IOException {
        // The opt-in: a multi-term clause without a point clause, with few matches, but each match is a wide row.
        Query wildcard = new WildcardQuery(new Term("kw", "v1?"));
        assertThat(pick(reader, wildcard, NO_LIMIT, HIGH_MIN_COST_FOR_DOC, false, false), equalTo(SEGMENT));
        assertThat(pick(reader, wildcard, NO_LIMIT, HIGH_MIN_COST_FOR_DOC, false, true), equalTo(DOC));
        // and a point range that matches far fewer docs than the bar
        assertThat(pick(reader, nearlyAllDocsRange(), NO_LIMIT, HIGH_MIN_COST_FOR_DOC, false, true), equalTo(DOC));
    }

    public void testStatsWideRowsOptInWithoutCostlyClauseFollowsCostThreshold() throws IOException {
        // The opt-in only changes costly clauses: a cheap indexed lookup stays on SEGMENT.
        assertThat(pick(reader, new TermQuery(new Term("kw", "v7")), NO_LIMIT, MIN_COST_FOR_DOC, false, true), equalTo(SEGMENT));
        assertThat(pick(reader, Queries.NO_DOCS_INSTANCE, NO_LIMIT, MIN_COST_FOR_DOC, false, true), equalTo(SEGMENT));
    }

    public void testLimitedWideRowsOptInPointRangeStillPicksShard() throws IOException {
        // A limit can fire early, so SHARD stays.
        assertThat(pick(reader, nearlyAllDocsRange(), between(1, 1000), MIN_COST_FOR_DOC, false, true), equalTo(SHARD));
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
        return pick(indexReader, query, limit, MIN_COST_FOR_DOC);
    }

    private LuceneSliceQueue.PartitioningStrategy pick(IndexReader indexReader, Query query, int limit, long minCostForDoc)
        throws IOException {
        return pick(indexReader, query, limit, minCostForDoc, true, false);
    }

    private LuceneSliceQueue.PartitioningStrategy pick(
        IndexReader indexReader,
        Query query,
        int limit,
        long minCostForDoc,
        boolean narrowRows,
        boolean wideRowsOptIn
    ) throws IOException {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(indexReader, 0);
        Query rewritten = new IndexSearcher(indexReader).rewrite(query);
        return LuceneSourceOperator.Factory.autoStrategy(minCostForDoc, narrowRows, wideRowsOptIn)
            .pickStrategy(limit)
            .apply(ctx, rewritten);
    }

    /** Every doc but the first: a range over all docs would be rewritten to match-all, which takes DOC anyway. */
    private static Query nearlyAllDocsRange() {
        return LongPoint.newRangeQuery("pt", 1, NUM_DOCS - 1);
    }

    private static Query rangeAnd(Query other) {
        return new BooleanQuery.Builder().add(nearlyAllDocsRange(), BooleanClause.Occur.FILTER)
            .add(other, BooleanClause.Occur.FILTER)
            .build();
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

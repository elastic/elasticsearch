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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.DOC;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SEGMENT;
import static org.elasticsearch.compute.lucene.query.LuceneSliceQueue.PartitioningStrategy.SHARD;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link LuceneSourceOperator.Factory#autoStrategy(long)}, the cost-aware source strategy.
 *
 * <p>A <b>no-limit</b> scan (e.g. {@code STATS}) visits every matching doc, so it shares the cost-threshold rule with
 * count and TopN: a scan-heavy doc-values filter → {@link LuceneSliceQueue.PartitioningStrategy#DOC}; a cheap indexed
 * lookup, an empty result, or the low-cost side of the threshold → {@link LuceneSliceQueue.PartitioningStrategy#SEGMENT};
 * a costly point/multi-term clause → SEGMENT; {@code MatchAll} → DOC.
 *
 * <p>A <b>limited</b> scan (implicit {@code LIMIT}) early-terminates after matching N docs, so it is deferred and keeps
 * the low-overhead {@link LuceneSliceQueue.PartitioningStrategy#SHARD} regardless of the query.
 */
public class LuceneSourceOperatorCostAwareStrategyTests extends ESTestCase {

    private static final int NUM_DOCS = 200;
    private static final long MIN_COST_FOR_DOC = 10;

    private Directory directory;
    private IndexReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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
    }

    @Override
    public void tearDown() throws Exception {
        IOUtils.close(reader, directory);
        super.tearDown();
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

    public void testStatsPointRangePicksSegment() throws IOException {
        assertThat(pick(LongPoint.newRangeQuery("pt", 0, NUM_DOCS / 2), NO_LIMIT), equalTo(SEGMENT));
    }

    // --- implicit limit: deferred, always SHARD ---

    public void testLimitedScanPicksShard() throws IOException {
        assertThat(pick(SortedNumericDocValuesField.newSlowRangeQuery("dv", 0, NUM_DOCS / 2), between(1, 1000)), equalTo(SHARD));
        assertThat(pick(Queries.ALL_DOCS_INSTANCE, between(1, 1000)), equalTo(SHARD));
    }

    private static final int NO_LIMIT = LuceneOperator.NO_LIMIT;

    private LuceneSliceQueue.PartitioningStrategy pick(Query query, int limit) throws IOException {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Query rewritten = new IndexSearcher(reader).rewrite(query);
        return LuceneSourceOperator.Factory.autoStrategy(MIN_COST_FOR_DOC).pickStrategy(limit).apply(ctx, rewritten);
    }
}

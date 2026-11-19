/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StripeStatsSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentMap;

/**
 * What a read stages for a stripe its chunks each saw part of. A chunk is cut on buffer fill, never on the
 * stripe grid, so these cases drive the production harvester chunk by chunk — the geometry and the identity come
 * from {@link StripeStatsHarvester#emit}, not from hand-written maps — and check what reaches the capture sink:
 * one record per stripe covered, the same record whatever order the chunks finish in, separate records where the
 * chunks left a gap, and a wire form unchanged from the one a chunk publishes on its own.
 */
public class StripeStatsStagingTests extends ESTestCase {

    private static final String PATH = "memory://staging.ndjson";
    private static final long MTIME = 1_700_000_000_000L;
    private static final String FINGERPRINT = "cfg";
    private static final String READ_CONFIG = "read";
    /** The stripe grid the identity cases measure their first piece on; the grid case varies it on the second. */
    private static final long IDENTITY_STRIPE = 1024L;

    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "n", DataType.LONG, Nullability.TRUE, null, false),
        new ReferenceAttribute(Source.EMPTY, null, "k", DataType.KEYWORD, Nullability.TRUE, null, false)
    );

    /** One record of the fixture file: where it starts, and its two column values. */
    private record Row(long offset, @Nullable Long number, @Nullable BytesRef keyword) {}

    public void testTheChunksOfOneStripeStageOneRecord() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 512, row(0, 7L, "m"), row(16, 3L, "a"));
        chunk(staging, 1024, true, 512, 1024, row(512, 9L, "z"));

        List<Map<String, Object>> staged = staged(sink);
        assertEquals("the two chunks of one stripe stage one record", 1, staged.size());
        Map<String, Object> record = staged.get(0);
        assertEquals(3L, record.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals(0L, record.get(ExternalStats.STRIPE_ORDINAL_KEY));
        assertEquals("the record covers the stripe's whole cell", 0L, record.get(ExternalStats.COVERAGE_START_KEY));
        assertEquals(1024L, record.get(ExternalStats.COVERAGE_END_KEY));
        assertEquals(Boolean.TRUE, record.get(ExternalStats.STRIPE_AT_START_KEY));
        assertEquals(Boolean.TRUE, record.get(ExternalStats.STRIPE_AT_END_KEY));
        assertEquals("the file-final chunk's last stripe is terminal", Boolean.TRUE, record.get(ExternalStats.COVERAGE_IS_LAST_KEY));
        assertEquals(3L, SourceStatisticsSerializer.extractColumnMin(record, "n"));
        assertEquals(9L, SourceStatisticsSerializer.extractColumnMax(record, "n"));
        assertEquals(new BytesRef("a"), SourceStatisticsSerializer.extractColumnMin(record, "k"));
        assertEquals(new BytesRef("z"), SourceStatisticsSerializer.extractColumnMax(record, "k"));
        assertEquals((Long) 3L, SourceStatisticsSerializer.extractColumnValueCount(record, "k"));
    }

    /**
     * Chunks are parsed concurrently and finish in any order, so the record a stripe stages must not depend on
     * which chunk closed first — including the case where the piece that arrives last is the one that bridges
     * two pieces already held.
     */
    public void testPiecesJoinWhateverOrderTheChunksFinishIn() {
        List<Map<String, Object>> inOrder = stageThreeChunks(0, 1, 2);
        for (List<Integer> order : List.of(List.of(2, 1, 0), List.of(0, 2, 1), List.of(2, 0, 1), List.of(1, 0, 2))) {
            assertEquals(
                "the staged record must not depend on the order the chunks finished in: " + order,
                inOrder,
                stageThreeChunks(order.get(0), order.get(1), order.get(2))
            );
        }
    }

    /** Stages the same three chunks of one stripe in the given order and returns what reached the sink. */
    private List<Map<String, Object>> stageThreeChunks(int... order) {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        List<Runnable> chunks = List.of(
            () -> chunk(staging, 1024, false, 0, 300, row(0, 1L, "a")),
            () -> chunk(staging, 1024, false, 300, 700, row(300, 2L, "b")),
            () -> chunk(staging, 1024, true, 700, 1024, row(700, 3L, "c"))
        );
        for (int i : order) {
            chunks.get(i).run();
        }
        staging.flush();
        return staged(sink);
    }

    /**
     * The read covered two disjoint parts of the stripe and nothing in between. Claiming the cell would claim
     * coverage of bytes no chunk read, so the pieces stay separate and the coordinator's interval cover is left
     * to decide — which, for a stripe with a hole in it, is a safe miss.
     */
    public void testAGapLeavesThePiecesStagedSeparately() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 300, row(0, 1L, "a"));
        chunk(staging, 1024, false, 600, 900, row(600, 2L, "b"));

        assertEquals("neither piece covers the stripe, so neither is staged yet", List.of(), staged(sink));
        staging.flush();

        List<Map<String, Object>> staged = staged(sink);
        assertEquals("a read that left a gap stages what it did cover, piece by piece", 2, staged.size());
        for (Map<String, Object> piece : staged) {
            assertEquals(1L, piece.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertNotEquals(
                "a piece that stops short of the right grid line must not claim it",
                Boolean.TRUE,
                piece.get(ExternalStats.STRIPE_AT_END_KEY)
            );
        }
    }

    /** A stripe reaches the sink as soon as its cell is covered, so the staging area holds only live stripes. */
    public void testACoveredStripeIsStagedBeforeTheReadEnds() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 1024, row(0, 1L, "a"));

        assertEquals("the covered stripe is staged without waiting for the read to end", 1, staged(sink).size());
    }

    /**
     * Two reads of the same path at different file versions measured different files. Joining their pieces
     * would commit the mixture under one version's freshness key, so the identity gates the join exactly as it
     * gates the reconciler's covering chain.
     */
    public void testPiecesOfDifferentFileVersionsDoNotJoin() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, MTIME, 0, 512, row(0, 1L, "a"));
        chunk(staging, 1024, true, MTIME + 1, 512, 1024, row(512, 2L, "b"));
        staging.flush();

        assertEquals("pieces of two file versions stay two records", 2, staged(sink).size());
    }

    /**
     * "Harvested, and every row of it was null" and "never harvested" are different answers, and the
     * reconciler tells them apart by whether the column is in the record at all. A join must not drop an
     * all-null column, which would turn the first answer into the second.
     */
    public void testAnAllNullColumnStaysPresentInTheJoinedRecord() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 512, new Row(0, 1L, null));
        chunk(staging, 1024, true, 512, 1024, new Row(512, 2L, null));

        Map<String, Object> record = staged(sink).get(0);
        assertEquals(
            "an all-null column keeps its null count, which is what makes it present",
            (Long) 2L,
            SourceStatisticsSerializer.extractColumnNullCount(record, "k")
        );
        assertEquals("and its value count", (Long) 0L, SourceStatisticsSerializer.extractColumnValueCount(record, "k"));
        assertNull("with no extremum, because it has no values", SourceStatisticsSerializer.extractColumnMin(record, "k"));
    }

    /**
     * The same contribution, cheaper to hold. The property is the round trip: what a stream writes it must
     * read back equal to the map the same chunk would publish directly, which pins the length prefix against
     * the entries after it. Byte identity is deliberately not asserted -- the entries follow the statistics,
     * so order may differ, and nothing reads order because the far side rebuilds a {@code HashMap}.
     */
    public void testAStagedRecordCarriesWhatTheChunkWouldHavePublished() throws IOException {
        ConcurrentMap<String, List<Map<String, Object>>> stagedSink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(stagedSink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"), row(16, 5L, "q"));

        ConcurrentMap<String, List<Map<String, Object>>> directSink = ExternalStatsCapture.newSink();
        try (ExternalStatsCapture.Handle handle = ExternalStatsCapture.bind(directSink)) {
            chunk(null, 1024, true, 0, 1024, row(0, 1L, "a"), row(16, 5L, "q"));
        }

        assertEquals(1, staged(stagedSink).size());
        assertEquals(1, staged(directSink).size());

        // The wire FIRST, and from a record no lookup has touched. Writing one goes through the compact size()
        // and entrySet(); a kept flat form would answer both instead, so the comparison would not exercise the
        // path it exists to pin. The value equality below calls get() -- HashMap.equals probes the argument key
        // by key -- so it has to come after this, not before it.
        Map<String, Object> stagedRecord = staged(stagedSink).get(0);
        assertFalse(
            "the serialization path must be exercised from a record no lookup has made keep its flat form",
            ((StagedStripeRecord) stagedRecord).keepsFlatForm()
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericMap(stagedRecord);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(
                    "a staged record must read back as the contribution the chunk would have published",
                    staged(directSink).get(0),
                    in.readGenericMap()
                );
            }
        }
        assertFalse("and writing it must leave the record compact", ((StagedStripeRecord) stagedRecord).keepsFlatForm());

        assertEquals("the staged record must carry the same keys and values", staged(directSink).get(0), stagedRecord);
    }

    /**
     * The contract that makes staging cheap. A consumer walking the whole record once -- which is what
     * serialization does, asking the size and then the entries -- must leave nothing behind, because it is
     * walking every staged record in turn. Only looking a VALUE up keeps the map, because hundreds more
     * lookups follow it. Membership of an always-present key is answered without walking at all; see
     * {@link #testMembershipOfAnAlwaysPresentKeyBuildsNothing}.
     */
    public void testAWalkOverAStagedRecordKeepsNoFlatForm() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"));

        StagedStripeRecord record = (StagedStripeRecord) staged(sink).get(0);
        assertFalse("a stripe is staged in compact form", record.keepsFlatForm());

        int size = record.size();
        assertEquals("the record carries its addressing keys and both columns", 21, size);
        assertFalse("asking the size must leave the record compact", record.keepsFlatForm());

        int walked = 0;
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            assertNotNull(entry.getKey());
            walked++;
        }
        assertEquals("a walk sees every entry", size, walked);
        assertFalse("walking the entries must leave the record compact", record.keepsFlatForm());

        assertTrue("the record carries its addressing keys", record.containsKey(ExternalStats.STRIPE_ORDINAL_KEY));
        assertFalse("a membership test must leave the record compact: asking is not reading", record.keepsFlatForm());

        assertEquals("a lookup answers from the flat form", 1024L, record.get(ExternalStats.STRIPE_SIZE_KEY));
        assertTrue("and keeps it, because more lookups follow", record.keepsFlatForm());
    }

    /**
     * A write through the presented map throws rather than being silently dropped by the next rebuild. The map
     * is built on demand, so an accepted write would vanish and the caller would never know.
     */
    public void testAStagedRecordRefusesAWrite() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"));

        Map<String, Object> record = staged(sink).get(0);
        expectThrows(UnsupportedOperationException.class, () -> record.put("_stats.row_count", 99L));
        expectThrows(
            UnsupportedOperationException.class,
            () -> record.entrySet().iterator().next().setValue("written through the entry view")
        );
        expectThrows(UnsupportedOperationException.class, record::clear);
        assertEquals("the record is unchanged", 1L, record.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    /**
     * A column carrying one extremum and not the other. {@code ColumnStatsAccumulator} folds each value into
     * MIN and MAX together, so a read cannot produce this pair; the fixture builds the compact form directly,
     * which makes it a guard on the counting arithmetic rather than on the producing path.
     */
    public void testTheSizeAgreesWithTheBuiltMapForAOneSidedColumn() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(2);
        int column = builder.addColumn("k");
        builder.nullCount(column, 0);
        builder.valueCount(column, 2);
        builder.min(column, new BytesRef("a")); // and deliberately no max
        StagedStripeRecord record = new StagedStripeRecord(
            new StripeStatsSink.ReadIdentity(MTIME, FINGERPRINT, READ_CONFIG, true, 1024L),
            0L,
            0L,
            1024L,
            true,
            true,
            true,
            builder.build()
        );

        // Read the preconditions through a COPY. A lookup on the record would make it keep its flat form, and
        // size() would then answer from that map instead of from the compact count this test exists to check.
        Map<String, Object> entries = new HashMap<>(record);
        assertEquals("the fixture must carry one extremum", new BytesRef("a"), entries.get("_stats.columns.k.min"));
        assertNull("and not the other, or it guards nothing", entries.get("_stats.columns.k.max"));
        assertFalse("copying the entries must leave the record compact", record.keepsFlatForm());

        assertEquals("the counted size must equal the built map's", entries.size(), record.size());
    }

    /**
     * The empty shape, which the emit loop really does produce: a chunk owns the left edge of a stripe whose
     * first record lands in the next chunk, so that stripe is staged with no rows and no columns. It is the
     * one record whose entries are nothing but the addressing keys, so it is where a size counted from the
     * always-present set and entries written from the statistics would disagree with nothing to mask it.
     */
    public void testAStripePieceWithNoRowsCountsItsEntries() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        // One chunk over two stripes, every record in the first: stripe 1 is walked, anchored and empty.
        chunk(staging, 1024, true, 0, 2048, row(0, 1L, "a"));

        List<Map<String, Object>> staged = staged(sink);
        assertEquals("both stripes the chunk spans must stage", 2, staged.size());

        StagedStripeRecord empty = (StagedStripeRecord) staged.get(1);
        Map<String, Object> entries = new HashMap<>(empty);
        assertFalse("copying the entries must leave the record compact", empty.keepsFlatForm());
        assertEquals("the empty stripe must carry no rows", 0L, entries.get("_stats.row_count"));
        assertTrue(
            "and no column entries, or the fixture is not the empty shape",
            entries.keySet().stream().noneMatch(k -> k.startsWith("_stats.columns."))
        );
        assertEquals("the counted size must equal the built map's", entries.size(), empty.size());
    }

    /**
     * The other half of the gap case: pieces this read staged separately must still reconcile. A second read
     * supplies the bytes between them, and the coordinator's interval cover — which is the only thing that can
     * see both reads — chains all three into one stripe and folds the file's row count. Without the third piece
     * the stripe stays uncommitted, which is the control: the cover is what completes the stripe, not the
     * staging area.
     */
    public void testPiecesStagedSeparatelyStillReconcileOnceAReadBridgesThem() throws Exception {
        List<Map<String, Object>> gapped = new ArrayList<>(stageWithAGap());
        List<Map<String, Object>> bridge = stageTheBridge();

        assertEquals("the first read staged the two parts it covered", 2, gapped.size());
        assertEquals("the second read staged the part between them", 1, bridge.size());

        assertNull("a stripe with a hole in it commits nothing", reconciledRowCount(gapped));
        gapped.addAll(bridge);
        assertEquals("the bridged cover folds every piece's rows exactly once", (Long) 3L, reconciledRowCount(gapped));
    }

    /** A read that covered [0,300) and [600,1024) of stripe 0 and nothing between. */
    private List<Map<String, Object>> stageWithAGap() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, false, 0, 300, row(0, 1L, "a"));
        chunk(staging, 1024, true, 600, 1024, row(600, 3L, "c"));
        staging.flush();
        return staged(sink);
    }

    /** A second read of the same file version covering only the bytes the first read missed. */
    private List<Map<String, Object>> stageTheBridge() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, false, 300, 600, row(300, 2L, "b"));
        staging.flush();
        return staged(sink);
    }

    /**
     * The whole-file row count the production reconciler commits for {@code contributions}, or {@code null} when
     * it commits none. Drives {@code ExternalSourceCacheService} rather than re-deriving the cover, because the
     * cover is the thing under test.
     */
    @Nullable
    private static Long reconciledRowCount(List<Map<String, Object>> contributions) throws Exception {
        Settings settings = Settings.builder().put("esql.external.cache.size", "10mb").put("esql.external.cache.enabled", true).build();
        try (ExternalSourceCacheService service = new ExternalSourceCacheService(settings)) {
            SchemaCacheKey key = SchemaCacheKey.build(
                PATH,
                MTIME,
                TestDatasetIdentities.identity(".ndjson", "", Map.of("format", "ndjson")),
                false
            );
            service.getOrComputeSchema(
                key,
                k -> SchemaCacheEntry.from(
                    SCHEMA,
                    "ndjson",
                    PATH,
                    Map.of(ExternalStats.CONFIG_FINGERPRINT_KEY, FINGERPRINT, ExternalStats.READ_CONFIG_FINGERPRINT_KEY, READ_CONFIG),
                    Map.of()
                )
            );
            service.reconcileSourceStatsFromContributions(Map.of(PATH, contributions));
            Map<String, Object> statistics = service.getStatistics(StatisticsKey.of(key, READ_CONFIG));
            return statistics == null ? null : SourceStatisticsSerializer.extractRowCount(statistics);
        }
    }

    /**
     * The fold runs pairwise here — a piece absorbs a neighbour, then absorbs the next — where the reconciler
     * folds a stripe's whole covering chain in one call. Answers are byte-identical only if those agree, so
     * stage three pieces of one stripe and hold the joined record's statistics equal to merging the same three
     * pieces, published separately, through the reconciler's own entry point.
     */
    public void testThePairwiseJoinAgreesWithFoldingTheWholeChainAtOnce() {
        Map<String, Object> joined = stageThreeChunks(0, 1, 2).get(0);

        ConcurrentMap<String, List<Map<String, Object>>> direct = ExternalStatsCapture.newSink();
        try (ExternalStatsCapture.Handle handle = ExternalStatsCapture.bind(direct)) {
            chunk(null, 1024, false, 0, 300, row(0, 1L, "a"));
            chunk(null, 1024, false, 300, 700, row(300, 2L, "b"));
            chunk(null, 1024, true, 700, 1024, row(700, 3L, "c"));
        }
        List<Map<String, Object>> pieces = staged(direct);
        assertEquals("the three chunks must publish three pieces when nothing joins them", 3, pieces.size());

        Map<String, Object> chainFolded = SourceStatisticsSerializer.mergeStatistics(pieces, false);
        assertNotNull("the chain must fold", chainFolded);
        assertEquals(
            "pairwise joining must reach the statistics the whole-chain fold reaches",
            statistics(chainFolded),
            statistics(joined)
        );
    }

    /**
     * Byte identity where the fold actually runs. A joined record is one no single chunk would have published,
     * so the property that holds it is the round trip: what a stream writes it must read back equal, which
     * also pins the length prefix against the entries that follow it.
     */
    public void testAJoinedRecordSurvivesTheWireUnchanged() throws IOException {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, false, 0, 512, row(0, 7L, "m"), row(16, 3L, "a"));
        chunk(staging, 1024, true, 512, 1024, row(512, 9L, "z"));

        Map<String, Object> joined = staged(sink).get(0);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericMap(joined);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals("a joined record must read back exactly as it was written", joined, in.readGenericMap());
            }
        }
    }

    /**
     * The length a stream writes and the entries it then writes come from two different computations, so they
     * must agree on every shape a record takes: with and without a read configuration, with and without the
     * count licence, with columns that carry both extrema or neither, and with a column that carries exactly
     * one. {@code ColumnStatsAccumulator} folds each value into a column's MIN and MAX together, so the last of
     * those is reachable only because the extremum width limit withholds one end and not the other -- the shape
     * a URL column takes, a narrow lower bound against an upper bound too wide to publish.
     */
    public void testTheSizeAgreesWithTheBuiltMap() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"), new Row(16, null, null));
        chunk(staging, 2048, true, MTIME, null, false, 0, 2048, row(0, 5L, "q"));
        chunk(staging, 4096, true, MTIME, "", true, 0, 4096, new Row(0, null, null));
        // One bound narrow, the other over the width limit: the size is counted from the compact form while the
        // entries are built by flattening it, so an uneven pair is where the two computations can disagree.
        chunk(staging, 8192, true, 0, 8192, row(0, 9L, "a"), row(16, 11L, "z".repeat(TextFormatStats.MAX_EXTREMUM_BYTES + 1)));

        List<Map<String, Object>> records = staged(sink);
        assertEquals("each read staged its own stripe", 4, records.size());
        for (Map<String, Object> record : records) {
            assertEquals("the counted size must equal the built map's", new HashMap<>(record).size(), record.size());
            // Asserted rather than left to the copy being a walk: without it a later change that made the copy
            // pin the flat form would hollow this test out silently, because size() would then answer from the
            // kept map and agree with it for any arithmetic at all. The sibling one-sided test holds the same.
            assertFalse(
                "copying the entries must leave the record compact, or the size came from the kept map",
                ((StagedStripeRecord) record).keepsFlatForm()
            );
        }

        // Values read only now, after the size comparison: a lookup makes a record keep its flat form. This
        // shows the last record really is the one-sided shape rather than a column empty at both ends.
        Map<String, Object> oneSided = new HashMap<>(records.get(3));
        assertEquals("the narrow bound is published", new BytesRef("a"), SourceStatisticsSerializer.extractColumnMin(oneSided, "k"));
        assertNull(
            "the wide one is withheld, which is what makes this shape reachable at all",
            SourceStatisticsSerializer.extractColumnMax(oneSided, "k")
        );
    }

    /**
     * Every key of the contribution vocabulary: the ten {@link StagedStripeRecord} writes unconditionally, the
     * two it writes only when they are set, the three this path never writes at all, the row count, a per-column
     * statistic, and a name that is not vocabulary. The membership test below probes all of them.
     */
    private static final List<String> CONTRIBUTION_KEYS = List.of(
        ExternalStats.MTIME_MILLIS_KEY,
        ExternalStats.CONFIG_FINGERPRINT_KEY,
        ExternalStats.PARTIAL_CHUNK_KEY,
        ExternalStats.STRIPE_SIZE_KEY,
        ExternalStats.STRIPE_ORDINAL_KEY,
        ExternalStats.COVERAGE_START_KEY,
        ExternalStats.COVERAGE_END_KEY,
        ExternalStats.STRIPE_AT_START_KEY,
        ExternalStats.STRIPE_AT_END_KEY,
        ExternalStats.COVERAGE_IS_LAST_KEY,
        ExternalStats.READ_CONFIG_FINGERPRINT_KEY,
        ExternalStats.ROW_COUNT_READ_CONFIG_INDEPENDENT_KEY,
        ExternalStats.CHUNK_HAD_ERRORS_KEY,
        ExternalStats.STRIPE_LAST_INDEX_KEY,
        ExternalStats.STRIPE_GRID_KEY,
        SourceStatisticsSerializer.STATS_ROW_COUNT,
        "_stats.columns.n.min",
        "_stats.columns.k.max",
        "_stats.not_a_contribution_key"
    );

    /**
     * Membership key by key over the whole vocabulary, against the built map. The count test cannot catch a
     * SWAP -- one always-present key dropped and another added keeps the size the same -- so this holds the
     * identity of the set rather than its size.
     */
    public void testMembershipAgreesWithTheBuiltMap() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"));
        chunk(staging, 2048, true, MTIME, null, false, 0, 2048, row(0, 5L, "q"));
        chunk(staging, 4096, true, MTIME, "", true, 0, 4096, new Row(0, null, null));
        // One bound narrow, the other over the width limit: the size is counted from the compact form while the
        // entries are built by flattening it, so an uneven pair is where the two computations can disagree.
        chunk(staging, 8192, true, 0, 8192, row(0, 9L, "a"), row(16, 11L, "z".repeat(TextFormatStats.MAX_EXTREMUM_BYTES + 1)));

        List<Map<String, Object>> records = staged(sink);
        assertEquals("each read staged its own stripe", 4, records.size());
        for (Map<String, Object> record : records) {
            Map<String, Object> built = new HashMap<>(record);
            for (String key : CONTRIBUTION_KEYS) {
                assertEquals("membership of [" + key + "] must agree with the built map", built.containsKey(key), record.containsKey(key));
            }
            assertFalse("probing membership must leave the record compact", ((StagedStripeRecord) record).keepsFlatForm());
        }
    }

    /**
     * The other half of {@link StagedStripeRecord#containsKey}: an always-present key is answered without
     * building the flat form at all, not merely without keeping it. {@code keepsFlatForm} cannot see that — a
     * walk builds and discards, so it reports {@code false} either way — so the builds are counted directly,
     * by the record. Without the counter, deleting the early return leaves every answer correct and every other
     * assertion in this suite green while costing one build per probe, and {@code Status.stripesCommitted} runs
     * two probes per staged record on every {@code GET _tasks?detailed} during a large text scan.
     */
    public void testMembershipOfAnAlwaysPresentKeyBuildsNothing() {
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(1);
        int column = builder.addColumn("n");
        builder.nullCount(column, 0);
        builder.valueCount(column, 1);
        builder.min(column, 7L);
        builder.max(column, 7L);
        StagedStripeRecord record = new StagedStripeRecord(
            new StripeStatsSink.ReadIdentity(MTIME, FINGERPRINT, READ_CONFIG, true, 1024L),
            0L,
            0L,
            1024L,
            true,
            true,
            true,
            builder.build()
        );

        assertEquals("a staged record builds nothing until someone asks", 0, record.buildCount());
        record.size();
        assertEquals("the size comes from the compact count", 0, record.buildCount());
        assertTrue(record.containsKey(ExternalStats.STRIPE_ORDINAL_KEY));
        assertTrue(record.containsKey(ExternalStats.STRIPE_SIZE_KEY));
        assertEquals("membership of the keys every record carries must build nothing", 0, record.buildCount());

        // The control: a key the set does not carry has to walk, so the counter can tick at all and a zero
        // above is evidence about the early return rather than about a counter nothing ever increments.
        assertFalse("a key this path never writes must not be claimed", record.containsKey(ExternalStats.STRIPE_GRID_KEY));
        assertEquals("a key outside the set walks, which builds once", 1, record.buildCount());
        assertFalse("and the walk keeps nothing", record.keepsFlatForm());
    }

    /** The {@code _stats.*} statistics of a contribution, without the addressing keys a fold re-stamps. */
    private static Map<String, Object> statistics(Map<String, Object> contribution) {
        Map<String, Object> only = new HashMap<>();
        for (Map.Entry<String, Object> entry : contribution.entrySet()) {
            if (entry.getKey().equals(SourceStatisticsSerializer.STATS_ROW_COUNT)
                || entry.getKey().startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX)) {
                only.put(entry.getKey(), entry.getValue());
            }
        }
        return only;
    }

    /**
     * Scan invariance: the same stripe read as one chunk and read as several must stage the same record. Rows
     * are attributed by each record's own start offset, so a misaligned chunk boundary cannot move a row
     * between stripes, and the pieces fold back to one summary.
     */
    public void testAStripeStagesTheSameRecordHoweverTheReadWasChunked() throws IOException {
        ConcurrentMap<String, List<Map<String, Object>>> inPieces = ExternalStatsCapture.newSink();
        StripeStatsStaging joined = new StripeStatsStaging(inPieces);
        chunk(joined, 1024, false, 0, 300, row(0, 1L, "a"));
        chunk(joined, 1024, false, 300, 700, row(300, 2L, "b"));
        chunk(joined, 1024, true, 700, 1024, row(700, 3L, "c"));

        ConcurrentMap<String, List<Map<String, Object>>> wholeStripe = ExternalStatsCapture.newSink();
        StripeStatsStaging single = new StripeStatsStaging(wholeStripe);
        chunk(single, 1024, true, 0, 1024, row(0, 1L, "a"), row(300, 2L, "b"), row(700, 3L, "c"));

        assertEquals("one chunk covering the stripe stages one record", 1, staged(wholeStripe).size());
        assertEquals("three chunks covering the same stripe join into one record", 1, staged(inPieces).size());
        assertEquals(
            "a stripe read in pieces must stage what the same stripe read whole stages",
            staged(wholeStripe).get(0),
            staged(inPieces).get(0)
        );
        // Through the wire too, because that is the form the coordinator consumes. Equality of the maps does
        // not on its own say the length prefix a staged record writes matches the entries after it.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericMap(staged(inPieces).get(0));
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(
                    "and must read back as the whole-stripe read's record, prefix and entries agreeing",
                    staged(wholeStripe).get(0),
                    in.readGenericMap()
                );
            }
        }
    }

    /**
     * A column one piece summarised and the other did not is DROPPED from the joined record, not folded in as
     * all-null. Text harvests are partial, so an absent column means "not harvested" and the reconciler must
     * scan; calling it all-null would serve {@code COUNT(col) == rowCount - rowCount == 0} for a column that
     * may be entirely non-null. {@link SplitStats#fold} is passed {@code false} for exactly this.
     */
    public void testAColumnOnePieceDidNotSummariseIsDroppedRatherThanCalledNull() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 512, row(0, 1L, "a"));
        narrowChunk(staging, 1024, true, 512, 1024, 2L);

        Map<String, Object> record = staged(sink).get(0);
        assertEquals("the column both pieces summarised folds", (Long) 2L, SourceStatisticsSerializer.extractColumnValueCount(record, "n"));
        assertNull(
            "a column one piece never summarised must be dropped, not counted as null rows",
            SourceStatisticsSerializer.extractColumnNullCount(record, "k")
        );
        assertNull("and must offer no extremum", SourceStatisticsSerializer.extractColumnMin(record, "k"));
    }

    /** A chunk that summarises only the first schema column, leaving the second unmeasured for its stripe. */
    private void narrowChunk(StripeStatsStaging staging, long stripeSize, boolean fileFinal, long start, long end, long number) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(stripeSize, fileFinal, staging);
        StripeStatsHarvester.StripeAccum accum = harvester.getOrCreate(harvester.ordinalOf(start));
        accum.rows++;
        accum.cols = ColumnStatsAccumulator.forProjectedAttributes(new Attribute[] { SCHEMA.get(0) });
        accum.cols.acceptLongAt(0, number);
        harvester.emit(PATH, start, end - start, MTIME, FINGERPRINT, READ_CONFIG, true, SCHEMA);
    }

    /**
     * Two byte-adjacent pieces of one stripe join whatever schemas they were measured under, and the joined
     * record still reports the size its entry set yields.
     * <p>
     * The arriving piece has NO ROWS and the narrower schema, which is the shape that discriminates. A record
     * used to write its entries from the schema it was staged with and count them from the statistics; folding
     * a zero-row piece keeps the other piece's columns, so the surviving {@code k} was counted and never
     * written, and the map's length prefix is written from that count -- a corrupt stream, not a missing value.
     * Both now come from the statistics. This is also the one input whose published columns differ from the
     * pre-change behaviour, and the new answer is the right one: the empty piece contributes no rows, so the
     * other piece's summary of {@code k} covers every row in the stripe.
     */
    public void testAZeroRowPieceUnderANarrowerSchemaStillJoins() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        List<Attribute> both = SCHEMA;
        List<Attribute> onlyN = List.of(SCHEMA.get(0));
        assertNotEquals("the fixture must hand the two chunks schemas naming different columns", both, onlyN);

        chunk(staging, 1024, false, MTIME, 0, 512, READ_CONFIG, true, both, row(0, 7L, "m"));
        // No rows: this chunk owns the stripe's right edge and every record started in the previous one.
        chunk(staging, 1024, true, MTIME, 512, 1024, READ_CONFIG, true, onlyN);

        // Nothing is flushed on purpose: joined, the pair anchors both grid lines and stages at contribute
        // time. Held apart, neither piece covers the stripe and this sink would be empty.
        List<Map<String, Object>> staged = staged(sink);
        assertEquals("byte-adjacent pieces must fold into one record whatever schema each was measured under", 1, staged.size());

        StagedStripeRecord record = (StagedStripeRecord) staged.get(0);
        Map<String, Object> entries = new HashMap<>(record);
        assertFalse("copying the entries must leave the record compact", record.keepsFlatForm());
        assertEquals("the fold must carry the whole stripe's rows", 1L, entries.get("_stats.row_count"));
        assertEquals(
            "a column the zero-row piece did not name must survive the fold, since it contributed no rows",
            7L,
            entries.get("_stats.columns.n.min")
        );
        assertEquals("and so must the column outside the arriving schema", new BytesRef("m"), entries.get("_stats.columns.k.min"));
        assertEquals(
            "the counted size must equal the built map's, which is what the old shape could not hold",
            entries.size(),
            record.size()
        );
    }

    private static Row row(long offset, long number, String keyword) {
        return new Row(offset, number, new BytesRef(keyword));
    }

    private void chunk(@Nullable StripeStatsStaging staging, long stripeSize, boolean fileFinal, long start, long end, Row... rows) {
        chunk(staging, stripeSize, fileFinal, MTIME, start, end, rows);
    }

    /**
     * Drives the production harvester over one chunk covering {@code [start, end)}: attributes each row to its
     * canonical stripe by its own start offset, folds its two column values, and emits the chunk's cover.
     */
    private void chunk(
        @Nullable StripeStatsStaging staging,
        long stripeSize,
        boolean fileFinal,
        long mtime,
        @Nullable String readConfig,
        boolean licensed,
        long start,
        long end,
        Row... rows
    ) {
        chunk(staging, stripeSize, fileFinal, mtime, start, end, readConfig, licensed, rows);
    }

    private void chunk(
        @Nullable StripeStatsStaging staging,
        long stripeSize,
        boolean fileFinal,
        long mtime,
        long start,
        long end,
        Row... rows
    ) {
        chunk(staging, stripeSize, fileFinal, mtime, start, end, READ_CONFIG, true, rows);
    }

    private void chunk(
        @Nullable StripeStatsStaging staging,
        long stripeSize,
        boolean fileFinal,
        long mtime,
        long start,
        long end,
        @Nullable String readConfig,
        boolean licensed,
        Row... rows
    ) {
        chunk(staging, stripeSize, fileFinal, mtime, start, end, readConfig, licensed, SCHEMA, rows);
    }

    /**
     * As above, with the schema this chunk hands to {@code emit}. A reader can hand each chunk an equal but
     * DISTINCT list, so a test about sharing has to be able to pass two of them.
     */
    private void chunk(
        @Nullable StripeStatsStaging staging,
        long stripeSize,
        boolean fileFinal,
        long mtime,
        long start,
        long end,
        @Nullable String readConfig,
        boolean licensed,
        List<Attribute> schema,
        Row... rows
    ) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(stripeSize, fileFinal, staging);
        for (Row r : rows) {
            StripeStatsHarvester.StripeAccum accum = harvester.getOrCreate(harvester.ordinalOf(r.offset()));
            accum.rows++;
            if (accum.cols == null) {
                accum.cols = ColumnStatsAccumulator.forProjectedAttributes(schema.toArray(new Attribute[0]));
            }
            if (r.number() == null) {
                accum.cols.acceptNullAt(0);
            } else {
                accum.cols.acceptLongAt(0, r.number());
            }
            if (r.keyword() == null) {
                accum.cols.acceptNullAt(1);
            } else {
                accum.cols.acceptBytesRefAt(1, r.keyword());
            }
        }
        harvester.emit(PATH, start, end - start, mtime, FINGERPRINT, readConfig, licensed, schema);
    }

    /**
     * Pieces measured under different read identities never join, whichever field differs -- mtime, config
     * fingerprint, read config, count licence or stripe grid. Folding across any of them would mix statistics
     * taken of different bytes or under different rules into one record the coordinator reads as a single
     * stripe. Each case varies one field and holds the pair apart.
     */
    public void testPiecesMeasuredUnderADifferentIdentityDoNotJoin() {
        assertDoesNotJoin("a different file version", MTIME + 1, FINGERPRINT, READ_CONFIG, true, IDENTITY_STRIPE);
        assertDoesNotJoin("a different config fingerprint", MTIME, FINGERPRINT + "-other", READ_CONFIG, true, IDENTITY_STRIPE);
        assertDoesNotJoin("a different read configuration", MTIME, FINGERPRINT, READ_CONFIG + "-other", true, IDENTITY_STRIPE);
        assertDoesNotJoin("a different count licence", MTIME, FINGERPRINT, READ_CONFIG, false, IDENTITY_STRIPE);
        // The fifth component. Both pieces still land in ordinal 0 and still abut on byte 512, so the only thing
        // keeping them apart is the grid their ordinals are measured on: under a 2048-byte grid "stripe 0" is
        // [0,2048) and under the 1024-byte one it is [0,1024), and folding the two would attribute the second
        // piece's rows to a cell that does not contain them.
        assertDoesNotJoin("a different stripe grid", MTIME, FINGERPRINT, READ_CONFIG, true, IDENTITY_STRIPE * 2);

        // The positive control: identical identities DO join, so the five cases above fail for the identity
        // rather than because this geometry never joins at all.
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        identityChunk(staging, false, 0, 512, MTIME, FINGERPRINT, READ_CONFIG, true, IDENTITY_STRIPE, row(0, 1L, "a"));
        identityChunk(staging, true, 512, 1024, MTIME, FINGERPRINT, READ_CONFIG, true, IDENTITY_STRIPE, row(512, 2L, "b"));
        assertEquals("two pieces of one identity cover the stripe and stage once", 1, staged(sink).size());
    }

    /** Stages [0,512) under the read's own identity and [512,1024) under the given one, and expects no join. */
    private void assertDoesNotJoin(String what, long mtime, String fingerprint, String readConfig, boolean licensed, long stripeSize) {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        identityChunk(staging, false, 0, 512, MTIME, FINGERPRINT, READ_CONFIG, true, IDENTITY_STRIPE, row(0, 1L, "a"));
        identityChunk(staging, true, 512, 1024, mtime, fingerprint, readConfig, licensed, stripeSize, row(512, 2L, "b"));
        staging.flush();

        assertEquals("pieces measured under " + what + " must stay two records", 2, staged(sink).size());
    }

    /** One chunk covering {@code [start, end)}, harvested under an explicitly chosen read identity. */
    private void identityChunk(
        StripeStatsStaging staging,
        boolean fileFinal,
        long start,
        long end,
        long mtime,
        String fingerprint,
        String readConfig,
        boolean licensed,
        long stripeSize,
        Row... rows
    ) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(stripeSize, fileFinal, staging);
        for (Row r : rows) {
            StripeStatsHarvester.StripeAccum accum = harvester.getOrCreate(harvester.ordinalOf(r.offset()));
            accum.rows++;
            accum.cols = ColumnStatsAccumulator.forProjectedAttributes(SCHEMA.toArray(new Attribute[0]));
            accum.cols.acceptLongAt(0, r.number());
            accum.cols.acceptBytesRefAt(1, r.keyword());
        }
        harvester.emit(PATH, start, end - start, mtime, fingerprint, readConfig, licensed, SCHEMA);
    }

    /**
     * A covered stripe leaves nothing behind. The staging area's job is the stripes in flight, so a stripe it
     * has staged must drop out of it: holding the key with an empty list instead would make a clean whole-file
     * read accumulate one entry per stripe in the file for the life of the read, which is the shape of the pile
     * this change exists to remove, just a cheaper one per stripe.
     */
    public void testACoveredStripeIsNotLeftOpen() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 512, row(0, 1L, "a"));
        assertEquals("a piece short of its cell is held", 1, staging.openStripes());

        chunk(staging, 1024, true, 512, 1024, row(512, 2L, "b"));
        assertEquals("the stripe is staged once its cell is covered", 1, staged(sink).size());
        assertEquals("and the staging area holds nothing further for it", 0, staging.openStripes());
    }

    /**
     * {@link StripeStatsStaging#flush} is documented idempotent because a close path may run it more than once.
     * Nothing exercised that, so the claim was assertion rather than behaviour: a flush that published its
     * pieces without forgetting them would re-publish every one on the second call, double-counting the rows of
     * every stripe the read left partly covered.
     */
    public void testFlushingTwiceStagesEachPieceOnce() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);

        chunk(staging, 1024, false, 0, 512, row(0, 1L, "a"));
        staging.flush();
        assertEquals("the piece the read left uncovered is staged", 1, staged(sink).size());

        staging.flush();
        assertEquals("a second flush must stage nothing further", 1, staged(sink).size());
        assertEquals("and must leave the area empty", 0, staging.openStripes());
    }

    private static List<Map<String, Object>> staged(ConcurrentMap<String, List<Map<String, Object>>> sink) {
        List<Map<String, Object>> staged = sink.get(PATH);
        if (staged == null) {
            return List.of();
        }
        synchronized (staged) {
            return Collections.unmodifiableList(new ArrayList<>(staged));
        }
    }

    /**
     * The guard on the one thing that can shift underneath this: {@link SplitStats} can hold statistics a
     * staged record does not publish, and {@link StagedStripeRecord#size} counts a fixed vocabulary per column
     * while the entries are written from the statistics. Today both omit the per-column size, so they agree.
     * Teach the harvest to carry one, or add any per-column statistic to either side alone, and the count and
     * the entries part company -- which is a corrupt stream rather than a missing value, because the count is
     * written as the length prefix. So this holds the per-column vocabulary explicitly AND feeds a compact form
     * that does carry a size, so the count is checked against entries built from statistics richer than it
     * expects.
     */
    public void testTheRecordPublishesOneVocabularyPerColumn() {
        Map<String, Object> flat = statsMap(
            9L,
            Map.of("a", Map.of(".null_count", 2L, ".value_count", 7L, ".min", 1L, ".max", 9L, ".size_bytes", 128L))
        );
        SourceStatistics typed = SourceStatisticsSerializer.extractStatistics(flat).orElse(null);
        assertNotNull("the fixture must deserialize to typed statistics", typed);
        SplitStats withSize = StripeStatsStaging.compact(typed);
        assertNotNull(withSize);
        assertEquals("the fixture must carry a per-column size, or it guards nothing", 128L, withSize.columnSizeBytes("a"));

        StagedStripeRecord record = new StagedStripeRecord(
            new StripeStatsSink.ReadIdentity(MTIME, FINGERPRINT, READ_CONFIG, true, 1024L),
            0L,
            0L,
            1024L,
            true,
            true,
            true,
            withSize
        );

        Map<String, Object> entries = new HashMap<>(record);
        assertFalse("copying the entries must leave the record compact", record.keepsFlatForm());
        assertEquals("the counted size must equal the built map's", entries.size(), record.size());

        List<String> suffixes = entries.keySet()
            .stream()
            .filter(k -> k.startsWith(SourceStatisticsSerializer.STATS_COL_PREFIX))
            .map(k -> k.substring(k.lastIndexOf('.')))
            .sorted()
            .toList();
        assertEquals(
            "a staged record publishes exactly these per-column keys; changing the set means changing size()",
            List.of(".max", ".min", ".null_count", ".value_count"),
            suffixes
        );
    }

    /**
     * The compact form can be built from typed statistics or from the flat {@code _stats.*} map they serialize
     * as, and staging builds it the first way. The two must agree, or a staged record would carry different
     * statistics from the contribution a chunk publishes for the same stripe. Servability markers are not among
     * the shapes: neither route can carry one, so there is no claim to assert.
     */
    public void testCompactingTypedStatisticsMatchesTheFlatMapRoute() {
        for (Map<String, Object> flat : List.of(
            statsMap(100L, Map.of()),
            statsMap(100L, Map.of("a", Map.of(".null_count", 1L, ".value_count", 99L, ".min", 1L, ".max", 9L))),
            sized(statsMap(100L, Map.of("a", Map.of(".null_count", 0L, ".value_count", 100L, ".size_bytes", 512L))), 4096L),
            statsMap(0L, Map.of("a", Map.of(".null_count", 0L, ".value_count", 0L))),
            statsMap(
                7L,
                Map.of(
                    "a",
                    Map.of(".null_count", 2L, ".value_count", 5L, ".min", new BytesRef("a"), ".max", new BytesRef("z")),
                    "nested.b",
                    Map.of(".null_count", 0L, ".value_count", 7L, ".min", 1.5, ".max", 2.5, ".size_bytes", 64L)
                )
            )
        )) {
            SplitStats fromFlat = SplitStats.of(flat);
            assertNotNull("the fixture must parse", fromFlat);
            SourceStatistics typed = SourceStatisticsSerializer.extractStatistics(flat).orElse(null);
            assertNotNull("the fixture must deserialize to typed statistics", typed);
            SplitStats fromTyped = StripeStatsStaging.compact(typed);
            assertNotNull("typed statistics with a row count must compact", fromTyped);
            assertEquals("the two constructions must be key-equivalent", fromFlat.toMap(), fromTyped.toMap());
        }
    }

    /** Typed statistics without a row count are not cacheable, exactly as a flat map without one is not. */
    public void testCompactingStatisticsWithoutARowCountIsNothing() {
        assertNull(StripeStatsStaging.compact(null));
        assertNull("no row count, nothing to hold", StripeStatsStaging.compact(new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.empty();
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.empty();
            }
        }));
    }

    /** {@code flat} with a file-level size, the one statistic beside the row count rather than per column. */
    private static Map<String, Object> sized(Map<String, Object> flat, long sizeBytes) {
        flat.put(SourceStatisticsSerializer.STATS_SIZE_BYTES, sizeBytes);
        return flat;
    }

    /** A flat {@code _stats.*} map: a row count plus the given per-column statistics, keyed by suffix. */
    private static Map<String, Object> statsMap(long rowCount, Map<String, Map<String, Object>> columns) {
        Map<String, Object> flat = new HashMap<>();
        flat.put(SourceStatisticsSerializer.STATS_ROW_COUNT, rowCount);
        for (Map.Entry<String, Map<String, Object>> column : columns.entrySet()) {
            for (Map.Entry<String, Object> stat : column.getValue().entrySet()) {
                flat.put("_stats.columns." + column.getKey() + stat.getKey(), stat.getValue());
            }
        }
        return flat;
    }
}

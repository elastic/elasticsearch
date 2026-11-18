/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
     * The wire constraint: staging changes how many records a read publishes, never what one looks like on the
     * transport. A chunk that covers a whole stripe on its own must serialize to the same bytes whether it was
     * staged or published directly.
     */
    public void testAStagedRecordSerializesToTheBytesTheChunkWouldHavePublished() throws IOException {
        ConcurrentMap<String, List<Map<String, Object>>> stagedSink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(stagedSink);
        chunk(staging, 1024, true, 0, 1024, row(0, 1L, "a"), row(16, 5L, "q"));

        ConcurrentMap<String, List<Map<String, Object>>> directSink = ExternalStatsCapture.newSink();
        try (ExternalStatsCapture.Handle handle = ExternalStatsCapture.bind(directSink)) {
            chunk(null, 1024, true, 0, 1024, row(0, 1L, "a"), row(16, 5L, "q"));
        }

        assertEquals(1, staged(stagedSink).size());
        assertEquals(1, staged(directSink).size());
        assertEquals("the staged record must carry the same keys and values", staged(directSink).get(0), staged(stagedSink).get(0));
        assertArrayEquals(
            "and serialize to the same bytes, since the wire form is unchanged",
            wire(staged(directSink).get(0)),
            wire(staged(stagedSink).get(0))
        );
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
     * Scan invariance, stated rather than assumed. A stripe's content is defined to be independent of how the
     * read was chunked, so the record a stripe stages after absorbing several pieces must be the record it
     * would have staged from one piece covering the same bytes — down to the serialized bytes, since that is
     * the form the coordinator consumes.
     * <p>
     * This is the guarantee that makes the chunk count invisible downstream, and it is why an integration
     * test cannot tell a six-chunk read from a one-chunk one. Nothing else holds it: an
     * absorb path that made the joined record differ from the single-piece one would be a silent
     * cache-correctness bug rather than a failure.
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
        assertArrayEquals(
            "and must serialize to the same bytes, because that is the form the coordinator consumes",
            wire(staged(wholeStripe).get(0)),
            wire(staged(inPieces).get(0))
        );
    }

    /**
     * The absent-column policy the join folds under, pinned.
     * <p>
     * Text and footer formats disagree about a column one piece has and another lacks: a footer format treats
     * the absence as physical and folds that piece's rows into the column's null count, while a text harvest
     * treats it as "this piece did not summarise that column" and drops it so the serve re-scans. Folding text
     * pieces under the footer rule would answer {@code COUNT(col)} and {@code MIN}/{@code MAX} from rows
     * nothing measured.
     * <p>
     * No production text read reaches this shape today — every chunk builds its accumulator from the same
     * schema, so the pieces of one stripe carry the same column set, which is why mutating the policy breaks
     * no other test here. This constructs the shape deliberately so the choice is guarded rather than
     * incidental, and so a future reader that harvests only the columns it saw cannot quietly change the
     * answer.
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

    /** The bytes one contribution occupies on the transport, written exactly as {@code DriverCompletionInfo} writes it. */
    private static byte[] wire(Map<String, Object> contribution) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericMap(contribution);
            return BytesReference.toBytes(out.bytes());
        }
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
        long start,
        long end,
        Row... rows
    ) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(stripeSize, fileFinal, staging);
        for (Row r : rows) {
            StripeStatsHarvester.StripeAccum accum = harvester.getOrCreate(harvester.ordinalOf(r.offset()));
            accum.rows++;
            if (accum.cols == null) {
                accum.cols = ColumnStatsAccumulator.forProjectedAttributes(SCHEMA.toArray(new Attribute[0]));
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
        harvester.emit(PATH, start, end - start, mtime, FINGERPRINT, READ_CONFIG, true, SCHEMA);
    }

    /**
     * The join predicate, pinned field by field. Two pieces join only when they describe the same rows measured
     * the same way, and every component of {@code StripeStatsSink.ReadIdentity} is a reason they might not: a
     * different file version is a different file, a different config fingerprint or read configuration is a
     * different way of interpreting its bytes, a different licence is a different error policy, which may have
     * dropped rows from one piece and not the other, and a different stripe grid makes the two ordinals name
     * different byte ranges even when the number is the same. Joining across any of them would commit the
     * mixture under one identity's key — a wrong answer, not a missed one.
     * <p>
     * Each case below is byte-adjacent and would cover the stripe if it joined, so a predicate that ignores the
     * field under test stages one record where it must stage two. Weakening the predicate to any proper subset
     * of the identity fails here.
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
}

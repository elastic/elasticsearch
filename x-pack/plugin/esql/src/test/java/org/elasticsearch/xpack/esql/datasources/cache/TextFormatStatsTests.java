/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * What a text read publishes about a column's extrema. Every staged contribution and every stored record
 * carries a copy of both extrema of every harvested column, so one wide keyword value would set the size of all
 * of them; above a width limit the extremum is withheld and the column's {@code MIN}/{@code MAX} is answered by
 * a scan instead. These cases pin the limit's boundary, that the counts are untouched by it, and that a fold
 * over a read that withheld one marks the extremum unservable rather than answering from the reads that did
 * publish one.
 */
public class TextFormatStatsTests extends ESTestCase {

    private static final String PATH = "memory://extrema.ndjson";

    private static final List<Attribute> SCHEMA = List.of(
        attribute("n", DataType.LONG),
        attribute("k", DataType.KEYWORD),
        attribute("t", DataType.TEXT)
    );

    public void testAKeywordExtremumWiderThanTheLimitIsWithheldAndItsCountsSurvive() {
        BytesRef wide = keyword(TextFormatStats.MAX_EXTREMUM_BYTES + 1);

        Map<String, Object> published = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, wide, wide)));

        assertNull("a min wider than the limit is withheld", SourceStatisticsSerializer.extractColumnMin(published, "k"));
        assertNull("a max wider than the limit is withheld", SourceStatisticsSerializer.extractColumnMax(published, "k"));
        // Documents the encoding rather than gating it: embedStatistics has no path that writes a servability
        // marker for any input, because the ColumnStatistics SPI has no channel for one.
        assertNull(
            "withholding writes no unservable marker -- the column is simply unanswerable from statistics",
            published.get(SourceStatisticsSerializer.columnMinUnservableKey("k"))
        );
        assertEquals("the null count is unaffected", (Long) 0L, SourceStatisticsSerializer.extractColumnNullCount(published, "k"));
        assertEquals("the value count is unaffected", (Long) 1L, SourceStatisticsSerializer.extractColumnValueCount(published, "k"));
        assertEquals("the row count is unaffected", 1L, published.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    /**
     * The limit is bytes, not characters, and an ASCII fixture cannot tell the two apart. A value of
     * {@code MAX_EXTREMUM_BYTES / 2 + 1} two-byte characters is under the limit counted in characters and over
     * it counted in bytes, so it withholds only if the predicate reads {@link BytesRef#length}.
     */
    public void testTheLimitIsBytesNotCharacters() {
        int chars = TextFormatStats.MAX_EXTREMUM_BYTES / 2 + 1;
        BytesRef twoByteChars = new BytesRef("\u00e9".repeat(chars));
        assertEquals("the fixture must be over the limit in bytes", 2 * chars, twoByteChars.length);
        assertThat("and under it in characters", chars, lessThanOrEqualTo(TextFormatStats.MAX_EXTREMUM_BYTES));

        Map<String, Object> published = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, twoByteChars, twoByteChars)));

        assertNull(
            "a value over the byte limit is withheld however few characters it has",
            SourceStatisticsSerializer.extractColumnMin(published, "k")
        );
        assertNull(
            "a value over the byte limit is withheld however few characters it has",
            SourceStatisticsSerializer.extractColumnMax(published, "k")
        );
    }

    public void testAnExtremumAtTheWidthLimitIsPublished() {
        BytesRef widest = keyword(TextFormatStats.MAX_EXTREMUM_BYTES);

        Map<String, Object> published = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, widest, widest)));

        assertEquals("the widest publishable value is published", widest, SourceStatisticsSerializer.extractColumnMin(published, "k"));
        assertEquals("the widest publishable value is published", widest, SourceStatisticsSerializer.extractColumnMax(published, "k"));
    }

    /** The limit is a width, not a type: a narrow keyword beside a wide one in the same record keeps its extrema. */
    public void testTheLimitAppliesPerExtremumNotPerRead() {
        BytesRef narrow = keyword(8);
        BytesRef wide = keyword(TextFormatStats.MAX_EXTREMUM_BYTES + 1);

        Map<String, Object> published = publish(
            2,
            Map.of(
                "k",
                new ExternalStats.ColumnStats(0, 2, narrow, wide),
                "t",
                new ExternalStats.ColumnStats(0, 2, narrow, narrow),
                "n",
                new ExternalStats.ColumnStats(0, 2, 1L, 2L)
            )
        );

        assertEquals("a narrow min beside a wide max is published", narrow, SourceStatisticsSerializer.extractColumnMin(published, "k"));
        assertNull("only the wide extremum is withheld", SourceStatisticsSerializer.extractColumnMax(published, "k"));
        assertEquals("another column's extrema are untouched", narrow, SourceStatisticsSerializer.extractColumnMax(published, "t"));
        assertEquals("a number is never withheld, whatever its magnitude", 2L, SourceStatisticsSerializer.extractColumnMax(published, "n"));
    }

    /**
     * The degraded path: one read of a column published its extrema and another withheld them, and the widest
     * value the file holds is the withheld one. The fold must mark the extremum unservable rather than answer
     * from the read that published one, while the counts still fold.
     */
    public void testAFoldOverAWithheldExtremumIsUnservable() {
        BytesRef narrow = keyword(8);
        BytesRef wide = keyword(TextFormatStats.MAX_EXTREMUM_BYTES + 1);

        Map<String, Object> publishing = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, narrow, narrow)));
        Map<String, Object> withholding = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, wide, wide)));
        Map<String, Object> folded = SourceStatisticsSerializer.mergeStatistics(List.of(publishing, withholding), false);

        assertNotNull("the fold must produce statistics", folded);
        assertNull("the surviving extremum must not be served as the file's", SourceStatisticsSerializer.extractColumnMin(folded, "k"));
        assertNull("the surviving extremum must not be served as the file's", SourceStatisticsSerializer.extractColumnMax(folded, "k"));
        assertEquals(
            "the fold marks the extremum unservable so a sibling cannot refill it",
            Boolean.TRUE,
            folded.get(SourceStatisticsSerializer.columnMinUnservableKey("k"))
        );
        assertEquals(
            "the fold marks the extremum unservable so a sibling cannot refill it",
            Boolean.TRUE,
            folded.get(SourceStatisticsSerializer.columnMaxUnservableKey("k"))
        );
        assertEquals("the value count still folds", (Long) 2L, SourceStatisticsSerializer.extractColumnValueCount(folded, "k"));
        assertEquals("the row count still folds", 2L, folded.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    /**
     * The fold's poison is per extremum, not per column. One read of a column published a narrow minimum and
     * withheld an oversized maximum; the fold must still serve the minimum, folded against the other read's,
     * while marking only the maximum unservable. Without this case a fold that dropped a column's valid
     * minimum whenever its maximum was withheld would pass every other case here, because they withhold both
     * ends of the same column.
     */
    public void testTheFoldPoisonsOnlyTheWithheldEndOfAColumn() {
        BytesRef lower = new BytesRef("aaaa");
        BytesRef higher = new BytesRef("mmmm");
        BytesRef wide = keyword(TextFormatStats.MAX_EXTREMUM_BYTES + 1);

        Map<String, Object> oneSided = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, lower, wide)));
        Map<String, Object> bothEnds = publish(1, Map.of("k", new ExternalStats.ColumnStats(0, 1, higher, higher)));

        assertEquals(
            "the narrow end is published before the fold sees it",
            lower,
            SourceStatisticsSerializer.extractColumnMin(oneSided, "k")
        );
        assertNull("and the wide end is not", SourceStatisticsSerializer.extractColumnMax(oneSided, "k"));

        Map<String, Object> folded = SourceStatisticsSerializer.mergeStatistics(List.of(oneSided, bothEnds), false);

        assertNotNull("the fold must produce statistics", folded);
        assertEquals(
            "the minimum still serves, folded across both reads",
            lower,
            SourceStatisticsSerializer.extractColumnMin(folded, "k")
        );
        assertNull("the withheld end does not", SourceStatisticsSerializer.extractColumnMax(folded, "k"));
        assertNull(
            "and the end that served is not marked unservable",
            folded.get(SourceStatisticsSerializer.columnMinUnservableKey("k"))
        );
        assertEquals(
            "while the withheld end is",
            Boolean.TRUE,
            folded.get(SourceStatisticsSerializer.columnMaxUnservableKey("k"))
        );
        assertEquals("the counts still fold", (Long) 2L, SourceStatisticsSerializer.extractColumnValueCount(folded, "k"));
    }

    /**
     * The limit meeting the cross-chunk join. One chunk of a stripe saw a keyword too wide to publish, so the
     * widest value the stripe holds was withheld; the joined record must not answer that column's extrema from
     * the chunk that did publish one. The counts still fold, and so do the other column's extrema.
     */
    public void testAJoinedStripeDoesNotAnswerAWithheldExtremumFromAnotherChunk() {
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        StripeStatsStaging staging = new StripeStatsStaging(sink);
        List<Attribute> schema = List.of(attribute("n", DataType.LONG), attribute("k", DataType.KEYWORD));

        harvest(staging, schema, 0, 512, false, 1L, new BytesRef("a"));
        harvest(staging, schema, 512, 1024, true, 2L, keyword(TextFormatStats.MAX_EXTREMUM_BYTES + 1));

        Map<String, Object> record = sink.get(PATH).get(0);
        assertNull("the published extremum is not the stripe's", SourceStatisticsSerializer.extractColumnMax(record, "k"));
        assertNull("nor is the other end of it", SourceStatisticsSerializer.extractColumnMin(record, "k"));
        assertEquals("the counts still fold", (Long) 2L, SourceStatisticsSerializer.extractColumnValueCount(record, "k"));
        assertEquals("and so do the other column's extrema", 1L, SourceStatisticsSerializer.extractColumnMin(record, "n"));
        assertEquals(2L, SourceStatisticsSerializer.extractColumnMax(record, "n"));
    }

    /** Harvests one row into stripe 0 through the production harvester and emits the chunk {@code [start, end)}. */
    private static void harvest(
        StripeStatsStaging staging,
        List<Attribute> schema,
        long start,
        long end,
        boolean fileFinal,
        long number,
        BytesRef keyword
    ) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(1024, fileFinal, staging);
        StripeStatsHarvester.StripeAccum accum = harvester.getOrCreate(0);
        accum.rows++;
        accum.cols = ColumnStatsAccumulator.forProjectedAttributes(schema.toArray(new Attribute[0]));
        accum.cols.acceptLongAt(0, number);
        accum.cols.acceptBytesRefAt(1, keyword);
        harvester.emit(PATH, start, end - start, 1_700_000_000_000L, "cfg", "read", true, schema);
    }

    /** The flat {@code _stats.*} map a text read publishes for one record of {@code rowCount} rows. */
    private static Map<String, Object> publish(long rowCount, Map<String, ExternalStats.ColumnStats> columns) {
        ExternalStats.Stats stats = new ExternalStats.Stats(rowCount, OptionalLong.empty(), columns);
        return SourceStatisticsSerializer.embedStatistics(
            Map.of(),
            TextFormatStats.build(Optional.of(stats), OptionalLong.empty(), SCHEMA)
        );
    }

    private static BytesRef keyword(int bytes) {
        return new BytesRef("z".repeat(bytes));
    }

    private static Attribute attribute(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type, Nullability.TRUE, null, false);
    }
}

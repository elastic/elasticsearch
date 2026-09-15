/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.bwc.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.DocOffsetsCodec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.greaterThan;

/**
 * Covers the merge path that copies whole compressed binary doc values blocks from a source segment into the
 * target segment instead of decompressing every value and compressing it again.
 *
 * <p>The scenario each test builds is the one that makes splicing possible: an index sorted on
 * {@code host.name} where each segment owns a disjoint, interleaved set of hostnames. After merging, all the
 * documents of a hostname come from a single segment and stay next to each other, so a source block's documents
 * land consecutively in the target and the block can travel as bytes. The hostnames interleave across segments
 * on purpose — with segments that simply concatenate in sort order Lucene reports {@code needsIndexSort == false}
 * and the whole optimized merge path is skipped.
 *
 * <p>Whether a block was spliced or re-encoded leaves no trace on disk, so these tests read the merge's own debug
 * line for the count.
 */
@TestLogging(
    value = "org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer:DEBUG",
    reason = "the number of spliced blocks is only observable through the merge's debug logging"
)
public class BinaryDocValuesBlockSpliceTests extends ESTestCase {

    private static final String CONSUMER_LOGGER = "org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer";

    private static final String HOSTNAME_FIELD = "host.name";
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final String BINARY_FIELD = "binary_field";
    private static final long BASE_TIMESTAMP = 1704067200000L;

    /** Deliberately small so a handful of documents fills a block and the test stays quick. */
    private static final int BLOCK_BYTES_THRESHOLD = 16 * 1024;
    private static final int BLOCK_COUNT_THRESHOLD = 256;

    private static final int SEGMENTS = 2;
    private static final int HOSTS_PER_SEGMENT = 2;
    /** Four whole blocks per hostname, so every run is long enough to splice from. */
    private static final int DOCS_PER_HOST = 4 * BLOCK_COUNT_THRESHOLD;

    /** Which documents carry the binary field. */
    private enum Presence {
        /** Every document, so the field is dense everywhere. */
        ALL,
        /** Holes scattered through every segment, so no segment is dense. */
        HOLES_WITHIN_SEGMENT,
        /** Only the hostnames owned by even-numbered segments, so some segments are dense and others lack the field. */
        ONLY_SOME_SEGMENTS
    }

    public void testSplicesBlocksWhenRunsSpanWholeBlocks() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSegments(dir, es95Codec(), Presence.ALL, true);
            final int[] blocks = mergeAndCountSplicedBlocks(dir, es95Codec());
            // Every run here is four whole blocks long, so the merge should be splicing rather than re-encoding for
            // most of the field; a handful of blocks straddle run boundaries and legitimately have to be re-encoded.
            assertThat("spliced " + blocks[0] + " of " + blocks[1] + " blocks", blocks[0], greaterThan(blocks[1] / 2));
            assertValuesRoundTripped(dir, Presence.ALL);
        }
    }

    /**
     * When the segments already sit in sort order Lucene reports {@code needsIndexSort == false} and hands the merge a
     * plain concatenating doc map. Every source block's documents are then contiguous in the target by construction,
     * so the same contiguity check accepts all of them.
     */
    public void testSplicesBlocksWhenSegmentsConcatenate() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSegments(dir, es95Codec(), Presence.ALL, false);
            final int[] blocks = mergeAndCountSplicedBlocks(dir, es95Codec());
            assertThat("spliced " + blocks[0] + " of " + blocks[1] + " blocks", blocks[0], greaterThan(blocks[1] / 2));
            assertValuesRoundTripped(dir, Presence.ALL);
        }
    }

    /**
     * Blocks can still be spliced when the merged field ends up sparse because some segments do not carry it at all:
     * what matters is that the field is dense in the segment the block comes from. This is the path where a splice has
     * to report the block's documents to the doc-ids-with-values iterator itself, since it never runs the per-document
     * loop that normally does so.
     */
    public void testSplicesWhenOnlySomeSegmentsCarryTheField() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSegments(dir, es95Codec(), Presence.ONLY_SOME_SEGMENTS, true);
            final int[] blocks = mergeAndCountSplicedBlocks(dir, es95Codec());
            assertThat("spliced " + blocks[0] + " of " + blocks[1] + " blocks", blocks[0], greaterThan(blocks[1] / 2));
            assertValuesRoundTripped(dir, Presence.ONLY_SOME_SEGMENTS);
        }
    }

    /**
     * A sparse field still merges correctly, but cannot be spliced: with holes in the field a source doc id is no
     * longer its index into the value stream, which is the mapping the contiguity check relies on.
     */
    public void testDoesNotSpliceSparseField() throws IOException {
        try (Directory dir = newDirectory()) {
            writeSegments(dir, es95Codec(), Presence.HOLES_WITHIN_SEGMENT, true);
            final int[] blocks = mergeAndCountSplicedBlocks(dir, es95Codec());
            assertEquals(0, blocks[0]);
            assertThat(blocks[1], greaterThan(0));
            assertValuesRoundTripped(dir, Presence.HOLES_WITHIN_SEGMENT);
        }
    }

    /**
     * Blocks written by a format that encodes its doc offsets differently must not be spliced. Nothing inside a
     * block records which {@link DocOffsetsCodec} produced it, so copying one into a segment whose reader expects
     * the other encoding would corrupt it silently, with a perfectly valid checksum over the result.
     */
    public void testDoesNotSpliceAcrossDocOffsetsCodecs() throws IOException {
        try (Directory dir = newDirectory()) {
            // ES819 writes group-vint doc offsets; the ES95 target below reads bit-packed ones.
            writeSegments(dir, es819GroupVIntCodec(), Presence.ALL, true);
            final int[] blocks = mergeAndCountSplicedBlocks(dir, es95Codec());
            assertEquals(0, blocks[0]);
            assertThat(blocks[1], greaterThan(0));
            assertValuesRoundTripped(dir, Presence.ALL);
        }
    }

    private static Codec es95Codec() {
        return codecFor(
            new ES95TSDBDocValuesFormat(
                DEFAULT_SKIP_INDEX_INTERVAL,
                1,
                true,
                BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
                true,
                ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT,
                false,
                BLOCK_BYTES_THRESHOLD,
                BLOCK_COUNT_THRESHOLD,
                NumericCodecFactory.DEFAULT,
                ES95NumericFieldReader::defaultFallbackDecoder,
                null
            )
        );
    }

    private static Codec es819GroupVIntCodec() {
        return codecFor(
            new ES819TSDBDocValuesFormat(DEFAULT_SKIP_INDEX_INTERVAL, 1, true, BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1, true)
        );
    }

    private static final int DEFAULT_SKIP_INDEX_INTERVAL = 4096;

    private static Codec codecFor(DocValuesFormat docValuesFormat) {
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
    }

    private static IndexWriterConfig writerConfig(Codec codec) {
        var config = new IndexWriterConfig();
        config.setIndexSort(
            new Sort(
                new SortField(HOSTNAME_FIELD, SortField.Type.STRING, false),
                new SortedNumericSortField(TIMESTAMP_FIELD, SortField.Type.LONG, true)
            )
        );
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(codec);
        return config;
    }

    /**
     * Writes one segment per {@link #SEGMENTS}, giving segment {@code s} the hostnames congruent to {@code s}, so
     * that sorting by hostname alternates between segments while keeping each hostname's documents together, unless
     * {@code interleaveHosts} is false, in which case the segments are left already in sort order.
     */
    private static void writeSegments(Directory dir, Codec codec, Presence presence, boolean interleaveHosts) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(codec))) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int h = 0; h < HOSTS_PER_SEGMENT; h++) {
                    // Interleaved hostnames force a real merge sort; contiguous ones leave the segments already in
                    // sort order, which is the concatenating case.
                    final String host = hostName(interleaveHosts ? segment + SEGMENTS * h : segment * HOSTS_PER_SEGMENT + h);
                    for (int i = 0; i < DOCS_PER_HOST; i++) {
                        final long timestamp = BASE_TIMESTAMP + 1000L * i;
                        final Document doc = new Document();
                        doc.add(new SortedDocValuesField(HOSTNAME_FIELD, new BytesRef(host)));
                        doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, timestamp));
                        if (hasValue(presence, host, timestamp)) {
                            doc.add(new BinaryDocValuesField(BINARY_FIELD, new BytesRef(expectedValue(host, timestamp))));
                        }
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }
    }

    private static final Pattern SPLICED_BLOCKS = Pattern.compile(
        "spliced \\[(\\d+)\\] of \\[(\\d+)\\] binary doc values blocks for field \\[" + BINARY_FIELD + "\\]"
    );

    /**
     * Force merges everything into one segment and returns {@code {splicedBlocks, totalBlocks}} for the binary field,
     * read back out of the merge's own debug line since the choice leaves no trace on disk.
     */
    private static int[] mergeAndCountSplicedBlocks(Directory dir, Codec codec) throws IOException {
        final int[] counts = new int[] { -1, -1 };
        try (var mockLog = MockLog.capture(CONSUMER_LOGGER)) {
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    final Matcher matcher = SPLICED_BLOCKS.matcher(event.getMessage().getFormattedMessage());
                    if (matcher.matches()) {
                        counts[0] = Integer.parseInt(matcher.group(1));
                        counts[1] = Integer.parseInt(matcher.group(2));
                    }
                }

                @Override
                public void assertMatched() {}
            });
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(codec))) {
                writer.forceMerge(1);
            }
            mockLog.assertAllExpectationsMatched();
        }
        assertNotEquals("the merge never reported a block count for [" + BINARY_FIELD + "]", -1, counts[0]);
        return counts;
    }

    private static void assertValuesRoundTripped(Directory dir, Presence presence) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(1, reader.leaves().size());
            final LeafReader leaf = reader.leaves().get(0).reader();
            final var hostnames = leaf.getSortedDocValues(HOSTNAME_FIELD);
            final var timestamps = leaf.getSortedNumericDocValues(TIMESTAMP_FIELD);
            final var binaryValues = leaf.getBinaryDocValues(BINARY_FIELD);

            final int expectedDocs = SEGMENTS * HOSTS_PER_SEGMENT * DOCS_PER_HOST;
            assertEquals(expectedDocs, leaf.maxDoc());
            for (int doc = 0; doc < expectedDocs; doc++) {
                assertTrue(hostnames.advanceExact(doc));
                final String host = hostnames.lookupOrd(hostnames.ordValue()).utf8ToString();
                assertTrue(timestamps.advanceExact(doc));
                final long timestamp = timestamps.nextValue();

                if (hasValue(presence, host, timestamp)) {
                    assertTrue("expected a value for doc [" + doc + "]", binaryValues.advanceExact(doc));
                    assertEquals(expectedValue(host, timestamp), binaryValues.binaryValue().utf8ToString());
                } else {
                    assertFalse("expected no value for doc [" + doc + "]", binaryValues.advanceExact(doc));
                }
            }
        }
    }

    private static String hostName(int host) {
        return String.format(Locale.ROOT, "host-%03d", host);
    }

    private static boolean hasValue(Presence presence, String host, long timestamp) {
        return switch (presence) {
            case ALL -> true;
            // A hole roughly every eleventh document, so the field is sparse without being mostly empty.
            case HOLES_WITHIN_SEGMENT -> ((timestamp - BASE_TIMESTAMP) / 1000L) % 11 != 0;
            case ONLY_SOME_SEGMENTS -> hostIndex(host) % SEGMENTS == 0;
        };
    }

    private static int hostIndex(String host) {
        return Integer.parseInt(host.substring(host.lastIndexOf('-') + 1));
    }

    /** Values are derived from the document so verification needs no bookkeeping, and padded so blocks fill evenly. */
    private static String expectedValue(String host, long timestamp) {
        return String.format(Locale.ROOT, "%s-%013d-payload-padding", host, timestamp);
    }
}

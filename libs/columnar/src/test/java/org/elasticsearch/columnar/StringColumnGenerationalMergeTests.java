/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnLayout;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

// NOTE: reads only what a merged column exposes, never the decision behind it, so the same source runs on
// main and the numbers compare directly.
public class StringColumnGenerationalMergeTests extends ESTestCase {

    private static final String FIELD = "message";
    private static final int SEGMENTS_PER_GENERATION = 4;
    private static final int GENERATIONS = 8;
    private static final int POOL_TERMS = 6_000;
    private static final int POOL_TERM_PERCENT = 30;
    private static final int POOL_SHARE_PERCENT = 60;

    // NOTE: summing counts alone drops a term only one segment held, which is what the wider quota exists
    // to prevent.
    public void testATermHeldOnceReturnsInALaterGeneration() throws IOException {
        try (Directory dir = newDirectory()) {
            final Random random = new Random(11);
            int document = 0;
            for (int generation = 1; generation <= GENERATIONS; generation++) {
                document = flushRecurringGeneration(dir, random, document);
                forceMerge(dir);
                final Merged merged = report(dir, generation);
                if (generation == 1) {
                    assertFalse("nothing has repeated yet", merged.hasDictionary());
                    assertThat("but the terms are written down", merged.summaryTerms(), greaterThan(POOL_TERMS / 2));
                } else {
                    assertTrue("the terms written down then repeat and earn a dictionary", merged.hasDictionary());
                    assertThat("a value only one segment held is never named", merged.coverage(), lessThan(1.0));
                }
            }
        }
    }

    private static int flushRecurringGeneration(Directory dir, Random random, int document) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < SEGMENTS_PER_GENERATION; segment++) {
                final List<String> values = new ArrayList<>();
                for (int term = 0; term < POOL_TERMS; term++) {
                    if (random.nextInt(100) < POOL_TERM_PERCENT) {
                        values.add("/api/v2/checkout/session/pool-" + Integer.toString(term, 36));
                    }
                }
                final int tail = Math.max(0, values.size() * (100 - POOL_SHARE_PERCENT) / POOL_SHARE_PERCENT);
                for (int i = 0; i < tail; i++, document++) {
                    values.add("/api/v2/checkout/session/one-off-" + Integer.toString(document, 36));
                }
                Collections.shuffle(values, random);
                for (String value : values) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, payload(value), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        return document;
    }

    private record Merged(boolean hasDictionary, int dictionaryTerms, int summaryTerms, double coverage) {}

    private Merged report(Directory dir, int generation) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("force-merged to one segment", 1, reader.leaves().size());
            final SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
            final StringColumnReader column = stringColumn(segment);
            final List<BytesRef> terms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            if (column.hasSummary()) {
                column.readSummary(terms, counts);
            }
            final long named = column.hasDictionary() ? column.numValues() - column.escapeCount() : 0;
            logger.info(
                "generation={} values={} layout={} dictionaryTerms={} escapes={} coverage={} summaryTerms={} columnBytes={}",
                generation,
                column.numValues(),
                column.hasDictionary() ? StringColumnLayout.DICTIONARY : StringColumnLayout.PLAIN,
                column.hasDictionary() ? column.dictionarySize() : 0,
                column.escapeCount(),
                String.format(Locale.ROOT, "%.3f", (double) named / column.numValues()),
                terms.size(),
                columnarBytes(dir, segment)
            );
            return new Merged(
                column.hasDictionary(),
                column.hasDictionary() ? column.dictionarySize() : 0,
                terms.size(),
                (double) named / column.numValues()
            );
        }
    }

    private static long columnarBytes(Directory dir, SegmentReader segment) throws IOException {
        long bytes = 0;
        for (String file : dir.listAll()) {
            if (file.startsWith(segment.getSegmentName() + "_") && (file.endsWith(".cnd") || file.endsWith(".cnm"))) {
                bytes += dir.fileLength(file);
            }
        }
        return bytes;
    }

    private static void forceMerge(Directory dir) throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(SEGMENTS_PER_GENERATION + 1);
        mergePolicy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(mergePolicy))) {
            writer.forceMerge(1);
        }
    }

    private static IndexWriterConfig writerConfig(org.apache.lucene.index.MergePolicy mergePolicy) {
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (f, t) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptions.DEFAULT_DICTIONARY,
            StringColumnOptions.DEFAULT_SUMMARY
        );
        return new IndexWriterConfig().setCodec(columnarCodec(format)).setUseCompoundFile(false).setMergePolicy(mergePolicy);
    }

    private static BytesRef payload(String value) {
        final List<BytesRef> slots = new ArrayList<>(1);
        slots.add(new BytesRef(value));
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots));
    }

    private static StringColumnReader stringColumn(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + values, values instanceof ColumnarStringBinaryDocValues);
        return ((ColumnarStringBinaryDocValues) values).reader();
    }
}

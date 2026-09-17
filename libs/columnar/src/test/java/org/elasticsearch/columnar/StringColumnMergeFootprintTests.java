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
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;

public class StringColumnMergeFootprintTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String HEAD_TERM = "INFO";
    private static final int TAIL_TERMS = 1_000;
    private static final int TAIL_TERM_LENGTH = 32;
    private static final Set<String> COLUMNAR_EXTENSIONS = Set.of(
        ColumNARDocValuesFormat.DATA_EXTENSION,
        ColumNARDocValuesFormat.META_EXTENSION,
        ColumNARDocValuesFormat.SKIP_EXTENSION
    );

    private static final int SEGMENTS = 10;

    public void testTermsRepeatingAcrossSegmentsAreNamedAfterMerge() throws IOException {
        try (Directory dir = newDirectory()) {
            flushSegments(dir, tailTerms());
            assertSegmentsSummarizeOnlyTheHeadTerm(dir);
            forceMerge(dir);
            assertMergedColumnNamesEveryTerm(dir);
        }
    }

    // NOTE: every segment holds the head term once per tail term and each tail term exactly once, so no tail term repeats within a
    // segment while the merged column holds each of them once per segment.
    private static void flushSegments(Directory dir, String[] tail) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (String term : tail) {
                    addValue(writer, HEAD_TERM);
                    addValue(writer, term);
                }
                writer.commit();
            }
        }
    }

    private static void assertSegmentsSummarizeOnlyTheHeadTerm(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("segments before the merge", SEGMENTS, reader.leaves().size());
            for (LeafReaderContext leaf : reader.leaves()) {
                final StringColumnReader column = column(leaf.reader());
                assertFalse("the head term covers too few bytes for a segment dictionary", column.hasDictionary());
                final Summary summary = Summary.of(column);
                assertEquals("a tail term seen once is left out of the summary", List.of(new BytesRef(HEAD_TERM)), summary.terms());
                assertEquals(List.of((long) TAIL_TERMS), summary.counts());
            }
        }
    }

    private static void forceMerge(Directory dir) throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(mergePolicy))) {
            writer.forceMerge(1);
        }
    }

    private void assertMergedColumnNamesEveryTerm(Directory dir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("force-merged to one segment", 1, reader.leaves().size());
            final SegmentReader segment = (SegmentReader) reader.leaves().get(0).reader();
            final StringColumnReader column = column(segment);
            logger.info(
                "merged {} segments: dictionary={} terms={} escapes={} bytes={}",
                SEGMENTS,
                column.hasDictionary(),
                column.dictionarySize(),
                column.escapeCount(),
                columnarBytes(dir, segment)
            );
            assertTrue("a dictionary", column.hasDictionary());
            assertEquals("the head term and every tail term", TAIL_TERMS + 1, column.dictionarySize());
            assertEquals("no value escapes", 0, column.escapeCount());
        }
    }

    private static IndexWriterConfig writerConfig(MergePolicy mergePolicy) {
        return new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
            .setUseCompoundFile(false)
            .setMergePolicy(mergePolicy);
    }

    private static void addValue(IndexWriter writer, String value) throws IOException {
        final Document doc = new Document();
        doc.add(new Field(FIELD, stringPayload(value), columnarBinaryFieldType()));
        writer.addDocument(doc);
    }

    private static String[] tailTerms() {
        final Random random = new Random(42);
        final String[] terms = new String[TAIL_TERMS];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = randomHex(random, TAIL_TERM_LENGTH);
        }
        return terms;
    }

    private static String randomHex(Random random, int length) {
        final char[] hex = "0123456789abcdef".toCharArray();
        final char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = hex[random.nextInt(hex.length)];
        }
        return new String(chars);
    }

    private static StringColumnReader column(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        if (values instanceof ColumnarStringBinaryDocValues columnar) {
            return columnar.reader();
        }
        throw new AssertionError("expected ColumNAR doc values but got [" + values.getClass().getName() + "]");
    }

    private static long columnarBytes(Directory dir, SegmentReader segment) throws IOException {
        long bytes = 0;
        for (String file : segment.getSegmentInfo().files()) {
            if (COLUMNAR_EXTENSIONS.contains(IndexFileNames.getExtension(file))) {
                bytes += dir.fileLength(file);
            }
        }
        return bytes;
    }

    private record Summary(List<BytesRef> terms, List<Long> counts) {

        static Summary of(StringColumnReader column) throws IOException {
            final List<BytesRef> terms = new ArrayList<>();
            final List<Long> counts = new ArrayList<>();
            column.readSummary(terms, counts);
            return new Summary(terms, counts);
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.codec.PrefetchRecordingDirectory;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.lucene.store.PrefetchableDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/** The prefetch contract on TSDB columns, and the values read back. */
public class TSDBDocValuesPrefetchTests extends ESTestCase {

    private static final String NUMERIC = "numeric";
    private static final String BINARY = "binary";
    private static final String KEYWORD = "keyword";

    public void testNumeric() throws IOException {
        Corpus corpus = new Corpus(randomBoolean(), randomBoolean());
        try (Directory dir = newDirectory()) {
            corpus.index(dir, format());
            try (
                var recording = new PrefetchRecordingDirectory(dir, name -> name.endsWith(".dvd"));
                var reader = DirectoryReader.open(recording)
            ) {
                LeafReader leaf = getOnlyLeafReader(reader);
                assertBatches(recording, () -> leaf.getNumericDocValues(NUMERIC), corpus.numerics.keySet(), (column, docs) -> {
                    NumericDocValues values = (NumericDocValues) column;
                    for (int doc : docs) {
                        assertTrue(values.advanceExact(doc));
                        assertThat(values.longValue(), equalTo(corpus.numerics.get(doc)));
                    }
                }, 2);
            }
        }
    }

    public void testBinary() throws IOException {
        Corpus corpus = new Corpus(randomBoolean(), randomBoolean());
        // ES95 always compresses its binary columns.
        boolean es819 = randomBoolean();
        BinaryDVCompressionMode compression = es819
            ? randomFrom(BinaryDVCompressionMode.values())
            : BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1;
        try (Directory dir = newDirectory()) {
            corpus.index(dir, es819 ? new ES819TSDBDocValuesFormat(compression) : new ES95TSDBDocValuesFormat());
            try (
                var recording = new PrefetchRecordingDirectory(dir, name -> name.endsWith(".dvd"));
                var reader = DirectoryReader.open(recording)
            ) {
                LeafReader leaf = getOnlyLeafReader(reader);
                // The presence and the tables that locate values, then the values.
                int stages = corpus.dense && corpus.fixedLength && compression == BinaryDVCompressionMode.NO_COMPRESS ? 1 : 2;
                assertBatches(recording, () -> leaf.getBinaryDocValues(BINARY), corpus.binaries.keySet(), (opened, docs) -> {
                    BinaryDocValues values = (BinaryDocValues) opened;
                    for (int doc : docs) {
                        assertTrue(values.advanceExact(doc));
                        assertThat(values.binaryValue(), equalTo(corpus.binaries.get(doc)));
                    }
                }, stages);
            }
        }
    }

    public void testKeyword() throws IOException {
        Corpus corpus = new Corpus(randomBoolean(), randomBoolean());
        try (Directory dir = newDirectory()) {
            corpus.index(dir, format());
            try (
                var recording = new PrefetchRecordingDirectory(dir, name -> name.endsWith(".dvd"));
                var reader = DirectoryReader.open(recording)
            ) {
                LeafReader leaf = getOnlyLeafReader(reader);
                assertBatches(recording, () -> leaf.getSortedDocValues(KEYWORD), corpus.keywords.keySet(), (column, docs) -> {
                    SortedDocValues values = (SortedDocValues) column;
                    for (int doc : docs) {
                        assertTrue(values.advanceExact(doc));
                        assertThat(values.lookupOrd(values.ordValue()), equalTo(corpus.keywords.get(doc)));
                    }
                }, 3);
            }
        }
    }

    @FunctionalInterface
    private interface Read {
        void read(Object column, int[] docs) throws IOException;
    }

    /** Prefetches two batches one after the other on a column and reads each through another. */
    private void assertBatches(
        PrefetchRecordingDirectory recording,
        IOSupplier<Object> open,
        Set<Integer> withValue,
        Read read,
        int maxStages
    ) throws IOException {
        int[] docs = withValue.stream().mapToInt(Integer::intValue).filter(doc -> randomInt(9) == 0).sorted().toArray();
        if (docs.length == 0) {
            docs = new int[] { withValue.stream().mapToInt(Integer::intValue).min().getAsInt() };
        }
        int split = randomIntBetween(1, docs.length);
        int[] batch = docs;
        Object values = open.get();
        PrefetchableDocValues column = prefetchable(open.get());
        recording.reset();
        for (int[] range : new int[][] { { 0, split }, { split, docs.length } }) {
            int from = range[0];
            int to = range[1];
            if (from == to) {
                continue;
            }
            int ran = recording.assertReadsArePrefetched(
                stage -> column.prefetch(stage, batch, from, to),
                () -> read.read(values, Arrays.copyOfRange(batch, from, to))
            );
            assertThat(ran, lessThanOrEqualTo(maxStages));
        }
        recording.assertPrefetched();
    }

    private static PrefetchableDocValues prefetchable(Object values) {
        assertThat(values, instanceOf(PrefetchableDocValues.class));
        return (PrefetchableDocValues) values;
    }

    private static DocValuesFormat format() {
        return randomBoolean() ? new ES819TSDBDocValuesFormat() : new ES95TSDBDocValuesFormat();
    }

    /** The values indexed, by document, for the documents that hold one. */
    private static class Corpus {
        final boolean dense;
        final boolean fixedLength;
        final int numDocs = randomIntBetween(2_000, 6_000);
        final Map<Integer, Long> numerics = new HashMap<>();
        final Map<Integer, BytesRef> binaries = new HashMap<>();
        final Map<Integer, BytesRef> keywords = new HashMap<>();

        Corpus(boolean dense, boolean fixedLength) {
            this.dense = dense;
            this.fixedLength = fixedLength;
        }

        void index(Directory dir, DocValuesFormat format) throws IOException {
            BytesRef[] terms = IntStream.range(0, randomIntBetween(2, 300))
                .mapToObj(i -> new BytesRef(randomAlphaOfLength(randomIntBetween(4, 30))))
                .toArray(BytesRef[]::new);
            IndexWriterConfig config = new IndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(format)).setUseCompoundFile(false);
            config.getMergePolicy().setNoCFSRatio(0);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int doc = 0; doc < numDocs; doc++) {
                    Document document = new Document();
                    if (dense || randomInt(2) == 0) {
                        long value = randomLong();
                        numerics.put(doc, value);
                        document.add(new NumericDocValuesField(NUMERIC, value));
                    }
                    if (dense || randomInt(2) == 0) {
                        BytesRef value = new BytesRef(randomByteArrayOfLength(fixedLength ? 8 : randomIntBetween(1, 40)));
                        binaries.put(doc, value);
                        document.add(new BinaryDocValuesField(BINARY, value));
                    }
                    if (dense || randomInt(2) == 0) {
                        BytesRef value = randomFrom(terms);
                        keywords.put(doc, value);
                        document.add(new SortedDocValuesField(KEYWORD, value));
                    }
                    writer.addDocument(document);
                }
                writer.forceMerge(1);
            }
        }
    }
}

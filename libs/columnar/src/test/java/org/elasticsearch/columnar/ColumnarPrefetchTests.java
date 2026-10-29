/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.index.codec.PrefetchRecordingDirectory;
import org.elasticsearch.lucene.store.PrefetchableDocValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The prefetch contract on ColumNAR columns, on the data, addressing and lengths files; the others are read whole when the
 * segment opens.
 */
public class ColumnarPrefetchTests extends ESTestCase {

    private static final String LONG = "long";
    private static final String STRING = "string";

    public void testNumeric() throws IOException {
        boolean multiValued = randomBoolean();
        assertColumn(LONG, ColumnarFieldType.LONG, () -> {
            long[] values = new long[multiValued ? randomIntBetween(0, 4) : 1];
            for (int i = 0; i < values.length; i++) {
                values[i] = randomBoolean() ? randomLong() : randomIntBetween(0, 100);
            }
            return NumericBinaryPayload.encode(values, values.length, new BytesRefBuilder());
        });
    }

    /** A plain column whose values are located from its lengths: value blocks smaller than length blocks, or than chunks. */
    public void testPlainStringWithSmallBlocks() throws IOException {
        StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        StringColumnOptions options = new StringColumnOptions(
            DictionaryPolicy.NONE,
            SummaryPolicy.NONE,
            randomFrom(ChunkCodec.values()),
            new StringColumnOptions.Sizes(
                128,
                new ChunkBounds(randomFrom(256, 32 * 1024), 128),
                defaults.escapeChunks(),
                defaults.packedOrdinalBlockSize(),
                defaults.compressedOrdinalBlockSize(),
                defaults.escapeRankBlockSize(),
                defaults.slotCountsBlockSize(),
                randomFrom(256, 512, 1024)
            ),
            false,
            true
        );
        Codec codec = ColumnarTestUtils.columnarCodec(
            new ColumNARDocValuesFormat(
                (fieldName, type) -> NumericPipeline::defaultPipeline,
                field -> ColumnarFieldType.STRING,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                (fieldName, type) -> options
            )
        );
        boolean multiValued = randomBoolean();
        assertColumn(STRING, codec, () -> {
            List<BytesRef> slots = new ArrayList<>();
            for (int i = 0; i < (multiValued ? randomIntBetween(0, 4) : 1); i++) {
                slots.add(new BytesRef(randomAlphaOfLength(randomIntBetween(0, 40))));
            }
            return new StringBinaryPayload.Builder().encode(slots);
        });
    }

    public void testString() throws IOException {
        // A small vocabulary is written through a dictionary, unique values are stored as they are, and a mix of the
        // two has the rare values escape the dictionary.
        List<BytesRef> frequent = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 50); i++) {
            frequent.add(new BytesRef(randomAlphaOfLength(randomIntBetween(1, 20))));
        }
        double unique = randomFrom(0.0, 0.01, 1.0);
        boolean multiValued = randomBoolean();
        assertColumn(STRING, ColumnarFieldType.STRING, () -> {
            List<BytesRef> slots = new ArrayList<>();
            int count = multiValued ? randomIntBetween(0, 4) : 1;
            for (int i = 0; i < count; i++) {
                slots.add(randomDouble() < unique ? new BytesRef(randomAlphaOfLength(randomIntBetween(0, 40))) : randomFrom(frequent));
            }
            return new StringBinaryPayload.Builder().encode(slots);
        });
    }

    @FunctionalInterface
    private interface Value {
        BytesRef next();
    }

    private void assertColumn(String field, ColumnarFieldType type, Value value) throws IOException {
        assertColumn(field, ColumnarTestUtils.columnarCodec(type), value);
    }

    private void assertColumn(String field, Codec codec, Value value) throws IOException {
        boolean dense = randomBoolean();
        int numDocs = randomIntBetween(1_000, 20_000);
        Map<Integer, BytesRef> indexed = new HashMap<>();
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setUseCompoundFile(false);
            config.getMergePolicy().setNoCFSRatio(0);
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int doc = 0; doc < numDocs; doc++) {
                    Document document = new Document();
                    if (dense || randomInt(3) == 0) {
                        BytesRef payload = BytesRef.deepCopyOf(value.next());
                        indexed.put(doc, payload);
                        document.add(new BinaryDocValuesField(field, payload));
                    }
                    writer.addDocument(document);
                }
                writer.forceMerge(1);
            }
            if (indexed.isEmpty()) {
                return;
            }
            try (
                var recording = new PrefetchRecordingDirectory(
                    dir,
                    name -> name.endsWith(".cnd") || name.endsWith(".cna") || name.endsWith(".cnl")
                );
                var reader = DirectoryReader.open(recording)
            ) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BinaryDocValues values = leaf.getBinaryDocValues(field);
                assertThat(leaf.getBinaryDocValues(field), instanceOf(PrefetchableDocValues.class));
                PrefetchableDocValues column = (PrefetchableDocValues) leaf.getBinaryDocValues(field);

                int[] docs = indexed.keySet().stream().mapToInt(Integer::intValue).filter(doc -> randomInt(19) == 0).sorted().toArray();
                if (docs.length == 0) {
                    docs = new int[] { indexed.keySet().stream().mapToInt(Integer::intValue).min().getAsInt() };
                }
                int[] batch = docs;
                int split = randomIntBetween(1, docs.length);
                recording.reset();
                for (int[] range : new int[][] { { 0, split }, { split, docs.length } }) {
                    int from = range[0];
                    int to = range[1];
                    if (from == to) {
                        continue;
                    }
                    int stages = recording.assertReadsArePrefetched(stage -> column.prefetch(stage, batch, from, to), () -> {
                        for (int doc : Arrays.copyOfRange(batch, from, to)) {
                            assertTrue(values.advanceExact(doc));
                            assertThat(values.binaryValue(), equalTo(indexed.get(doc)));
                        }
                    });
                    // Presence and tables, addresses, then at most three stages of values: ordinals, terms and escapes.
                    assertThat(stages, lessThanOrEqualTo(5));
                }
                recording.assertPrefetched();
            }
        }
    }
}

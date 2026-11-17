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
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.lessThan;

/**
 * Navigation is read whole rather than a page at a time, so what it holds has to stay one entry per block of
 * values or per chunk, never one per value or per document. A table of either of those is a step change in
 * what a column costs to open, so the file is held to a fraction of a byte a value.
 */
public class ColumnarNavigationFootprintTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(ColumnarNavigationFootprintTests.class);

    /**
     * Half a bit a value. A table of one entry a value costs at least a bit an entry however well it packs,
     * so one landing in navigation breaks this however little its entries carry, while what navigation holds
     * today sits an order of magnitude below it.
     */
    private static final double MAX_BYTES_A_VALUE = 1.0 / Byte.SIZE / 2;

    private static final int DOCS = 100_000;

    /** Values of many lengths and few repeats, so the column stays plain and its lengths need their own table. */
    public void testPlainStrings() throws IOException {
        assertNavigationStaysPerBlock(doc -> new BytesRef("value-" + doc + "-" + randomAlphaOfLengthBetween(0, 40)));
    }

    /** A handful of terms, so the column is named by a dictionary and the navigation tables its ordinals instead. */
    public void testDictionaryStrings() throws IOException {
        final String[] terms = new String[between(4, 64)];
        for (int t = 0; t < terms.length; t++) {
            terms[t] = randomAlphaOfLengthBetween(1, 30);
        }
        assertNavigationStaysPerBlock(doc -> new BytesRef(randomFrom(terms)));
    }

    private void assertNavigationStaysPerBlock(java.util.function.IntFunction<BytesRef> value) throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            final long values = write(dir, value);
            long navigation = 0;
            long total = 0;
            for (String file : dir.listAll()) {
                final long length = dir.fileLength(file);
                total += length;
                if (file.endsWith(".cnn")) {
                    navigation += length;
                }
            }
            assertTrue("navigation holds more than its header", navigation > 0);
            final double bytesAValue = (double) navigation / values;
            logger.info(
                "values={} navigation={} total={} bytes_a_value={}",
                values,
                navigation,
                total,
                String.format(Locale.ROOT, "%.5f", bytesAValue)
            );
            assertThat("navigation bytes a value", bytesAValue, lessThan(MAX_BYTES_A_VALUE));
        }
    }

    /** One long and one to three string slots a document, with nulls, so every navigation table is written. */
    private long write(Directory dir, java.util.function.IntFunction<BytesRef> value) throws IOException {
        final FieldType type = columnarBinaryFieldType();
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            field -> field.name.equals("number") ? ColumnarFieldType.LONG : ColumnarFieldType.STRING
        );
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(format)).setUseCompoundFile(false);
        iwc.getMergePolicy().setNoCFSRatio(0.0);
        final BytesRefBuilder builder = new BytesRefBuilder();
        long values = 0;
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int d = 0; d < DOCS; d++) {
                final List<BytesRef> slots = new ArrayList<>();
                for (int s = between(1, 3); s > 0; s--) {
                    slots.add(randomInt(19) == 0 ? null : value.apply(d));
                }
                values += slots.size() + 1;
                final Document doc = new Document();
                doc.add(new Field("text", BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(slots)), type));
                doc.add(
                    new Field("number", BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { randomLong() }, 1, builder)), type)
                );
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return values;
    }
}

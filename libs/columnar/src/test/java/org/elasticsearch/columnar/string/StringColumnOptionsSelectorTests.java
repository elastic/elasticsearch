/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarFieldTypeSelector;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Two fields of the same values written into one segment, each under the options its own name asks for. What
 * a field is written as is a write-time choice the column records for itself, so the two are read back by
 * the same reader without it being told which is which.
 */
public class StringColumnOptionsSelectorTests extends ESTestCase {

    private static final String NAMED = "named_field";
    private static final String STORED = "stored_field";

    public void testTwoFieldsTakeDifferentLayouts() throws IOException {
        // Enough repetition that a dictionary is worth keeping, so only the options tell the two apart.
        final List<String> values = new ArrayList<>();
        for (int d = 0; d < 500; d++) {
            values.add("host-" + (d % 8));
        }
        final StringColumnOptionsSelector selector = (fieldName, type) -> fieldName.equals(NAMED)
            ? StringColumnOptions.DEFAULT
            : StringColumnOptions.DEFAULT.withDictionary(DictionaryPolicy.NONE);

        try (Directory dir = newDirectory()) {
            write(dir, selector, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertTrue("the field asking for one was not named by a dictionary", readerOf(leaf, NAMED).hasDictionary());
                assertFalse("the field asking for none was named by a dictionary", readerOf(leaf, STORED).hasDictionary());
                assertEquals("named field values", values, valuesOf(leaf, NAMED));
                assertEquals("stored field values", values, valuesOf(leaf, STORED));
            }
        }
    }

    /** A field naming its own codec is read back through the codec its chunk index records, not a default. */
    public void testAFieldMayChooseItsChunkCodec() throws IOException {
        final List<String> values = new ArrayList<>();
        for (int d = 0; d < 500; d++) {
            values.add("host-" + (d % 8));
        }
        final StringColumnOptionsSelector selector = (fieldName, type) -> new StringColumnOptions(
            StringColumnOptions.DEFAULT_DICTIONARY,
            fieldName.equals(NAMED) ? ChunkCodec.ZSTD : ChunkCodec.IDENTITY,
            StringColumnOptions.DEFAULT_TARGET_CHUNK_BYTES
        );

        try (Directory dir = newDirectory()) {
            write(dir, selector, values);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals("compressed field values", values, valuesOf(leaf, NAMED));
                assertEquals("verbatim field values", values, valuesOf(leaf, STORED));
            }
        }
    }

    public void testOptionsRejectWhatWouldNotRoundTrip() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new StringColumnOptions(null, ChunkCodec.ZSTD, StringColumnOptions.DEFAULT_TARGET_CHUNK_BYTES)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new StringColumnOptions(StringColumnOptions.DEFAULT_DICTIONARY, null, StringColumnOptions.DEFAULT_TARGET_CHUNK_BYTES)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new StringColumnOptions(StringColumnOptions.DEFAULT_DICTIONARY, ChunkCodec.ZSTD, 0)
        );
    }

    private static void write(Directory dir, StringColumnOptionsSelector selector, List<String> values) throws IOException {
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            (ColumnarFieldTypeSelector) ColumnarFieldType::fromField,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            selector
        );
        final FieldType type = columnarBinaryFieldType(ColumnarFieldType.STRING);
        final BytesRefBuilder builder = new BytesRefBuilder();
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec(format)))) {
            for (String value : values) {
                final Document doc = new Document();
                builder.copyChars(value);
                doc.add(new Field(NAMED, BytesRef.deepCopyOf(builder.get()), type));
                doc.add(new Field(STORED, BytesRef.deepCopyOf(builder.get()), type));
                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    private static StringColumnReader readerOf(LeafReader leaf, String field) throws IOException {
        return ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(field)).reader();
    }

    private static List<String> valuesOf(LeafReader leaf, String field) throws IOException {
        final ColumnarStringBinaryDocValues values = (ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(field);
        final List<String> read = new ArrayList<>();
        for (int doc = values.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            read.add(values.binaryValue().utf8ToString());
        }
        return read;
    }
}

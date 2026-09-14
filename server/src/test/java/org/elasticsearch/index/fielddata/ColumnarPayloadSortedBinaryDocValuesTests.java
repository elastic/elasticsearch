/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.instanceOf;

/**
 * What a column says about its own shape, which is what lets a single value check rewrite itself away
 * rather than ask every document.
 */
public class ColumnarPayloadSortedBinaryDocValuesTests extends ESTestCase {

    private static final String FIELD = "kw";

    public void testOneSlotADocumentIsSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { "b" }, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
            assertEquals(SortedBinaryDocValues.Sparsity.DENSE, values.getSparsity());
        });
    }

    /** A null slot is no value, and single valued says at most one, so a column holding them still qualifies. */
    public void testANullSlotIsStillSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { null }, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
        });
    }

    public void testASecondSlotIsNotSingleValued() throws IOException {
        withColumn(new String[][] { { "a" }, { "b", "c" } }, values -> {
            assertNotEquals(SortedBinaryDocValues.ValueMode.SINGLE_VALUED, values.getValueMode());
        });
    }

    /** A document the field is absent from leaves the column short of the segment, which is sparse. */
    public void testAColumnMissingDocumentsIsSparse() throws IOException {
        withColumn(new String[][] { { "a" }, null, { "c" } }, values -> {
            assertEquals(SortedBinaryDocValues.Sparsity.SPARSE, values.getSparsity());
        });
    }

    private interface Check {
        void check(SortedBinaryDocValues values) throws IOException;
    }

    /** {@code null} in place of a document's slots writes no field for it at all. */
    private void withColumn(String[][] docs, Check check) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String[] slots : docs) {
                    final Document doc = new Document();
                    if (slots != null) {
                        doc.add(new Field(FIELD, encode(slots), binaryDocValuesType()));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertThat("the field is a column", leaf.getBinaryDocValues(FIELD), instanceOf(StringColumnSource.class));
                check.check(ColumnarPayloadSortedBinaryDocValues.from(leaf, FIELD));
            }
        }
    }

    private static BytesRef encode(String[] slots) {
        final BytesRef[] refs = new BytesRef[slots.length];
        for (int i = 0; i < slots.length; i++) {
            refs[i] = slots[i] == null ? null : new BytesRef(slots[i]);
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(Arrays.asList(refs)));
    }

    private static FieldType binaryDocValuesType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        return type;
    }

    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }
}

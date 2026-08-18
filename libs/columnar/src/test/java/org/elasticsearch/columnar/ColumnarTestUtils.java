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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

/** Shared test helpers for ColumNAR unit tests. */
public final class ColumnarTestUtils {

    private ColumnarTestUtils() {}

    /** Returns a random valid block size: a power of 2 in [{@code 128}, {@code 8192}]. */
    public static int randomValidBlockSize() {
        return 128 << randomIntBetween(0, 6);
    }

    /**
     * Returns a single-valued cursor over {@code values}. Each document holds exactly one value;
     * {@link NumericColumnValues#advance} is fully implemented.
     */
    public static NumericColumnValues singleValuedCursor(final long[] values) {
        return new NumericColumnValues() {
            private int doc = -1;

            @Override
            public int valueCount() {
                return 1;
            }

            @Override
            public long nextValue() {
                return values[doc];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                return doc = target >= values.length ? DocIdSetIterator.NO_MORE_DOCS : target;
            }

            @Override
            public long cost() {
                return values.length;
            }
        };
    }

    /**
     * Returns a {@link Codec} that routes all doc-values fields through a default
     * {@link ColumNARDocValuesFormat}.
     */
    public static Codec columnarCodec() {
        return columnarCodec(new ColumNARDocValuesFormat());
    }

    /**
     * Returns a {@link Codec} that routes all doc-values fields through {@code fmt}.
     */
    public static Codec columnarCodec(final DocValuesFormat fmt) {
        final Codec base = TestUtil.getDefaultCodec();
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return fmt;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }

    /** Returns a frozen {@link FieldType} for a {@code BINARY} doc-values field tagged as {@code fieldType}. */
    public static FieldType columnarBinaryFieldType(final ColumnarFieldType fieldType) {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, fieldType.name());
        type.freeze();
        return type;
    }

    /**
     * Opens {@code fileName} from {@code dir}, reads and validates the ColumNAR meta header, reads
     * the {@link NumericColumnMetadata}, validates the footer, and returns the metadata.
     */
    public static NumericColumnMetadata readNumericMeta(
        final Directory dir,
        final String fileName,
        final byte[] segmentId,
        final int maxDoc
    ) throws IOException {
        try (ChecksumIndexInput meta = dir.openChecksumInput(fileName)) {
            final FormatVersion version = ColumnarCodecUtil.checkHeader(meta, ColumNARDocValuesFormat.META_CODEC, segmentId, "");
            final NumericColumnMetadata metadata = NumericColumnMetadata.readFrom(meta, maxDoc, version);
            ColumnarCodecUtil.checkFooter(meta);
            return metadata;
        }
    }
}

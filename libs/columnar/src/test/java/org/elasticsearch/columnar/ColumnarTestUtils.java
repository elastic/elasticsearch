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
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
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
     * Returns a single-valued cursor over {@code values} where a {@code null} entry means the document
     * has no value, producing a sparse column. Documents that do have one hold exactly one.
     */
    public static NumericColumnValues sparseSingleValuedCursor(final Long[] values) {
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
                for (doc = target; doc < values.length; doc++) {
                    if (values[doc] != null) {
                        return doc;
                    }
                }
                return doc = DocIdSetIterator.NO_MORE_DOCS;
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
    /**
     * The columnar format for {@code field} and the default for everything else, for a test that needs a
     * companion field the columnar format does not write, such as one to sort the index on.
     */
    public static Codec columnarCodecForField(final String field) {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat();
        final DocValuesFormat fallback = new Lucene90DocValuesFormat();
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String name) {
                    return name.equals(field) ? columnar : fallback;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }

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

    /**
     * Hides the columnar instance behind a plain {@link BinaryDocValues}, leaving only the surface every
     * binary doc values has.
     */
    public static DirectoryReader hideTheColumn(DirectoryReader in) throws IOException {
        return new FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader leaf) {
                return new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String name) throws IOException {
                        final BinaryDocValues values = in.leaves().get(0).reader().getBinaryDocValues(name);
                        return values == null ? null : new BinaryDocValues() {
                            @Override
                            public BytesRef binaryValue() throws IOException {
                                return values.binaryValue();
                            }

                            @Override
                            public boolean advanceExact(int target) throws IOException {
                                return values.advanceExact(target);
                            }

                            @Override
                            public int docID() {
                                return values.docID();
                            }

                            @Override
                            public int nextDoc() throws IOException {
                                return values.nextDoc();
                            }

                            @Override
                            public int advance(int target) throws IOException {
                                return values.advance(target);
                            }

                            @Override
                            public long cost() {
                                return values.cost();
                            }
                        };
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return leaf.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return leaf.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader reader) {
                return reader;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }
}

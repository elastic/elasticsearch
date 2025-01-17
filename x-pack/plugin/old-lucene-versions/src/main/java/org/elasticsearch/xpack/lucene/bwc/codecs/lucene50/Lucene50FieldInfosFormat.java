/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Lucene 5.0 Field Infos format.
 * @deprecated
 */
@Deprecated
public final class Lucene50FieldInfosFormat extends FieldInfosFormat {

    public Lucene50FieldInfosFormat() {}

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        try (ChecksumIndexInput input = EndiannessReverserUtil.openChecksumInput(directory, fileName, context)) {
            Throwable priorE = null;
            FieldInfo infos[] = null;
            try {
                CodecUtil.checkIndexHeader(
                    input,
                    Lucene50FieldInfosFormat.CODEC_NAME,
                    Lucene50FieldInfosFormat.FORMAT_START,
                    Lucene50FieldInfosFormat.FORMAT_CURRENT,
                    segmentInfo.getId(),
                    segmentSuffix
                );

                final int size = input.readVInt(); // read in the size
                infos = new FieldInfo[size];

                // previous field's attribute map, we share when possible:
                Map<String, String> lastAttributes = Collections.emptyMap();

                for (int i = 0; i < size; i++) {
                    String name = input.readString();
                    final int fieldNumber = input.readVInt();
                    if (fieldNumber < 0) {
                        throw new CorruptIndexException("invalid field number for field: " + name + ", fieldNumber=" + fieldNumber, input);
                    }
                    byte bits = input.readByte();
                    boolean storeTermVector = (bits & STORE_TERMVECTOR) != 0;
                    boolean omitNorms = (bits & OMIT_NORMS) != 0;
                    boolean storePayloads = (bits & STORE_PAYLOADS) != 0;

                    final IndexOptions indexOptions = getIndexOptions(input, input.readByte());

                    // DV Types are packed in one byte
                    final DocValuesType docValuesType = getDocValuesType(input, input.readByte());
                    final long dvGen = input.readLong();
                    Map<String, String> attributes = input.readMapOfStrings();

                    // just use the last field's map if its the same
                    if (attributes.equals(lastAttributes)) {
                        attributes = lastAttributes;
                    }
                    lastAttributes = attributes;
                    try {
                        infos[i] = new FieldInfo(
                            name,
                            fieldNumber,
                            storeTermVector,
                            omitNorms,
                            storePayloads,
                            indexOptions,
                            docValuesType,
                            DocValuesSkipIndexType.NONE,
                            dvGen,
                            attributes,
                            0,
                            0,
                            0,
                            0,
                            VectorEncoding.FLOAT32,
                            VectorSimilarityFunction.EUCLIDEAN,
                            false,
                            false
                        );
                        infos[i].checkConsistency();
                    } catch (IllegalStateException e) {
                        throw new CorruptIndexException("invalid fieldinfo for field: " + name + ", fieldNumber=" + fieldNumber, input, e);
                    }
                }
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(input, priorE);
            }
            return new FieldInfos(infos);
        }
    }

    private static DocValuesType getDocValuesType(IndexInput input, byte b) throws IOException {
        return switch (b) {
            case 0 -> DocValuesType.NONE;
            case 1 -> DocValuesType.NUMERIC;
            case 2 -> DocValuesType.BINARY;
            case 3 -> DocValuesType.SORTED;
            case 4 -> DocValuesType.SORTED_SET;
            case 5 -> DocValuesType.SORTED_NUMERIC;
            default -> throw new CorruptIndexException("invalid docvalues byte: " + b, input);
        };
    }

    private static IndexOptions getIndexOptions(IndexInput input, byte b) throws IOException {
        return switch (b) {
            case 0 -> IndexOptions.NONE;
            case 1 -> IndexOptions.DOCS;
            case 2 -> IndexOptions.DOCS_AND_FREQS;
            case 3 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
            case 4 -> IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
            default ->
                // BUG
                throw new CorruptIndexException("invalid IndexOptions byte: " + b, input);
        };
    }

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) {
        throw new UnsupportedOperationException();
    }

    /** Extension of field infos */
    static final String EXTENSION = "fnm";

    // Codec header
    static final String CODEC_NAME = "Lucene50FieldInfos";
    static final int FORMAT_SAFE_MAPS = 1;
    static final int FORMAT_START = FORMAT_SAFE_MAPS;
    static final int FORMAT_CURRENT = FORMAT_SAFE_MAPS;

    // Field flags
    static final byte STORE_TERMVECTOR = 0x1;
    static final byte OMIT_NORMS = 0x2;
    static final byte STORE_PAYLOADS = 0x4;
}

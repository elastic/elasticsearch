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
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FlatVectorsReader;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads Scalar Quantized vectors from the index segments along with index data structures.
 */
@SuppressForbidden(reason = "copy from Lucene")
public final class ESLucene99ScalarQuantizedVectorsReader extends FlatVectorsReader implements ESQuantizedVectorsReader {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ESLucene99ScalarQuantizedVectorsFormat.class);

    private final Map<String, FieldEntry> fields = new HashMap<>();
    private final IndexInput quantizedVectorData;
    private final FlatVectorsReader rawVectorsReader;

    ESLucene99ScalarQuantizedVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        this.rawVectorsReader = rawVectorsReader;
        int versionMeta = -1;
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ESLucene99ScalarQuantizedVectorsFormat.META_EXTENSION
        );
        boolean success = false;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName, state.context)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    ESLucene99ScalarQuantizedVectorsFormat.META_CODEC_NAME,
                    ESLucene99ScalarQuantizedVectorsFormat.VERSION_START,
                    ESLucene99ScalarQuantizedVectorsFormat.VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(meta, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
            quantizedVectorData = openDataInput(
                state,
                versionMeta,
                ESLucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION,
                ESLucene99ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            FieldEntry fieldEntry = readField(meta);
            validateFieldEntry(info, fieldEntry);
            fields.put(info.name, fieldEntry);
        }
    }

    static void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
        int dimension = info.getVectorDimension();
        if (dimension != fieldEntry.dimension) {
            throw new IllegalStateException(
                "Inconsistent vector dimension for field=\"" + info.name + "\"; " + dimension + " != " + fieldEntry.dimension
            );
        }

        // int8 quantized and calculated stored offset.
        long quantizedVectorBytes = dimension + Float.BYTES;
        long numQuantizedVectorBytes = Math.multiplyExact(quantizedVectorBytes, fieldEntry.size);
        if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
            throw new IllegalStateException(
                "Quantized vector data length "
                    + fieldEntry.vectorDataLength
                    + " not matching size="
                    + fieldEntry.size
                    + " * (dim="
                    + dimension
                    + " + 4)"
                    + " = "
                    + numQuantizedVectorBytes
            );
        }
    }

    @Override
    public void checkIntegrity() throws IOException {
        rawVectorsReader.checkIntegrity();
        CodecUtil.checksumEntireFile(quantizedVectorData);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return rawVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return rawVectorsReader.getByteVectorValues(field);
    }

    private static IndexInput openDataInput(SegmentReadState state, int versionMeta, String fileExtension, String codecName)
        throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        IndexInput in = state.directory.openInput(fileName, state.context);
        boolean success = false;
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                ESLucene99ScalarQuantizedVectorsFormat.VERSION_START,
                ESLucene99ScalarQuantizedVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (versionMeta != versionVectorData) {
                throw new CorruptIndexException(
                    "Format versions mismatch: meta=" + versionMeta + ", " + codecName + "=" + versionVectorData,
                    in
                );
            }
            CodecUtil.retrieveChecksum(in);
            success = true;
            return in;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(in);
            }
        }
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        FieldEntry fieldEntry = fields.get(field);
        if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
            return null;
        }
        if (fieldEntry.scalarQuantizer == null) {
            return rawVectorsReader.getRandomVectorScorer(field, target);
        }
        ESOffHeapQuantizedByteVectorValues vectorValues = ESOffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData
        );
        return new ESScalarQuantizedRandomVectorScorer(fieldEntry.similarityFunction, fieldEntry.scalarQuantizer, vectorValues, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return rawVectorsReader.getRandomVectorScorer(field, target);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(quantizedVectorData, rawVectorsReader);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOfMap(fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
        size += rawVectorsReader.ramBytesUsed();
        return size;
    }

    private FieldEntry readField(IndexInput input) throws IOException {
        VectorEncoding vectorEncoding = readVectorEncoding(input);
        VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
        return new FieldEntry(input, vectorEncoding, similarityFunction);
    }

    private VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
        int similarityFunctionId = input.readInt();
        if (similarityFunctionId < 0 || similarityFunctionId >= VectorSimilarityFunction.values().length) {
            throw new CorruptIndexException("Invalid similarity function id: " + similarityFunctionId, input);
        }
        return VectorSimilarityFunction.values()[similarityFunctionId];
    }

    private VectorEncoding readVectorEncoding(DataInput input) throws IOException {
        int encodingId = input.readInt();
        if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
            throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
        }
        return VectorEncoding.values()[encodingId];
    }

    @Override
    public ESQuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException {
        FieldEntry fieldEntry = fields.get(fieldName);
        if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
            return null;
        }
        return ESOffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData
        );
    }

    @Override
    public ScalarQuantizer getQuantizationState(String fieldName) {
        FieldEntry fieldEntry = fields.get(fieldName);
        if (fieldEntry == null || fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
            return null;
        }
        return fieldEntry.scalarQuantizer;
    }

    private static class FieldEntry implements Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class);
        final VectorSimilarityFunction similarityFunction;
        final VectorEncoding vectorEncoding;
        final int dimension;
        final long vectorDataOffset;
        final long vectorDataLength;
        final ScalarQuantizer scalarQuantizer;
        final int size;
        final OrdToDocDISIReaderConfiguration ordToDoc;

        FieldEntry(IndexInput input, VectorEncoding vectorEncoding, VectorSimilarityFunction similarityFunction) throws IOException {
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            vectorDataOffset = input.readVLong();
            vectorDataLength = input.readVLong();
            dimension = input.readVInt();
            size = input.readInt();
            if (size > 0) {
                float confidenceInterval = Float.intBitsToFloat(input.readInt());
                float minQuantile = Float.intBitsToFloat(input.readInt());
                float maxQuantile = Float.intBitsToFloat(input.readInt());
                scalarQuantizer = new ScalarQuantizer(minQuantile, maxQuantile, confidenceInterval);
            } else {
                scalarQuantizer = null;
            }
            ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(ordToDoc);
        }
    }
}

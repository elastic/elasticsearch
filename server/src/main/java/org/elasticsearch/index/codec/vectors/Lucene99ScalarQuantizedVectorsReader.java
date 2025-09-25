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
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
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
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

/**
 * Copied from Lucene 10.3.
 */
@SuppressForbidden(reason = "Lucene classes")
final class Lucene99ScalarQuantizedVectorsReader extends FlatVectorsReader implements QuantizedVectorsReader {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Lucene99ScalarQuantizedVectorsReader.class);

    static final int VERSION_START = 0;
    static final int VERSION_ADD_BITS = 1;
    static final int VERSION_CURRENT = VERSION_ADD_BITS;
    static final String META_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "Lucene99ScalarQuantizedVectorsFormatData";
    static final String META_EXTENSION = "vemq";
    static final String VECTOR_DATA_EXTENSION = "veq";

    /** Dynamic confidence interval */
    public static final float DYNAMIC_CONFIDENCE_INTERVAL = 0f;

    private final IntObjectHashMap<FieldEntry> fields = new IntObjectHashMap<>();
    private final IndexInput quantizedVectorData;
    private final FlatVectorsReader rawVectorsReader;
    private final FieldInfos fieldInfos;

    Lucene99ScalarQuantizedVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader, FlatVectorsScorer scorer)
        throws IOException {
        super(scorer);
        this.rawVectorsReader = rawVectorsReader;
        this.fieldInfos = state.fieldInfos;
        int versionMeta = -1;
        String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
        boolean success = false;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    META_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                readFields(meta, versionMeta, state.fieldInfos);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(meta, priorE);
            }
            quantizedVectorData = openDataInput(
                state,
                versionMeta,
                VECTOR_DATA_EXTENSION,
                VECTOR_DATA_CODEC_NAME,
                // Quantized vectors are accessed randomly from their node ID stored in the HNSW
                // graph.
                state.context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM)
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private void readFields(ChecksumIndexInput meta, int versionMeta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            FieldEntry fieldEntry = readField(meta, versionMeta, info);
            validateFieldEntry(info, fieldEntry);
            fields.put(info.number, fieldEntry);
        }
    }

    static void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
        int dimension = info.getVectorDimension();
        if (dimension != fieldEntry.dimension) {
            throw new IllegalStateException(
                "Inconsistent vector dimension for field=\"" + info.name + "\"; " + dimension + " != " + fieldEntry.dimension
            );
        }

        final long quantizedVectorBytes;
        if (fieldEntry.bits <= 4 && fieldEntry.compress) {
            // two dimensions -> one byte
            quantizedVectorBytes = ((dimension + 1) >> 1) + Float.BYTES;
        } else {
            // one dimension -> one byte
            quantizedVectorBytes = dimension + Float.BYTES;
        }
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

    private FieldEntry getFieldEntry(String field) {
        final FieldInfo info = fieldInfos.fieldInfo(field);
        final FieldEntry fieldEntry;
        if (info == null || (fieldEntry = fields.get(info.number)) == null) {
            throw new IllegalArgumentException("field=\"" + field + "\" not found");
        }
        if (fieldEntry.vectorEncoding != VectorEncoding.FLOAT32) {
            throw new IllegalArgumentException(
                "field=\"" + field + "\" is encoded as: " + fieldEntry.vectorEncoding + " expected: " + VectorEncoding.FLOAT32
            );
        }
        return fieldEntry;
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field);
        final FloatVectorValues rawVectorValues = rawVectorsReader.getFloatVectorValues(field);
        OffHeapQuantizedByteVectorValues quantizedByteVectorValues = OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.compress,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData
        );
        return new QuantizedVectorValues(rawVectorValues, quantizedByteVectorValues);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return rawVectorsReader.getByteVectorValues(field);
    }

    private static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        IOContext context
    ) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        IndexInput in = state.directory.openInput(fileName, context);
        boolean success = false;
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                VERSION_START,
                VERSION_CURRENT,
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
        final FieldEntry fieldEntry = getFieldEntry(field);
        if (fieldEntry.scalarQuantizer == null) {
            return rawVectorsReader.getRandomVectorScorer(field, target);
        }
        OffHeapQuantizedByteVectorValues vectorValues = OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.compress,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData
        );
        return vectorScorer.getRandomVectorScorer(fieldEntry.similarityFunction, vectorValues, target);
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
        return SHALLOW_SIZE + fields.ramBytesUsed() + rawVectorsReader.ramBytesUsed();
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        var raw = rawVectorsReader.getOffHeapByteSize(fieldInfo);
        var fieldEntry = fields.get(fieldInfo.number);
        if (fieldEntry == null) {
            assert fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
            return raw;
        }
        var quant = Map.of(VECTOR_DATA_EXTENSION, fieldEntry.vectorDataLength());
        return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, quant);
    }

    private FieldEntry readField(IndexInput input, int versionMeta, FieldInfo info) throws IOException {
        VectorEncoding vectorEncoding = readVectorEncoding(input);
        VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
        if (similarityFunction != info.getVectorSimilarityFunction()) {
            throw new IllegalStateException(
                "Inconsistent vector similarity function for field=\""
                    + info.name
                    + "\"; "
                    + similarityFunction
                    + " != "
                    + info.getVectorSimilarityFunction()
            );
        }
        return FieldEntry.create(input, versionMeta, vectorEncoding, info.getVectorSimilarityFunction());
    }

    @Override
    public QuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
        final FieldEntry fieldEntry = getFieldEntry(field);
        return OffHeapQuantizedByteVectorValues.load(
            fieldEntry.ordToDoc,
            fieldEntry.dimension,
            fieldEntry.size,
            fieldEntry.scalarQuantizer,
            fieldEntry.similarityFunction,
            vectorScorer,
            fieldEntry.compress,
            fieldEntry.vectorDataOffset,
            fieldEntry.vectorDataLength,
            quantizedVectorData
        );
    }

    @Override
    public ScalarQuantizer getQuantizationState(String field) {
        final FieldEntry fieldEntry = getFieldEntry(field);
        return fieldEntry.scalarQuantizer;
    }

    private record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int dimension,
        long vectorDataOffset,
        long vectorDataLength,
        ScalarQuantizer scalarQuantizer,
        int size,
        byte bits,
        boolean compress,
        OrdToDocDISIReaderConfiguration ordToDoc
    ) {

        static FieldEntry create(
            IndexInput input,
            int versionMeta,
            VectorEncoding vectorEncoding,
            VectorSimilarityFunction similarityFunction
        ) throws IOException {
            final var vectorDataOffset = input.readVLong();
            final var vectorDataLength = input.readVLong();
            final var dimension = input.readVInt();
            final var size = input.readInt();
            final ScalarQuantizer scalarQuantizer;
            final byte bits;
            final boolean compress;
            if (size > 0) {
                if (versionMeta < VERSION_ADD_BITS) {
                    int floatBits = input.readInt(); // confidenceInterval, unused
                    if (floatBits == -1) { // indicates a null confidence interval
                        throw new CorruptIndexException("Missing confidence interval for scalar quantizer", input);
                    }
                    float confidenceInterval = Float.intBitsToFloat(floatBits);
                    // indicates a dynamic interval, which shouldn't be provided in this version
                    if (confidenceInterval == DYNAMIC_CONFIDENCE_INTERVAL) {
                        throw new CorruptIndexException("Invalid confidence interval for scalar quantizer: " + confidenceInterval, input);
                    }
                    bits = (byte) 7;
                    compress = false;
                    float minQuantile = Float.intBitsToFloat(input.readInt());
                    float maxQuantile = Float.intBitsToFloat(input.readInt());
                    scalarQuantizer = new ScalarQuantizer(minQuantile, maxQuantile, (byte) 7);
                } else {
                    input.readInt(); // confidenceInterval, unused
                    bits = input.readByte();
                    compress = input.readByte() == 1;
                    float minQuantile = Float.intBitsToFloat(input.readInt());
                    float maxQuantile = Float.intBitsToFloat(input.readInt());
                    scalarQuantizer = new ScalarQuantizer(minQuantile, maxQuantile, bits);
                }
            } else {
                scalarQuantizer = null;
                bits = (byte) 7;
                compress = false;
            }
            final var ordToDoc = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
            return new FieldEntry(
                similarityFunction,
                vectorEncoding,
                dimension,
                vectorDataOffset,
                vectorDataLength,
                scalarQuantizer,
                size,
                bits,
                compress,
                ordToDoc
            );
        }
    }

    private static final class QuantizedVectorValues extends FloatVectorValues {
        private final FloatVectorValues rawVectorValues;
        private final QuantizedByteVectorValues quantizedVectorValues;

        QuantizedVectorValues(FloatVectorValues rawVectorValues, QuantizedByteVectorValues quantizedVectorValues) {
            this.rawVectorValues = rawVectorValues;
            this.quantizedVectorValues = quantizedVectorValues;
        }

        @Override
        public int dimension() {
            return rawVectorValues.dimension();
        }

        @Override
        public int size() {
            return rawVectorValues.size();
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return rawVectorValues.vectorValue(ord);
        }

        @Override
        public int ordToDoc(int ord) {
            return rawVectorValues.ordToDoc(ord);
        }

        @Override
        public QuantizedVectorValues copy() throws IOException {
            return new QuantizedVectorValues(rawVectorValues.copy(), quantizedVectorValues.copy());
        }

        @Override
        public VectorScorer scorer(float[] query) throws IOException {
            return quantizedVectorValues.scorer(query);
        }

        @Override
        public VectorScorer rescorer(float[] query) throws IOException {
            return rawVectorValues.rescorer(query);
        }

        @Override
        public DocIndexIterator iterator() {
            return rawVectorValues.iterator();
        }
    }
}

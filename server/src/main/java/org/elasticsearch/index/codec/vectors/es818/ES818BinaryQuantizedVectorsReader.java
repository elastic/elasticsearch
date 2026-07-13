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
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es816.BinaryQuantizer;
import org.elasticsearch.search.internal.FilterFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;
import static org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
@SuppressForbidden(reason = "Lucene classes")
public class ES818BinaryQuantizedVectorsReader extends FlatVectorsReader implements QuantizedVectorsReader {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ES818BinaryQuantizedVectorsReader.class);

    private final Map<String, FieldEntry> fields;
    private final IndexInput quantizedVectorData;
    private final FlatVectorsReader rawVectorsReader;
    private final ES818BinaryFlatVectorsScorer vectorScorer;

    @SuppressWarnings("this-escape")
    public ES818BinaryQuantizedVectorsReader(
        SegmentReadState state,
        FlatVectorsReader rawVectorsReader,
        ES818BinaryFlatVectorsScorer vectorsScorer
    ) throws IOException {
        this.fields = new HashMap<>();
        this.vectorScorer = vectorsScorer;
        this.rawVectorsReader = rawVectorsReader;
        int versionMeta = -1;
        String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsFormat.META_EXTENSION
        );
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    meta,
                    ES818BinaryQuantizedVectorsFormat.META_CODEC_NAME,
                    ES818BinaryQuantizedVectorsFormat.VERSION_START,
                    ES818BinaryQuantizedVectorsFormat.VERSION_CURRENT,
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
                VECTOR_DATA_EXTENSION,
                ES818BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
                // Quantized vectors are accessed randomly from their node ID stored in the HNSW
                // graph.
                state.context.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM)
            );
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    private ES818BinaryQuantizedVectorsReader(ES818BinaryQuantizedVectorsReader clone, FlatVectorsReader rawVectorsReader) {
        this.rawVectorsReader = rawVectorsReader;
        this.vectorScorer = clone.vectorScorer;
        this.quantizedVectorData = clone.quantizedVectorData;
        this.fields = clone.fields;
    }

    @Override
    public FlatVectorsReader getMergeInstance() throws IOException {
        return new ES818BinaryQuantizedVectorsReader(this, rawVectorsReader.getMergeInstance());
    }

    private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            FieldInfo info = infos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }
            FieldEntry fieldEntry = readField(meta, info);
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

        int binaryDims = BQVectorUtils.discretize(dimension, 64) / 8;
        long numQuantizedVectorBytes = Math.multiplyExact((binaryDims + (Float.BYTES * 3) + Short.BYTES), (long) fieldEntry.size);
        if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
            throw new IllegalStateException(
                "Binarized vector data length "
                    + fieldEntry.vectorDataLength
                    + " not matching size = "
                    + fieldEntry.size
                    + " * (binaryBytes="
                    + binaryDims
                    + " + 14"
                    + ") = "
                    + numQuantizedVectorBytes
            );
        }
    }

    @Override
    public FlatVectorsScorer getFlatVectorScorer(String field) {
        return vectorScorer;
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
        FieldEntry fi = fields.get(field);
        if (fi == null || fi.size() == 0) {
            return null;
        }
        return vectorScorer.getRandomVectorScorer(
            fi.similarityFunction,
            OffHeapBinarizedVectorValues.load(
                fi.ordToDocDISIReaderConfiguration,
                fi.dimension,
                fi.size,
                new OptimizedScalarQuantizer(fi.similarityFunction),
                fi.similarityFunction,
                vectorScorer,
                fi.centroid,
                fi.centroidDP,
                fi.vectorDataOffset,
                fi.vectorDataLength,
                quantizedVectorData
            ),
            target
        );
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
        return rawVectorsReader.getRandomVectorScorer(field, target);
    }

    @Override
    public void checkIntegrity() throws IOException {
        rawVectorsReader.checkIntegrity();
        CodecUtil.checksumEntireFile(quantizedVectorData);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        FieldEntry fi = fields.get(field);
        if (fi == null) {
            return null;
        }
        if (fi.vectorEncoding != VectorEncoding.FLOAT32) {
            throw new IllegalArgumentException(
                "field=\"" + field + "\" is encoded as: " + fi.vectorEncoding + " expected: " + VectorEncoding.FLOAT32
            );
        }
        OffHeapBinarizedVectorValues bvv = OffHeapBinarizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new OptimizedScalarQuantizer(fi.similarityFunction),
            fi.similarityFunction,
            vectorScorer,
            fi.centroid,
            fi.centroidDP,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            quantizedVectorData
        );
        return new BinarizedVectorValues(rawVectorsReader.getFloatVectorValues(field), bvv);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return rawVectorsReader.getByteVectorValues(field);
    }

    @Override
    public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        rawVectorsReader.search(field, target, knnCollector, acceptDocs);
    }

    @Override
    public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        scoreAndCollectAll(knnCollector, acceptDocs, getFloatVectorValues(field).scorer(target));
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

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        var raw = rawVectorsReader.getOffHeapByteSize(fieldInfo);
        FieldEntry fe = fields.get(fieldInfo.name);
        if (fe == null) {
            assert fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
            return raw;
        }
        var quant = Map.of(VECTOR_DATA_EXTENSION, fe.vectorDataLength());
        return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, quant);
    }

    public float[] getCentroid(String field) {
        FieldEntry fieldEntry = fields.get(field);
        if (fieldEntry != null) {
            return fieldEntry.centroid;
        }
        return null;
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
        try {
            int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                ES818BinaryQuantizedVectorsFormat.VERSION_START,
                ES818BinaryQuantizedVectorsFormat.VERSION_CURRENT,
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
            return in;
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(in);
            throw t;
        }
    }

    private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
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
        return FieldEntry.create(input, vectorEncoding, info.getVectorSimilarityFunction());
    }

    @Override
    public QuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
        FieldEntry fi = fields.get(field);
        if (fi == null) {
            return null;
        }
        if (fi.vectorEncoding != VectorEncoding.FLOAT32) {
            throw new IllegalArgumentException(
                "field=\"" + field + "\" is encoded as: " + fi.vectorEncoding + " expected: " + VectorEncoding.FLOAT32
            );
        }
        BinarizedByteVectorValues binarizedByteVectorValues = OffHeapBinarizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new OptimizedScalarQuantizer(fi.similarityFunction),
            fi.similarityFunction,
            vectorScorer,
            fi.centroid,
            fi.centroidDP,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            quantizedVectorData
        );
        return new LuceneQuantizedByteVectorValues(binarizedByteVectorValues, fi.similarityFunction);
    }

    @Override
    public ScalarQuantizer getQuantizationState(String fieldName) {
        return null;
    }

    @Override
    public CloseableRandomVectorScorerSupplier getRandomVectorScorerSupplierForMerge(
        FieldInfo fieldInfo,
        SegmentWriteState segmentWriteState
    ) throws IOException {
        FieldEntry fi = fields.get(fieldInfo.name);
        if (fi == null) {
            return null;
        }
        FloatVectorValues floatVectorValues = getFloatVectorValues(fieldInfo.name);
        if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
            floatVectorValues = new ES818BinaryQuantizedVectorsWriter.NormalizedFloatVectorValues(floatVectorValues);
        }
        OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        String tempScoreQuantizedVectorName = null;
        DocsWithFieldSet docsWithField;
        try (
            IndexOutput tempScoreQuantizedVector = segmentWriteState.directory.createTempOutput(
                segmentWriteState.segmentInfo.name,
                "queries",
                segmentWriteState.context
            )
        ) {
            tempScoreQuantizedVectorName = tempScoreQuantizedVector.getName();
            docsWithField = writeBinarizedQueryData(fi.centroid, tempScoreQuantizedVector, floatVectorValues, quantizer);
            CodecUtil.writeFooter(tempScoreQuantizedVector);
        } catch (Throwable t) {
            if (tempScoreQuantizedVectorName != null) {
                IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempScoreQuantizedVectorName);
            }
            throw t;
        }
        IndexInput quantizedScoreDataInput = segmentWriteState.directory.openInput(tempScoreQuantizedVectorName, segmentWriteState.context);
        try {
            final IndexInput finalBinarizedScoreDataInput = quantizedScoreDataInput;
            // Read the index vectors from their region within the segment data file (sliced to the field
            // offset), mirroring getQuantizedVectorValues. Using the raw, unsliced input would score the
            // graph against bytes at the wrong file offset.
            OffHeapBinarizedVectorValues vectorValues = OffHeapBinarizedVectorValues.load(
                fi.ordToDocDISIReaderConfiguration,
                fi.dimension,
                fi.size,
                quantizer,
                fieldInfo.getVectorSimilarityFunction(),
                vectorScorer,
                fi.centroid,
                fi.centroidDP,
                fi.vectorDataOffset,
                fi.vectorDataLength,
                quantizedVectorData
            );
            RandomVectorScorerSupplier scorerSupplier = vectorScorer.getRandomVectorScorerSupplier(
                fieldInfo.getVectorSimilarityFunction(),
                new OffHeapBinarizedQueryVectorValues(
                    finalBinarizedScoreDataInput,
                    fieldInfo.getVectorDimension(),
                    docsWithField.cardinality()
                ),
                vectorValues
            );
            final String finalTempScoreQuantizedVectorName = tempScoreQuantizedVectorName;
            return new BinarizedCloseableRandomVectorScorerSupplier(scorerSupplier, vectorValues, () -> {
                IOUtils.close(finalBinarizedScoreDataInput);
                IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, finalTempScoreQuantizedVectorName);
            });
        } catch (Throwable t) {
            IOUtils.closeWhileSuppressingExceptions(t, quantizedScoreDataInput);
            IOUtils.deleteFilesIgnoringExceptions(segmentWriteState.directory, tempScoreQuantizedVectorName);
            throw t;
        }
    }

    static DocsWithFieldSet writeBinarizedQueryData(
        float[] centroid,
        IndexOutput binarizedQueryData,
        FloatVectorValues floatVectorValues,
        OptimizedScalarQuantizer binaryQuantizer
    ) throws IOException {
        DocsWithFieldSet docsWithField = new DocsWithFieldSet();
        int discretizedDims = BQVectorUtils.discretize(floatVectorValues.dimension(), 64);
        int[] quantizationScratch = new int[floatVectorValues.dimension()];
        byte[] toQuery = new byte[(discretizedDims / 8) * BinaryQuantizer.B_QUERY];
        float[] scratch = new float[floatVectorValues.dimension()];
        KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            // write index vector
            OptimizedScalarQuantizer.QuantizationResult r = binaryQuantizer.scalarQuantize(
                floatVectorValues.vectorValue(iterator.index()),
                scratch,
                quantizationScratch,
                BinaryQuantizer.B_QUERY,
                centroid
            );
            docsWithField.add(docV);
            // pack and store the 4bit query vector
            ESVectorUtil.transposeHalfByte(quantizationScratch, toQuery);
            binarizedQueryData.writeBytes(toQuery, toQuery.length);
            binarizedQueryData.writeInt(Float.floatToIntBits(r.lowerInterval()));
            binarizedQueryData.writeInt(Float.floatToIntBits(r.upperInterval()));
            binarizedQueryData.writeInt(Float.floatToIntBits(r.additionalCorrection()));
            assert r.quantizedComponentSum() >= 0 && r.quantizedComponentSum() <= 0xffff;
            binarizedQueryData.writeShort((short) r.quantizedComponentSum());
        }
        return docsWithField;
    }

    private record FieldEntry(
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int dimension,
        int descritizedDimension,
        long vectorDataOffset,
        long vectorDataLength,
        int size,
        float[] centroid,
        float centroidDP,
        OrdToDocDISIReaderConfiguration ordToDocDISIReaderConfiguration
    ) {

        static FieldEntry create(IndexInput input, VectorEncoding vectorEncoding, VectorSimilarityFunction similarityFunction)
            throws IOException {
            int dimension = input.readVInt();
            long vectorDataOffset = input.readVLong();
            long vectorDataLength = input.readVLong();
            int size = input.readVInt();
            final float[] centroid;
            float centroidDP = 0;
            if (size > 0) {
                centroid = new float[dimension];
                input.readFloats(centroid, 0, dimension);
                centroidDP = Float.intBitsToFloat(input.readInt());
            } else {
                centroid = null;
            }
            OrdToDocDISIReaderConfiguration conf = OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
            return new FieldEntry(
                similarityFunction,
                vectorEncoding,
                dimension,
                BQVectorUtils.discretize(dimension, 64),
                vectorDataOffset,
                vectorDataLength,
                size,
                centroid,
                centroidDP,
                conf
            );
        }
    }

    /** Binarized vector values holding row and quantized vector values */
    protected static class BinarizedVectorValues extends FilterFloatVectorValues {
        final BinarizedByteVectorValues quantizedVectorValues;

        BinarizedVectorValues(FloatVectorValues rawVectorValues, BinarizedByteVectorValues quantizedVectorValues) {
            // Its critical that `rawVectorValues` are the filtered format
            // this aligns with rescore assumptions with HasSlice
            super(rawVectorValues);
            this.quantizedVectorValues = quantizedVectorValues;
        }

        @Override
        public BinarizedVectorValues copy() throws IOException {
            return new BinarizedVectorValues(in.copy(), quantizedVectorValues.copy());
        }

        @Override
        public VectorScorer scorer(float[] query) throws IOException {
            return quantizedVectorValues.scorer(query);
        }

        @Override
        public VectorScorer rescorer(float[] floats) throws IOException {
            return in.rescorer(floats);
        }

        BinarizedByteVectorValues getQuantizedVectorValues() throws IOException {
            return quantizedVectorValues;
        }
    }

    // When accessing vectorValue method, targerOrd here means a row ordinal.
    static class OffHeapBinarizedQueryVectorValues {
        private final IndexInput slice;
        private final int dimension;
        private final int size;
        protected final byte[] binaryValue;
        protected final ByteBuffer byteBuffer;
        private final int byteSize;
        protected final float[] correctiveValues;
        private int lastOrd = -1;
        private int quantizedComponentSum;

        OffHeapBinarizedQueryVectorValues(IndexInput data, int dimension, int size) {
            this.slice = data;
            this.dimension = dimension;
            this.size = size;
            // 4x the quantized binary dimensions
            int binaryDimensions = (BQVectorUtils.discretize(dimension, 64) / 8) * BinaryQuantizer.B_QUERY;
            this.byteBuffer = ByteBuffer.allocate(binaryDimensions);
            this.binaryValue = byteBuffer.array();
            // + 1 for the quantized sum
            this.correctiveValues = new float[3];
            this.byteSize = binaryDimensions + Float.BYTES * 3 + Short.BYTES;
        }

        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int targetOrd) throws IOException {
            if (lastOrd == targetOrd) {
                return new OptimizedScalarQuantizer.QuantizationResult(
                    correctiveValues[0],
                    correctiveValues[1],
                    correctiveValues[2],
                    quantizedComponentSum
                );
            }
            vectorValue(targetOrd);
            return new OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }

        int quantizedDimension() {
            return byteBuffer.array().length;
        }

        public int size() {
            return size;
        }

        public int dimension() {
            return dimension;
        }

        public OffHeapBinarizedQueryVectorValues copy() throws IOException {
            return new OffHeapBinarizedQueryVectorValues(slice.clone(), dimension, size);
        }

        public IndexInput getSlice() {
            return slice;
        }

        public byte[] vectorValue(int targetOrd) throws IOException {
            if (lastOrd == targetOrd) {
                return binaryValue;
            }
            slice.seek((long) targetOrd * byteSize);
            slice.readBytes(binaryValue, 0, binaryValue.length);
            slice.readFloats(correctiveValues, 0, 3);
            quantizedComponentSum = Short.toUnsignedInt(slice.readShort());
            lastOrd = targetOrd;
            return binaryValue;
        }
    }

    @SuppressForbidden(reason = "Lucene classes")
    static class LuceneQuantizedByteVectorValues extends QuantizedByteVectorValues {
        private final BinarizedByteVectorValues delegate;
        private final VectorSimilarityFunction similarityFunction;
        private final org.apache.lucene.util.quantization.OptimizedScalarQuantizer quantizer;

        LuceneQuantizedByteVectorValues(BinarizedByteVectorValues delegate, VectorSimilarityFunction similarityFunction) {
            this.delegate = delegate;
            this.similarityFunction = similarityFunction;
            this.quantizer = new org.apache.lucene.util.quantization.OptimizedScalarQuantizer(similarityFunction);
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord)
            throws IOException {
            OptimizedScalarQuantizer.QuantizationResult result = delegate.getCorrectiveTerms(ord);
            return new org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult(
                result.lowerInterval(),
                result.upperInterval(),
                result.additionalCorrection(),
                result.quantizedComponentSum()
            );
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            return delegate.vectorValue(ord);
        }

        @Override
        public int dimension() {
            return delegate.dimension();
        }

        @Override
        public org.apache.lucene.util.quantization.OptimizedScalarQuantizer getQuantizer() {
            return quantizer;
        }

        @Override
        public ScalarEncoding getScalarEncoding() {
            return ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE;
        }

        @Override
        public float[] getCentroid() throws IOException {
            return delegate.getCentroid();
        }

        @Override
        public float getCentroidDP() throws IOException {
            return delegate.getCentroidDP();
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return delegate.scorer(target);
        }

        @Override
        public QuantizedByteVectorValues copy() throws IOException {
            return new LuceneQuantizedByteVectorValues(delegate.copy(), similarityFunction);
        }

        @Override
        public DocIndexIterator iterator() {
            return delegate.iterator();
        }

        @Override
        public int ordToDoc(int ord) {
            return delegate.ordToDoc(ord);
        }
    }

    static class BinarizedCloseableRandomVectorScorerSupplier implements CloseableRandomVectorScorerSupplier {
        private final RandomVectorScorerSupplier supplier;
        private final KnnVectorValues vectorValues;
        private final Closeable onClose;

        BinarizedCloseableRandomVectorScorerSupplier(RandomVectorScorerSupplier supplier, KnnVectorValues vectorValues, Closeable onClose) {
            this.supplier = supplier;
            this.onClose = onClose;
            this.vectorValues = vectorValues;
        }

        @Override
        public UpdateableRandomVectorScorer scorer() throws IOException {
            return supplier.scorer();
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return supplier.copy();
        }

        @Override
        public void close() throws IOException {
            onClose.close();
        }

        @Override
        public int totalVectorCount() {
            return vectorValues.size();
        }
    }
}

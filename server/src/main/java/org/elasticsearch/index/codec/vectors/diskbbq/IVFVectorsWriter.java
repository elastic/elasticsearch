/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Base class for IVF vectors writer.
 */
public abstract class IVFVectorsWriter extends KnnVectorsWriter {

    private final List<FieldWriter> fieldWriters = new ArrayList<>();
    private final IndexOutput ivfCentroids, ivfClusters;
    private final IndexOutput ivfMeta;
    private final String rawVectorFormatName;
    private final int writeVersion;
    private final Boolean useDirectIOReads;
    private final FlatVectorsWriter rawVectorDelegate;

    @SuppressWarnings("this-escape")
    protected IVFVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        Boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        int writeVersion
    ) throws IOException {
        // if version >= VERSION_DIRECT_IO, useDirectIOReads should have a value
        if ((writeVersion >= ES920DiskBBQVectorsFormat.VERSION_DIRECT_IO) == (useDirectIOReads == null)) throw new IllegalArgumentException(
            "Write version " + writeVersion + " does not match direct IO value " + useDirectIOReads
        );

        this.rawVectorFormatName = rawVectorFormatName;
        this.writeVersion = writeVersion;
        this.useDirectIOReads = useDirectIOReads;
        this.rawVectorDelegate = rawVectorDelegate;
        final String metaFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES920DiskBBQVectorsFormat.IVF_META_EXTENSION
        );

        final String ivfCentroidsFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES920DiskBBQVectorsFormat.CENTROID_EXTENSION
        );
        final String ivfClustersFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ES920DiskBBQVectorsFormat.CLUSTER_EXTENSION
        );
        boolean success = false;
        try {
            ivfMeta = state.directory.createOutput(metaFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfMeta,
                ES920DiskBBQVectorsFormat.NAME,
                writeVersion,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            ivfCentroids = state.directory.createOutput(ivfCentroidsFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfCentroids,
                ES920DiskBBQVectorsFormat.NAME,
                writeVersion,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            ivfClusters = state.directory.createOutput(ivfClustersFileName, state.context);
            CodecUtil.writeIndexHeader(
                ivfClusters,
                ES920DiskBBQVectorsFormat.NAME,
                writeVersion,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public final KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
        if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
            throw new IllegalArgumentException("IVF does not support cosine similarity");
        }
        final FlatFieldVectorsWriter<?> rawVectorDelegate = this.rawVectorDelegate.addField(fieldInfo);
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            @SuppressWarnings("unchecked")
            final FlatFieldVectorsWriter<float[]> floatWriter = (FlatFieldVectorsWriter<float[]>) rawVectorDelegate;
            fieldWriters.add(new FieldWriter(fieldInfo, floatWriter));
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            fieldWriters.add(new FieldWriter(fieldInfo, null));
        }
        return rawVectorDelegate;
    }

    public abstract CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues) throws IOException;

    public abstract CentroidAssignments calculateCentroids(FieldInfo fieldInfo, FloatVectorValues floatVectorValues, MergeState mergeState)
        throws IOException;

    public record CentroidOffsetAndLength(LongValues offsets, LongValues lengths) {}

    public abstract void writeCentroids(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException;

    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    public abstract CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        FloatVectorValues floatVectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        int[] overspillAssignments
    ) throws IOException;

    public abstract CentroidSupplier createCentroidSupplier(
        IndexInput centroidsInput,
        int numCentroids,
        FieldInfo fieldInfo,
        float[] globalCentroid
    ) throws IOException;

    @Override
    public final void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
        rawVectorDelegate.flush(maxDoc, sortMap);
        for (FieldWriter fieldWriter : fieldWriters) {
            if (fieldWriter.delegate == null) {
                // field is not float, we just write meta information
                writeMeta(fieldWriter.fieldInfo, 0, 0, 0, 0, 0, null);
                continue;
            }
            // build a float vector values with random access
            final FloatVectorValues floatVectorValues = getFloatVectorValues(fieldWriter.fieldInfo, fieldWriter.delegate, maxDoc);
            // build centroids
            final CentroidAssignments centroidAssignments = calculateCentroids(fieldWriter.fieldInfo, floatVectorValues);
            // wrap centroids with a supplier
            final CentroidSupplier centroidSupplier = CentroidSupplier.fromArray(centroidAssignments.centroids());
            // write posting lists
            final long postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
            final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                fieldWriter.fieldInfo,
                centroidSupplier,
                floatVectorValues,
                ivfClusters,
                postingListOffset,
                centroidAssignments.assignments(),
                centroidAssignments.overspillAssignments()
            );
            final long postingListLength = ivfClusters.getFilePointer() - postingListOffset;
            // write centroids
            final float[] globalCentroid = centroidAssignments.globalCentroid();
            final long centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
            writeCentroids(
                fieldWriter.fieldInfo,
                centroidSupplier,
                centroidAssignments.assignments(),
                globalCentroid,
                centroidOffsetAndLength,
                ivfCentroids
            );
            final long centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
            // write meta file
            writeMeta(
                fieldWriter.fieldInfo,
                centroidSupplier.size(),
                centroidOffset,
                centroidLength,
                postingListOffset,
                postingListLength,
                globalCentroid
            );
        }
    }

    private static FloatVectorValues getFloatVectorValues(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> fieldVectorsWriter,
        int maxDoc
    ) throws IOException {
        List<float[]> vectors = fieldVectorsWriter.getVectors();
        if (vectors.size() == maxDoc) {
            return FloatVectorValues.fromFloats(vectors, fieldInfo.getVectorDimension());
        }
        final DocIdSetIterator iterator = fieldVectorsWriter.getDocsWithFieldSet().iterator();
        final int[] docIds = new int[vectors.size()];
        for (int i = 0; i < docIds.length; i++) {
            docIds[i] = iterator.nextDoc();
        }
        assert iterator.nextDoc() == NO_MORE_DOCS;
        return new FloatVectorValues() {
            @Override
            public float[] vectorValue(int ord) {
                return vectors.get(ord);
            }

            @Override
            public FloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return fieldInfo.getVectorDimension();
            }

            @Override
            public int size() {
                return vectors.size();
            }

            @Override
            public int ordToDoc(int ord) {
                return docIds[ord];
            }
        };
    }

    @Override
    public final void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.FLOAT32)) {
            mergeOneFieldIVF(fieldInfo, mergeState);
        } else {
            // we simply write information that the field is present but we don't do anything with it.
            writeMeta(fieldInfo, 0, 0, 0, 0, 0, null);
        }
        // we merge the vectors at the end so we only have two copies of the vectors on disk at the same time.
        rawVectorDelegate.mergeOneField(fieldInfo, mergeState);
    }

    private void writeMeta(
        FieldInfo field,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid
    ) throws IOException {
        ivfMeta.writeInt(field.number);
        ivfMeta.writeString(rawVectorFormatName);
        if (writeVersion >= ES920DiskBBQVectorsFormat.VERSION_DIRECT_IO) {
            ivfMeta.writeByte(useDirectIOReads ? (byte) 1 : 0);
        }
        ivfMeta.writeInt(field.getVectorEncoding().ordinal());
        ivfMeta.writeInt(distFuncToOrd(field.getVectorSimilarityFunction()));
        ivfMeta.writeInt(numCentroids);
        ivfMeta.writeLong(centroidOffset);
        ivfMeta.writeLong(centroidLength);
        if (centroidLength > 0) {
            ivfMeta.writeLong(postingListOffset);
            ivfMeta.writeLong(postingListLength);
            final ByteBuffer buffer = ByteBuffer.allocate(globalCentroid.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.asFloatBuffer().put(globalCentroid);
            ivfMeta.writeBytes(buffer.array(), buffer.array().length);
            ivfMeta.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(globalCentroid, globalCentroid)));
        }
        doWriteMeta(ivfMeta, field, numCentroids);
    }

    protected abstract void doWriteMeta(IndexOutput metaOutput, FieldInfo field, int numCentroids) throws IOException;

    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#deleteFilesIgnoringExceptions(...)")
    private void mergeOneFieldIVF(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        final int numVectors;
        String tempRawVectorsFileName = null;
        String docsFileName = null;
        boolean success = false;
        // build a float vector values with random access. In order to do that we dump the vectors to
        // a temporary file and if the segment is not dense, the docs to another file/
        try (
            IndexOutput vectorsOut = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfvec_", IOContext.DEFAULT)
        ) {
            tempRawVectorsFileName = vectorsOut.getName();
            FloatVectorValues mergedFloatVectorValues = MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
            // if the segment is dense, we don't need to do anything with docIds.
            boolean dense = mergedFloatVectorValues.size() == mergeState.segmentInfo.maxDoc();
            try (
                IndexOutput docsOut = dense
                    ? null
                    : mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "ivfdoc_", IOContext.DEFAULT)
            ) {
                if (docsOut != null) {
                    docsFileName = docsOut.getName();
                }
                // TODO do this better, we shouldn't have to write to a temp file, we should be able to
                // to just from the merged vector values, the tricky part is the random access.
                numVectors = writeFloatVectorValues(fieldInfo, docsOut, vectorsOut, mergedFloatVectorValues);
                CodecUtil.writeFooter(vectorsOut);
                if (docsOut != null) {
                    CodecUtil.writeFooter(docsOut);
                }
                success = true;
            }
        } finally {
            if (success == false) {
                if (tempRawVectorsFileName != null) {
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
                }
                if (docsFileName != null) {
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, docsFileName);
                }
            }
        }
        // now open the temp file and build the index structures. It is expected these files to be read in sequential order.
        // Even when the file might be sample, the reads will be always in increase order, therefore we set the ReadAdvice to SEQUENTIAL
        // so the OS can optimize read ahead in low memory situations.
        try (
            IndexInput vectors = mergeState.segmentInfo.dir.openInput(
                tempRawVectorsFileName,
                IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL)
            );
            IndexInput docs = docsFileName == null
                ? null
                : mergeState.segmentInfo.dir.openInput(docsFileName, IOContext.DEFAULT.withHints(DataAccessHint.SEQUENTIAL))
        ) {
            final FloatVectorValues floatVectorValues = getFloatVectorValues(fieldInfo, docs, vectors, numVectors);

            final long centroidOffset;
            final long centroidLength;
            final long postingListOffset;
            final long postingListLength;
            final int numCentroids;
            final int[] assignments;
            final int[] overspillAssignments;
            final float[] calculatedGlobalCentroid;
            String centroidTempName = null;
            IndexOutput centroidTemp = null;
            success = false;
            try {
                centroidTemp = mergeState.segmentInfo.dir.createTempOutput(mergeState.segmentInfo.name, "civf_", IOContext.DEFAULT);
                centroidTempName = centroidTemp.getName();
                CentroidAssignments centroidAssignments = calculateCentroids(
                    fieldInfo,
                    getFloatVectorValues(fieldInfo, docs, vectors, numVectors),
                    mergeState
                );
                // write the centroids to a temporary file so we are not holding them on heap
                final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                for (float[] centroid : centroidAssignments.centroids()) {
                    buffer.asFloatBuffer().put(centroid);
                    centroidTemp.writeBytes(buffer.array(), buffer.array().length);
                }
                numCentroids = centroidAssignments.numCentroids();
                assignments = centroidAssignments.assignments();
                calculatedGlobalCentroid = centroidAssignments.globalCentroid();
                overspillAssignments = centroidAssignments.overspillAssignments();
                success = true;
            } finally {
                if (success == false && centroidTempName != null) {
                    IOUtils.closeWhileHandlingException(centroidTemp);
                    org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
                }
            }
            try {
                if (numCentroids == 0) {
                    centroidOffset = ivfCentroids.getFilePointer();
                    writeMeta(fieldInfo, 0, centroidOffset, 0, 0, 0, null);
                    CodecUtil.writeFooter(centroidTemp);
                    IOUtils.close(centroidTemp);
                    return;
                }
                CodecUtil.writeFooter(centroidTemp);
                IOUtils.close(centroidTemp);

                try (IndexInput centroidsInput = mergeState.segmentInfo.dir.openInput(centroidTempName, IOContext.DEFAULT)) {
                    CentroidSupplier centroidSupplier = createCentroidSupplier(
                        centroidsInput,
                        numCentroids,
                        fieldInfo,
                        calculatedGlobalCentroid
                    );
                    // write posting lists
                    postingListOffset = ivfClusters.alignFilePointer(Float.BYTES);
                    final CentroidOffsetAndLength centroidOffsetAndLength = buildAndWritePostingsLists(
                        fieldInfo,
                        centroidSupplier,
                        floatVectorValues,
                        ivfClusters,
                        postingListOffset,
                        mergeState,
                        assignments,
                        overspillAssignments
                    );
                    postingListLength = ivfClusters.getFilePointer() - postingListOffset;
                    // write centroids
                    centroidOffset = ivfCentroids.alignFilePointer(Float.BYTES);
                    writeCentroids(
                        fieldInfo,
                        centroidSupplier,
                        assignments,
                        calculatedGlobalCentroid,
                        centroidOffsetAndLength,
                        ivfCentroids
                    );
                    centroidLength = ivfCentroids.getFilePointer() - centroidOffset;
                    // write meta
                    writeMeta(
                        fieldInfo,
                        centroidSupplier.size(),
                        centroidOffset,
                        centroidLength,
                        postingListOffset,
                        postingListLength,
                        calculatedGlobalCentroid
                    );
                }
            } finally {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, centroidTempName);
            }
        } finally {
            if (docsFileName != null) {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(
                    mergeState.segmentInfo.dir,
                    tempRawVectorsFileName,
                    docsFileName
                );
            } else {
                org.apache.lucene.util.IOUtils.deleteFilesIgnoringExceptions(mergeState.segmentInfo.dir, tempRawVectorsFileName);
            }
        }
    }

    private static FloatVectorValues getFloatVectorValues(FieldInfo fieldInfo, IndexInput docs, IndexInput vectors, int numVectors)
        throws IOException {
        if (numVectors == 0) {
            return FloatVectorValues.fromFloats(List.of(), fieldInfo.getVectorDimension());
        }
        final long vectorLength = (long) Float.BYTES * fieldInfo.getVectorDimension();
        final float[] vector = new float[fieldInfo.getVectorDimension()];
        final RandomAccessInput randomDocs = docs == null ? null : docs.randomAccessSlice(0, docs.length());
        return new FloatVectorValues() {
            @Override
            public float[] vectorValue(int ord) throws IOException {
                vectors.seek(ord * vectorLength);
                vectors.readFloats(vector, 0, vector.length);
                return vector;
            }

            @Override
            public FloatVectorValues copy() {
                return this;
            }

            @Override
            public int dimension() {
                return fieldInfo.getVectorDimension();
            }

            @Override
            public int size() {
                return numVectors;
            }

            @Override
            public int ordToDoc(int ord) {
                if (randomDocs == null) {
                    return ord;
                }
                try {
                    return randomDocs.readInt((long) ord * Integer.BYTES);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private static int writeFloatVectorValues(
        FieldInfo fieldInfo,
        IndexOutput docsOut,
        IndexOutput vectorsOut,
        FloatVectorValues floatVectorValues
    ) throws IOException {
        int numVectors = 0;
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        final KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
        for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
            numVectors++;
            buffer.asFloatBuffer().put(floatVectorValues.vectorValue(iterator.index()));
            vectorsOut.writeBytes(buffer.array(), buffer.array().length);
            if (docsOut != null) {
                docsOut.writeInt(iterator.docID());
            }
        }
        return numVectors;
    }

    private static int distFuncToOrd(VectorSimilarityFunction func) {
        for (int i = 0; i < SIMILARITY_FUNCTIONS.size(); i++) {
            if (SIMILARITY_FUNCTIONS.get(i).equals(func)) {
                return (byte) i;
            }
        }
        throw new IllegalArgumentException("invalid distance function: " + func);
    }

    @Override
    public final void finish() throws IOException {
        rawVectorDelegate.finish();
        if (ivfMeta != null) {
            // write end of fields marker
            ivfMeta.writeInt(-1);
            CodecUtil.writeFooter(ivfMeta);
        }
        if (ivfCentroids != null) {
            CodecUtil.writeFooter(ivfCentroids);
        }
        if (ivfClusters != null) {
            CodecUtil.writeFooter(ivfClusters);
        }
    }

    @Override
    public final void close() throws IOException {
        IOUtils.close(rawVectorDelegate, ivfMeta, ivfCentroids, ivfClusters);
    }

    @Override
    public final long ramBytesUsed() {
        return rawVectorDelegate.ramBytesUsed();
    }

    private record FieldWriter(FieldInfo fieldInfo, FlatFieldVectorsWriter<float[]> delegate) {}

}

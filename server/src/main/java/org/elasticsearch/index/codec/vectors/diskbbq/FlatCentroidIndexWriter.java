/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectWriter;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import static org.elasticsearch.simdvec.ES940OSQVectorsScorer.BULK_SIZE;

/**
 * Methods for writing a centroid index with one or two layers
 */
public class FlatCentroidIndexWriter {

    /**
     * Information on parent centroid groups for two-layer centroid indexing.
     *
     * @param centroids                   the parent centroids
     * @param vectors                     indexed by parent ordinal; {@code vectors[p]} holds the ordinals of the
     *                                    child centroids assigned to parent {@code p}
     * @param maxVectorsPerCentroidLength the largest number of children assigned to a single parent
     */
    public record CentroidGroups(float[][] centroids, int[][] vectors, int maxVectorsPerCentroidLength) {}

    public static CentroidGroups writeCentroidIndex(
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        IndexOutput centroidOutput
    ) throws IOException {
        CentroidSlices centroidSlices = centroidSupplier.slices();
        if (centroidSlices != null) {
            int numSlices = centroidSlices.sliceNumVectors().length;
            int maxSlice = centroidSlices.maxSliceSize();
            int bits = DirectWriter.bitsRequired(maxSlice);
            DirectWriter writer = DirectWriter.getInstance(centroidOutput, numSlices, bits);
            for (int i = 0; i < centroidSlices.sliceNumVectors().length; i++) {
                writer.add(centroidSlices.sliceNumVectors()[i]);
            }
            writer.finish();
        }
        if (centroidSupplier.centroidIndex().hasData()) {
            final CentroidGroups centroidGroups = buildCentroidGroups((FlatCentroidClusters) centroidSupplier.centroidIndex());
            // write vector ord -> centroid lookup table. We need to remap current centroid ordinals
            // to the ordinals on the parent / child structure.
            final int[] centroidOrdinalMap = new int[centroidSupplier.size()];
            int idx = 0;
            for (int[] centroidVectors : centroidGroups.vectors()) {
                for (int assignment : centroidVectors) {
                    centroidOrdinalMap[assignment] = idx++;
                }
            }
            assert idx == centroidSupplier.size() : "Expected [" + centroidSupplier.size() + "], got [" + idx + "]";
            writeCentroidLookup(centroidOutput, centroidAssignments, i -> centroidOrdinalMap[i], centroidSupplier.size());
            return centroidGroups;
        } else {
            writeCentroidLookup(centroidOutput, centroidAssignments, IntUnaryOperator.identity(), centroidSupplier.size());
            return null;
        }
    }

    public static void writeCentroidData(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        IVFVectorsWriter.CentroidOffsetAndLength centroidOffsetAndLength,
        CentroidGroups centroidGroups,
        IndexOutput centroidOutput
    ) throws IOException {
        if (centroidGroups != null) {
            writeCentroidsWithParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput, centroidGroups);
        } else {
            writeCentroidsWithoutParents(fieldInfo, centroidSupplier, globalCentroid, centroidOffsetAndLength, centroidOutput);
        }
    }

    private static void writeCentroidLookup(IndexOutput out, int[] centroidAssignments, IntUnaryOperator ordinalMap, int numberCentroids)
        throws IOException {
        final int bitsRequired = DirectWriter.bitsRequired(numberCentroids);
        final long bytesRequired = DirectWriter.bytesRequired(centroidAssignments.length, bitsRequired);
        final ByteBuffersDataOutput memory = new ByteBuffersDataOutput(bytesRequired);
        final DirectWriter writer = DirectWriter.getInstance(memory, centroidAssignments.length, bitsRequired);
        for (int centroidAssignment : centroidAssignments) {
            writer.add(ordinalMap.applyAsInt(centroidAssignment));
        }
        writer.finish();
        out.copyBytes(memory.toDataInput(), memory.size());
    }

    private static void writeSlicesOffsets(IndexOutput out, CentroidSlices centroidSlices) throws IOException {
        if (centroidSlices == null) {
            return;
        }
        // TODO: should we compress slice offsets?
        for (int offset : centroidSlices.sliceOffsets()) {
            out.writeInt(offset);
        }
    }

    private static void writeCentroidsWithParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        IVFVectorsWriter.CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput,
        CentroidGroups centroidGroups
    ) throws IOException {
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        centroidOutput.writeVInt(centroidGroups.centroids().length);
        writeSlicesOffsets(centroidOutput, centroidSupplier.slices());
        centroidOutput.writeVInt(centroidGroups.maxVectorsPerCentroidLength());
        // let's also write the raw parent centroids
        final ByteBuffer buffer = ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < centroidGroups.centroids().length; i++) {
            float[] centroid = centroidGroups.centroids()[i];
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
        QuantizedCentroids parentQuantizeCentroid = new QuantizedCentroids(
            CentroidSupplier.fromArray(centroidGroups.centroids, CentroidIndex.NO_INDEX, fieldInfo.getVectorDimension()),
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(parentQuantizeCentroid, null);
        int offset = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            centroidOutput.writeInt(offset);
            centroidOutput.writeInt(centroidVectors.length);
            offset += centroidVectors.length;
        }

        QuantizedCentroids childrenQuantizeCentroid = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        for (int[] centroidVectors : centroidGroups.vectors()) {
            childrenQuantizeCentroid.reset(idx -> centroidVectors[idx], centroidVectors.length);
            bulkWriter.writeVectors(childrenQuantizeCentroid, null);
        }
        // write the centroid offsets at the end of the file
        int parentOrd = 0;
        for (int[] centroidVectors : centroidGroups.vectors()) {
            for (int assignment : centroidVectors) {
                centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(assignment));
                centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(assignment));
                centroidOutput.writeInt(parentOrd);
            }
            parentOrd++;
        }
        // write raw centroids for merge strategy centroid reuse
        writeRawCentroids(centroidOutput, centroidSupplier, fieldInfo.getVectorDimension());
    }

    private static void writeCentroidsWithoutParents(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        IVFVectorsWriter.CentroidOffsetAndLength centroidOffsetAndLength,
        IndexOutput centroidOutput
    ) throws IOException {
        centroidOutput.writeVInt(0);
        writeSlicesOffsets(centroidOutput, centroidSupplier.slices());
        DiskBBQBulkWriter bulkWriter = DiskBBQBulkWriter.fromBitSize(7, BULK_SIZE, centroidOutput, true, true);
        final OptimizedScalarQuantizer osq = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        QuantizedCentroids quantizedCentroids = new QuantizedCentroids(
            centroidSupplier,
            fieldInfo.getVectorDimension(),
            osq,
            globalCentroid
        );
        bulkWriter.writeVectors(quantizedCentroids, null);
        // write the centroid offsets at the end of the file
        for (int i = 0; i < centroidSupplier.size(); i++) {
            centroidOutput.writeLong(centroidOffsetAndLength.offsets().get(i));
            centroidOutput.writeLong(centroidOffsetAndLength.lengths().get(i));
        }
        // write raw centroids for merge strategy centroid reuse
        writeRawCentroids(centroidOutput, centroidSupplier, fieldInfo.getVectorDimension());
    }

    private static void writeRawCentroids(IndexOutput centroidOutput, CentroidSupplier centroidSupplier, int dimension) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(dimension * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < centroidSupplier.size(); i++) {
            float[] centroid = centroidSupplier.centroid(i);
            buffer.clear();
            buffer.asFloatBuffer().put(centroid);
            centroidOutput.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    private static CentroidGroups buildCentroidGroups(FlatCentroidClusters centroidClusters) {
        final int[] centroidVectorCount = new int[centroidClusters.size()];
        for (int i = 0; i < centroidClusters.assignments().length; i++) {
            centroidVectorCount[centroidClusters.assignments()[i]]++;
        }
        final int[][] vectorsPerCentroid = new int[centroidClusters.size()][];
        int maxVectorsPerCentroidLength = 0;
        for (int i = 0; i < centroidClusters.size(); i++) {
            vectorsPerCentroid[i] = new int[centroidVectorCount[i]];
            maxVectorsPerCentroidLength = Math.max(maxVectorsPerCentroidLength, centroidVectorCount[i]);
        }
        Arrays.fill(centroidVectorCount, 0);
        for (int i = 0; i < centroidClusters.assignments().length; i++) {
            final int c = centroidClusters.assignments()[i];
            vectorsPerCentroid[c][centroidVectorCount[c]++] = i;
        }
        return new CentroidGroups(centroidClusters.centroids(), vectorsPerCentroid, maxVectorsPerCentroidLength);
    }
}

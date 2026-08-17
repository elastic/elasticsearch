/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.WelfordVariance;
import org.elasticsearch.index.codec.vectors.ash.AshPostingsListWriter;
import org.elasticsearch.index.codec.vectors.ash.AshProjectionMatrix;
import org.elasticsearch.index.codec.vectors.cluster.CentroidOps;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValuesSlice;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.HierarchicalKMeans;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.KMeansNeighbors;
import org.elasticsearch.index.codec.vectors.cluster.KMeansResult;
import org.elasticsearch.index.codec.vectors.cluster.KMeansWithOverspill;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndex;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidInformation;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSlices;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidSupplier;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidClusters;
import org.elasticsearch.index.codec.vectors.diskbbq.FlatCentroidIndexWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsWriter;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfSegmentConfig;
import org.elasticsearch.index.codec.vectors.diskbbq.OverspillAssignments;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.index.codec.vectors.diskbbq.TieredMergeStrategy;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ASH-specific implementation of {@link IVFVectorsWriter}. Delegates quantized posting list
 * writing to {@link AshPostingsListWriter} and stores the learned projection matrix in the
 * preconditioner slot of the centroid file.
 */
public class ESNextDiskASHVectorsWriter extends IVFVectorsWriter<FlatCentroidIndexWriter.CentroidGroups> {
    private static final Logger logger = LogManager.getLogger(ESNextDiskASHVectorsWriter.class);

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final String sliceField;
    private final IvfFlushConfigSource flushConfigSource;
    private final IvfMergeConfigResolver mergeConfigResolver;
    private final int bitsPerDim;
    private final float projectedDimsFraction;

    // Temporary storage for ASH projection matrix between buildAndWritePostingsLists and writePreconditioner
    private AshProjectionMatrix pendingAshMatrix;

    public ESNextDiskASHVectorsWriter(
        SegmentWriteState state,
        String rawVectorFormatName,
        boolean useDirectIOReads,
        FlatVectorsWriter rawVectorDelegate,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        TaskExecutor mergeExec,
        int numMergeWorkers,
        int flatVectorThreshold,
        String sliceField,
        IvfFlushConfigSource flushConfigSource,
        IvfMergeConfigResolver mergeConfigResolver,
        int bitsPerDim,
        float projectedDimsFraction
    ) throws IOException {
        super(
            state,
            rawVectorFormatName,
            useDirectIOReads,
            rawVectorDelegate,
            ESNextDiskASHVectorsFormat.VERSION_CURRENT,
            ESNextDiskASHVectorsFormat.NAME,
            ESNextDiskASHVectorsFormat.IVF_META_EXTENSION,
            ESNextDiskASHVectorsFormat.CENTROID_EXTENSION,
            ESNextDiskASHVectorsFormat.CLUSTER_EXTENSION,
            true,
            flatVectorThreshold
        );
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.mergeExec = mergeExec;
        this.numMergeWorkers = numMergeWorkers;
        this.sliceField = sliceField;
        this.flushConfigSource = flushConfigSource != null ? flushConfigSource : IvfFlushConfigSource.empty();
        this.mergeConfigResolver = mergeConfigResolver != null ? mergeConfigResolver : IvfMergeConfigResolver.useCodecDefault();
        this.bitsPerDim = bitsPerDim;
        this.projectedDimsFraction = projectedDimsFraction;
        if (sliceField != null) {
            Sort sort = state.segmentInfo.getIndexSort();
            if (sort == null || sort.getSort().length == 0) {
                throw new IllegalStateException("sliceField requires index sort");
            }
            SortField primary = sort.getSort()[0];
            if (sliceField.equals(primary.getField()) == false) {
                throw new IllegalStateException("sliceField must be primary index sort");
            }
            if (primary.getType() != SortField.Type.STRING) {
                throw new IllegalStateException("sliceField requires primary index sort");
            }
        }
    }

    @Override
    protected IvfSegmentConfig beginIvfFieldFlush(FieldInfo fieldInfo) throws IOException {
        return IvfSegmentConfig.fromCodecDefaults(CentroidIndexFormat.FLAT, ashConfig(), false);
    }

    @Override
    protected IvfSegmentConfig resolveMergeConfig(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        return IvfSegmentConfig.fromCodecDefaults(CentroidIndexFormat.FLAT, ashConfig(), false);
    }

    private IvfSegmentConfig.AshConfig ashConfig() {
        return new IvfSegmentConfig.AshConfig(
            projectedDimsFraction,
            bitsPerDim,
            IvfSegmentConfig.AshConfig.DEFAULT_TRAINING_ITERATIONS,
            IvfSegmentConfig.AshConfig.DEFAULT_TRAINING_FACTOR
        );
    }

    @Override
    protected Preconditioner inheritPreconditioner(FieldInfo fieldInfo, MergeState mergeState, IvfSegmentConfig ivfSegmentConfig)
        throws IOException {
        // ASH trains a new W matrix on each merge; no inheritance needed
        return null;
    }

    @Override
    protected Preconditioner createPreconditioner(int dimension, IvfSegmentConfig ivfSegmentConfig) {
        // ASH writes its own projection matrix; no standard preconditioner needed
        return null;
    }

    @Override
    protected void writePreconditioner(Preconditioner preconditioner, IndexOutput out) throws IOException {
        if (pendingAshMatrix != null) {
            pendingAshMatrix.write(out);
            pendingAshMatrix = null;
        }
    }

    @Override
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        ClusteringVectorValues<?> vectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException {
        return buildAndWriteAshPostingsLists(
            fieldInfo,
            centroidSupplier,
            vectorValues,
            postingsOutput,
            fileOffset,
            assignments,
            overspillAssignments,
            ivfSegmentConfig
        );
    }

    @Override
    public CentroidOffsetAndLength buildAndWritePostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        ClusteringVectorValues<?> vectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        MergeState mergeState,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig ivfSegmentConfig
    ) throws IOException {
        return buildAndWriteAshPostingsLists(
            fieldInfo,
            centroidSupplier,
            vectorValues,
            postingsOutput,
            fileOffset,
            assignments,
            overspillAssignments,
            ivfSegmentConfig
        );
    }

    private CentroidOffsetAndLength buildAndWriteAshPostingsLists(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        ClusteringVectorValues<?> vectorValues,
        IndexOutput postingsOutput,
        long fileOffset,
        int[] assignments,
        OverspillAssignments overspillAssignments,
        IvfSegmentConfig segmentConfig
    ) throws IOException {
        if (vectorValues instanceof FloatVectorValues == false) {
            throw new IllegalStateException("ASH requires float vectors, got: " + vectorValues.getClass().getSimpleName());
        }
        var ashWriter = new AshPostingsListWriter();
        var result = ashWriter.buildAndWrite(
            fieldInfo,
            centroidSupplier,
            (FloatVectorValues) vectorValues,
            postingsOutput,
            fileOffset,
            assignments,
            overspillAssignments,
            segmentConfig.ashConfig(),
            fieldInfo.getVectorSimilarityFunction()
        );
        pendingAshMatrix = ashWriter.getAshProjectionMatrix();
        return new CentroidOffsetAndLength(result.offsets(), result.lengths());
    }

    @Override
    protected void doWriteMeta(
        IndexOutput metaOutput,
        FieldInfo field,
        int numCentroids,
        long preconditionerOffset,
        long preconditionerLength,
        int numberOfSlices,
        int maxSliceSize,
        IvfSegmentConfig ivfSegmentConfig,
        boolean byteCentroids
    ) throws IOException {
        metaOutput.writeInt(ES940OSQVectorsScorer.BULK_SIZE);
        metaOutput.writeInt(CentroidIndexFormat.FLAT.id());
        metaOutput.writeLong(preconditionerLength);
        if (preconditionerLength > 0) {
            metaOutput.writeLong(preconditionerOffset);
        }
        if (sliceField == null) {
            assert numberOfSlices == 0;
            metaOutput.writeInt(-1);
        } else {
            metaOutput.writeInt(numberOfSlices);
            if (numberOfSlices > 0) {
                metaOutput.writeVInt(maxSliceSize);
            }
        }
        // ASH-specific: bits per dimension
        metaOutput.writeVInt(ivfSegmentConfig.ashConfig().bitsPerDim());
    }

    @Override
    protected FlatCentroidIndexWriter.CentroidGroups writeCentroidIndex(
        CentroidSupplier centroidSupplier,
        int[] centroidAssignments,
        IndexOutput centroidOutput
    ) throws IOException {
        return FlatCentroidIndexWriter.writeCentroidIndex(centroidSupplier, centroidAssignments, centroidOutput);
    }

    @Override
    protected void writeCentroidData(
        FieldInfo fieldInfo,
        CentroidSupplier centroidSupplier,
        float[] globalCentroid,
        CentroidOffsetAndLength centroidOffsetAndLength,
        FlatCentroidIndexWriter.CentroidGroups centroidGroups,
        IndexOutput centroidOutput
    ) throws IOException {
        FlatCentroidIndexWriter.writeCentroidData(
            fieldInfo,
            centroidSupplier,
            globalCentroid,
            centroidOffsetAndLength,
            centroidGroups,
            centroidOutput
        );
    }

    @Override
    public CentroidSupplier createCentroidSupplier(IndexInput centroidsInput, CentroidAssignments centroidAssignments, FieldInfo fieldInfo)
        throws IOException {
        int numCentroids = centroidAssignments.numCentroids();
        float[] globalCentroid = centroidAssignments.globalCentroid();
        CentroidSlices centroidSlices = centroidAssignments.centroidSlices();

        CentroidSupplier centroidSupplier = new OffHeapCentroidSupplier(
            centroidsInput,
            numCentroids,
            fieldInfo,
            KMeansResult.singleCluster(globalCentroid, numCentroids),
            centroidSlices
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            ClusteringFloatVectorValues floatVectorValues = centroidSupplier.asKmeansFloatVectorValues();
            if (centroidSlices == null) {
                KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(fieldInfo, floatVectorValues, true);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, centroidClusters, null);
            } else {
                List<KMeansResult<float[]>> centroidClusters = new ArrayList<>(centroidSlices.sliceOffsets().length);
                int start = 0;
                for (int i = 0; i < centroidSlices.sliceOffsets().length; i++) {
                    final int offset = start;
                    start = centroidSlices.sliceOffsets()[i];
                    int count = start - offset;
                    ClusteringFloatVectorValues slice = new ClusteringFloatVectorValuesSlice(floatVectorValues, j -> offset + j, count);
                    KMeansResult<float[]> result = buildSecondLevelClusters(fieldInfo, slice, true);
                    centroidClusters.add(result);
                    if (i == 0) {
                        centroidSlices.sliceOffsets()[i] = result.centroids().length;
                    } else {
                        centroidSlices.sliceOffsets()[i] = centroidSlices.sliceOffsets()[i - 1] + result.centroids().length;
                    }
                }
                KMeansResult<float[]> result = KMeansResult.merge(centroidClusters, CentroidOps.FLOAT);
                assert CentroidSlices.assertSliceOffsets(centroidSlices.sliceOffsets(), result.centroids().length);
                return new OffHeapCentroidSupplier(centroidsInput, numCentroids, fieldInfo, result, centroidSlices);
            }
        }
        return centroidSupplier;
    }

    @Override
    public CentroidSupplier createCentroidSupplier(FieldInfo info, float[][] centroids, float[] globalCentroid) throws IOException {
        CentroidSupplier centroidSupplier = CentroidSupplier.fromArray(
            centroids,
            new FlatCentroidClusters(KMeansResult.singleCluster(globalCentroid, centroids.length)),
            info.getVectorDimension()
        );
        if (centroidSupplier.size() > centroidsPerParentCluster * centroidsPerParentCluster) {
            KMeansResult<float[]> centroidClusters = buildSecondLevelClusters(info, centroidSupplier.asKmeansFloatVectorValues(), false);
            return CentroidSupplier.fromArray(centroids, new FlatCentroidClusters(centroidClusters), info.getVectorDimension());
        }
        return centroidSupplier;
    }

    private KMeansResult<float[]> buildSecondLevelClusters(
        FieldInfo fieldInfo,
        ClusteringFloatVectorValues floatVectorValues,
        boolean isMerge
    ) throws IOException {
        HierarchicalKMeans<float[]> hierarchicalKMeans;
        if (isMerge && mergeExec != null) {
            hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                CentroidOps.FLOAT,
                fieldInfo.getVectorDimension(),
                mergeExec,
                numMergeWorkers,
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK
            );
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(
                CentroidOps.FLOAT,
                fieldInfo.getVectorDimension(),
                HierarchicalKMeans.MAX_ITERATIONS_DEFAULT,
                HierarchicalKMeans.SAMPLES_PER_CLUSTER_DEFAULT,
                HierarchicalKMeans.MAXK
            );
        }
        return hierarchicalKMeans.cluster(floatVectorValues, centroidsPerParentCluster).result();
    }

    @Override
    public CentroidInformation<?> calculateCentroids(FieldInfo fieldInfo, ClusteringVectorValues<?> vectorValues) throws IOException {
        if (sliceField != null) {
            return buildFlatCentroidAssignments(fieldInfo, vectorValues);
        }
        ClusteringFloatVectorValues floatVectorValues = (ClusteringFloatVectorValues) vectorValues;
        HierarchicalKMeans<float[]> hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
        KMeansNeighbors<float[]> kMeansResult = hierarchicalKMeans.cluster(floatVectorValues, vectorPerCluster);
        OverspillAssignments soarOverspill = hierarchicalKMeans.computeSoar(
            floatVectorValues,
            kMeansResult.result(),
            kMeansResult.neighborHoods()
        );
        if (logger.isDebugEnabled()) {
            logger.debug("ASH flush: final centroid count: {}", kMeansResult.centroids().length);
        }
        return CentroidInformation.ofFloat(
            fieldInfo.getVectorDimension(),
            kMeansResult.centroids(),
            kMeansResult.assignments(),
            soarOverspill
        );
    }

    @Override
    @SuppressForbidden(reason = "require usage of Lucene's IOUtils#closeWhileHandlingException(...)")
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CentroidInformation<?> calculateCentroids(FieldInfo fieldInfo, ClusteringVectorValues<?> vectorValues, MergeState mergeState)
        throws IOException {
        if (sliceField != null) {
            return calculateCentroidsFullRebuildSliced(vectorValues, mergeState);
        }

        KMeansFloatVectorValues floatVectorValues = asFloatVectorValues(fieldInfo, vectorValues);

        // Gather prior segment statistics for tiered merge strategy selection
        int numSegments = mergeState.knnVectorsReaders.length;
        int[] segmentSizes = new int[numSegments];
        int[] segmentCentroidCounts = new int[numSegments];
        IVFVectorsReader.CentroidData[] segmentCentroidData = new IVFVectorsReader.CentroidData[numSegments];

        try {
            for (int i = 0; i < numSegments; i++) {
                KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
                if (reader instanceof PerFieldKnnVectorsFormat.FieldsReader perFieldReader) {
                    reader = perFieldReader.getFieldReader(fieldInfo.name);
                }
                if (reader instanceof IVFVectorsReader<?> ivfReader && mergeState.fieldInfos[i].fieldInfo(fieldInfo.name) != null) {
                    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
                        ByteVectorValues bvv = ivfReader.getByteVectorValues(fieldInfo.name);
                        segmentSizes[i] = bvv != null ? bvv.size() : 0;
                    } else {
                        segmentSizes[i] = ivfReader.getFloatVectorValues(fieldInfo.name).size();
                    }
                    segmentCentroidData[i] = ivfReader.readCentroidData(fieldInfo.name);
                    segmentCentroidCounts[i] = segmentCentroidData[i] != null ? segmentCentroidData[i].numCentroids() : 0;
                } else {
                    segmentSizes[i] = 0;
                    segmentCentroidCounts[i] = 0;
                }
            }

            TieredMergeStrategy<float[]> tieredStrategy = new TieredMergeStrategy<>(vectorPerCluster, CentroidOps.FLOAT);
            TieredMergeStrategy.MergeAction<float[]> action = tieredStrategy.selectAction(
                segmentSizes,
                segmentCentroidCounts,
                segmentCentroidData
            );

            if (logger.isDebugEnabled()) {
                int totalVectors = 0;
                int totalCentroids = 0;
                for (int s : segmentSizes) {
                    totalVectors += s;
                }
                for (int c : segmentCentroidCounts) {
                    totalCentroids += c;
                }
                logger.debug(
                    "ASH merge for field [{}]: selected strategy [{}], segments={}, totalVectors={}, totalCentroids={}",
                    fieldInfo.name,
                    action.strategy(),
                    numSegments,
                    totalVectors,
                    totalCentroids
                );
            }

            HierarchicalKMeans<float[]> hierarchicalKMeans;
            if (mergeExec != null) {
                hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(
                    CentroidOps.FLOAT,
                    floatVectorValues.dimension(),
                    mergeExec,
                    numMergeWorkers
                );
            } else {
                hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, floatVectorValues.dimension());
            }
            KMeansWithOverspill<float[]> kMeansResult = action.execute(hierarchicalKMeans, floatVectorValues, vectorPerCluster);
            if (logger.isDebugEnabled()) {
                int[] clusterSizes = new int[kMeansResult.centroids().length];
                for (int a : kMeansResult.assignments()) {
                    clusterSizes[a]++;
                }
                printClusterQualityStatistics(clusterSizes);
            }

            return CentroidInformation.ofFloat(
                fieldInfo.getVectorDimension(),
                kMeansResult.centroids(),
                kMeansResult.assignments(),
                kMeansResult.overspill()
            );
        } finally {
            org.apache.lucene.util.IOUtils.closeWhileHandlingException(segmentCentroidData);
        }
    }

    private CentroidInformation<?> calculateCentroidsFullRebuildSliced(ClusteringVectorValues<?> vectorValues, MergeState mergeState)
        throws IOException {
        final FieldInfo slicedFieldInfo = mergeState.mergeFieldInfos.fieldInfo(sliceField);
        assert slicedFieldInfo != null;
        assert slicedFieldInfo.getDocValuesType() == DocValuesType.SORTED : "sliceField must be SortedDocValues";
        final SortedDocValues values = DocValueConsumerHelper.INSTANCE.getMergeSortedField(slicedFieldInfo, mergeState);
        final int numSlices = values.getValueCount();

        // ASH only supports float vectors
        return calculateCentroidsFullRebuildSlicedFloat((ClusteringFloatVectorValues) vectorValues, values, numSlices);
    }

    private CentroidInformation<float[]> calculateCentroidsFullRebuildSlicedFloat(
        ClusteringFloatVectorValues vectorValues,
        SortedDocValues values,
        int numSlices
    ) throws IOException {
        if (vectorValues.size() / numSlices <= 4 * flatVectorThreshold) {
            final int dim = vectorValues.dimension();
            float[] centroid = new float[dim];
            CentroidOps.FLOAT.accumulateAll(vectorValues, centroid);
            for (int d = 0; d < dim; d++) {
                centroid[d] /= vectorValues.size();
            }
            return CentroidInformation.ofFloat(dim, new float[][] { centroid }, new int[vectorValues.size()], OverspillAssignments.NONE);
        }

        HierarchicalKMeans<float[]> hierarchicalKMeans;
        if (mergeExec != null) {
            hierarchicalKMeans = HierarchicalKMeans.ofConcurrent(CentroidOps.FLOAT, vectorValues.dimension(), mergeExec, numMergeWorkers);
        } else {
            hierarchicalKMeans = HierarchicalKMeans.ofSerial(CentroidOps.FLOAT, vectorValues.dimension());
        }
        final KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
        iterator.advance(0);
        values.nextDoc();
        final int[] sliceOffsets = new int[numSlices];
        final int[] sliceLengths = new int[numSlices];
        List<KMeansWithOverspill<float[]>> kmeansResults = new ArrayList<>();
        for (int i = 0; i < numSlices; i++) {
            if (iterator.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                sliceLengths[i] = 0;
                sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                continue;
            }
            int sliceDocStart = values.docID();
            while (values.docID() != DocIdSetIterator.NO_MORE_DOCS && values.ordValue() == i) {
                values.nextDoc();
            }
            final int sliceDocEnd = values.docID();
            int vectorDocStart = iterator.docID();
            if (vectorDocStart < sliceDocStart) {
                vectorDocStart = iterator.advance(sliceDocStart);
            }
            if (vectorDocStart > sliceDocEnd) {
                sliceLengths[i] = 0;
                sliceOffsets[i] = i == 0 ? 0 : sliceOffsets[i - 1];
                continue;
            }
            final int vectorOrdStart = iterator.index();
            final int docEnd = vectorDocStart == sliceDocEnd ? sliceDocEnd : iterator.advance(sliceDocEnd);
            final int vectorOrdEnd = docEnd == KnnVectorValues.DocIndexIterator.NO_MORE_DOCS ? vectorValues.size() : iterator.index();
            final int sliceNumVectors = vectorOrdEnd - vectorOrdStart;
            final ClusteringFloatVectorValues slice = new ClusteringFloatVectorValuesSlice(
                vectorValues,
                j -> vectorOrdStart + j,
                sliceNumVectors
            );
            final KMeansNeighbors<float[]> kMeansResult = hierarchicalKMeans.cluster(slice, vectorPerCluster);
            final OverspillAssignments overspill = hierarchicalKMeans.computeSoar(
                slice,
                kMeansResult.result(),
                kMeansResult.neighborHoods()
            );
            kmeansResults.add(new KMeansWithOverspill<>(kMeansResult.result(), overspill));
            sliceLengths[i] = sliceNumVectors;
            sliceOffsets[i] = i == 0 ? kMeansResult.centroids().length : sliceOffsets[i - 1] + kMeansResult.centroids().length;
        }
        final KMeansWithOverspill<float[]> merged = KMeansWithOverspill.merge(kmeansResults, CentroidOps.FLOAT);
        if (logger.isDebugEnabled()) {
            logger.debug("ASH sliced merge: final centroid count: {}", merged.centroids().length);
        }
        final CentroidSlices centroidSlices = new CentroidSlices(sliceOffsets, sliceLengths);
        return CentroidInformation.ofFloat(
            vectorValues.dimension(),
            merged.centroids(),
            merged.assignments(),
            merged.overspill(),
            centroidSlices
        );
    }

    private static void printClusterQualityStatistics(int[] clusterSizes) {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;
        WelfordVariance clusterSizeStats = new WelfordVariance();
        for (int size : clusterSizes) {
            clusterSizeStats.add(size);
            min = Math.min(min, size);
            max = Math.max(max, size);
        }
        double variance = clusterSizeStats.m2() / (clusterSizes.length - 1);
        logger.debug(
            "Centroid count: {} min: {} max: {} mean: {} stdDev: {} variance: {}",
            clusterSizes.length,
            min,
            max,
            clusterSizeStats.mean(),
            Math.sqrt(variance),
            variance
        );
    }

    // Off-heap centroid supplier — duplicated from BBQ writer for now.
    static class OffHeapCentroidSupplier implements CentroidSupplier {
        private final IndexInput centroidsInput;
        private final int numCentroids;
        private final int dimension;
        private final float[] scratch;
        private final KMeansResult<float[]> clusters;
        private final CentroidSlices centroidSlices;
        private int currOrd = -1;

        OffHeapCentroidSupplier(
            IndexInput centroidsInput,
            int numCentroids,
            FieldInfo info,
            KMeansResult<float[]> clusters,
            CentroidSlices centroidSlices
        ) {
            this.centroidsInput = centroidsInput;
            this.numCentroids = numCentroids;
            this.dimension = info.getVectorDimension();
            this.scratch = new float[dimension];
            this.clusters = clusters;
            this.centroidSlices = centroidSlices;
        }

        @Override
        public int size() {
            return numCentroids;
        }

        @Override
        public float[] centroid(int centroidOrdinal) throws IOException {
            if (centroidOrdinal != currOrd) {
                centroidsInput.seek((long) centroidOrdinal * dimension * Float.BYTES);
                centroidsInput.readFloats(scratch, 0, dimension);
                currOrd = centroidOrdinal;
            }
            return scratch;
        }

        @Override
        public byte[] byteCentroid(int centroidOrdinal) throws IOException {
            return null;
        }

        @Override
        public CentroidIndex centroidIndex() {
            return new FlatCentroidClusters(clusters);
        }

        @Override
        public CentroidSlices slices() throws IOException {
            return centroidSlices;
        }

        @Override
        public KMeansFloatVectorValues asKmeansFloatVectorValues() throws IOException {
            return KMeansFloatVectorValues.build(centroidsInput, null, numCentroids, dimension);
        }
    }

    // Helper class to access merged sorted doc values for sliced indices
    private static class DocValueConsumerHelper extends DocValuesConsumer {

        static final DocValueConsumerHelper INSTANCE = new DocValueConsumerHelper();

        public SortedDocValues getMergeSortedField(FieldInfo fieldInfo, final MergeState mergeState) throws IOException {
            final OrdinalMap map = createOrdinalMapForSortedDV(fieldInfo, mergeState);
            return getMergedSortedSetDocValues(fieldInfo, mergeState, map);
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new AssertionError("Method should not be called");
        }

        @Override
        public void close() {
            throw new AssertionError("Method should not be called");
        }
    }
}

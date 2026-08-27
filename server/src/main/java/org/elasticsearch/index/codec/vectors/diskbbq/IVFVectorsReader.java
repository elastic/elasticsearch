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
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.vectors.GenericFlatVectorReaders;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringFloatVectorValues;
import org.elasticsearch.index.codec.vectors.cluster.ClusteringVectorValues;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.search.vectors.IVFKnnSearchStrategy;
import org.elasticsearch.search.vectors.KnnSearchProfileData;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.SIMILARITY_FUNCTIONS;

/**
 * Reader for IVF vectors. This reader is used to read the IVF vectors from the index.
 */
public abstract class IVFVectorsReader<E extends IVFVectorsReader.FieldEntry> extends KnnVectorsReader {

    /**
     * Sealed query target for IVF search dispatch. Allows the search path to handle
     * both float and byte queries through pattern matching.
     */
    public sealed interface QueryTarget {
        int dimension();

        /** A byte vector query target. */
        record ByteQuery(byte[] vector) implements QueryTarget {
            public int dimension() {
                return vector.length;
            }
        }

        /** A float vector query target. */
        record FloatQuery(float[] vector) implements QueryTarget {
            public int dimension() {
                return vector.length;
            }
        }
    }

    // Two-Signal Model constants for dynamic visit ratio computation.
    // Computes a visit ratio from the num_candidates/k ratio signal.
    private static final double V_MIN = 0.003;
    private static final double V_MAX = 0.04;
    private static final double LOG1P_R_MAX = Math.log1p(10.0);
    private static final double LOG1P_K_MAX = Math.log1p(10_000.0);
    private static final double RATIO_WEIGHT = 0.85;
    private static final double K_WEIGHT = 0.15;

    // Segment-size cap constants.
    // Empirical power-law curve calibrated on GIST-1M, Wiki-Cohere-1M, and MSMarco-130M datasets.
    // Caps the visit ratio for large segments where fewer clusters need visiting to achieve the target recall.
    // Produces ~10% cap for small segments (100K), ~4.5% at 1M, and ~2-3% for large segments (5-10M).
    private static final double CAP_COEFFICIENT = 0.045;
    private static final int CAP_REF_SIZE = 1_000_000;
    private static final double CAP_EXPONENT = 0.35;
    static final float DEFAULT_TARGET_RECALL = 0.9f;

    // Small-segment boost constants.
    // Amplifies the dynamic visit ratio for segments below BOOST_REF_SIZE where IVF clusters are less
    // well-formed and the base formula under-provisions. Calibrated on cross-validation across
    // Wiki-Cohere, GIST-1M, and Quora-E5 datasets; validated on held-out GloVe-200.
    private static final int BOOST_REF_SIZE = 500_000;
    private static final double BOOST_EXPONENT = 0.30;
    private static final int BOOST_K_REF = 10;
    private static final double BOOST_K_EXPONENT = 0.10;

    protected final IndexInput ivfCentroids, ivfClusters;
    private final SegmentReadState state;
    protected final FieldInfos fieldInfos;
    protected final IntObjectHashMap<E> fields;
    private final GenericFlatVectorReaders genericReaders;
    private final String centroidExtension;
    private final String clusterExtension;
    private final int versionDirectIo;
    private final float dynamicVisitRatio;
    protected int versionMeta = -1;

    @SuppressWarnings("this-escape")
    protected IVFVectorsReader(
        SegmentReadState state,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader,
        String codecName,
        String centroidExtension,
        String clusterExtension,
        String metaExtension,
        int versionStart,
        int versionCurrent,
        int versionDirectIo,
        float dynamicVisitRatio
    ) throws IOException {
        this.state = state;
        this.fieldInfos = state.fieldInfos;
        this.fields = new IntObjectHashMap<>();
        this.genericReaders = new GenericFlatVectorReaders();
        this.centroidExtension = centroidExtension;
        this.clusterExtension = clusterExtension;
        this.versionDirectIo = versionDirectIo;
        this.dynamicVisitRatio = dynamicVisitRatio;
        String meta = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

        int versionMeta = -1;
        try (ChecksumIndexInput ivfMeta = state.directory.openChecksumInput(meta)) {
            Throwable priorE = null;
            try {
                versionMeta = CodecUtil.checkIndexHeader(
                    ivfMeta,
                    codecName,
                    versionStart,
                    versionCurrent,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                this.versionMeta = versionMeta;
                readFields(ivfMeta, versionMeta, genericReaders, loadReader);
            } catch (Throwable exception) {
                priorE = exception;
            } finally {
                CodecUtil.checkFooter(ivfMeta, priorE);
            }
            ivfCentroids = openDataInput(state, versionMeta, centroidExtension, codecName, versionStart, versionCurrent, state.context);
            ivfClusters = openDataInput(state, versionMeta, clusterExtension, codecName, versionStart, versionCurrent, state.context);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(this);
            throw t;
        }
    }

    /**
     * Copy constructor used to build a merge instance: shares everything with {@code other} but uses
     * the provided flat vector readers.
     */
    protected IVFVectorsReader(IVFVectorsReader<E> other, GenericFlatVectorReaders genericReaders) {
        this.state = other.state;
        this.fieldInfos = other.fieldInfos;
        this.fields = other.fields;
        this.genericReaders = genericReaders;
        this.centroidExtension = other.centroidExtension;
        this.clusterExtension = other.clusterExtension;
        this.versionDirectIo = other.versionDirectIo;
        this.dynamicVisitRatio = other.dynamicVisitRatio;
        this.versionMeta = other.versionMeta;
        this.ivfCentroids = other.ivfCentroids;
        this.ivfClusters = other.ivfClusters;
    }

    public abstract CentroidIterator getCentroidIterator(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        QueryTarget queryTarget,
        IndexInput postingListSlice,
        AcceptDocs acceptDocs,
        float approximateCost,
        KnnVectorValues values,
        float visitRatio
    ) throws IOException;

    /** Get the number of vectors to search, which is typically the total number of vectors in the segment or the
     *  number of vectors in a slice if the segment is sliced.*/
    protected int getNumberOfVectors(E entry, KnnVectorValues values, IndexInput centroidSlice, ESAcceptDocs esAcceptDocs)
        throws IOException {
        return values.size();
    }

    protected static IndexInput openDataInput(
        SegmentReadState state,
        int versionMeta,
        String fileExtension,
        String codecName,
        int versionStart,
        int versionCurrent,
        IOContext context
    ) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
        final IndexInput in = state.directory.openInput(fileName, context);
        try {
            final int versionVectorData = CodecUtil.checkIndexHeader(
                in,
                codecName,
                versionStart,
                versionCurrent,
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

    private void readFields(
        ChecksumIndexInput meta,
        int versionMeta,
        GenericFlatVectorReaders genericFields,
        GenericFlatVectorReaders.LoadFlatVectorsReader loadReader
    ) throws IOException {
        for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
            final FieldInfo info = fieldInfos.fieldInfo(fieldNumber);
            if (info == null) {
                throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
            }

            E fieldEntry = readField(meta, info, versionMeta);
            genericFields.loadField(fieldNumber, fieldEntry, loadReader);

            fields.put(info.number, fieldEntry);
        }
    }

    private E readField(IndexInput input, FieldInfo info, int versionMeta) throws IOException {
        final String rawVectorFormat = input.readString();
        final boolean useDirectIOReads = versionMeta >= versionDirectIo && input.readByte() == 1;
        final VectorEncoding vectorEncoding = readVectorEncoding(input);
        final VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
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
        final int numCentroids = input.readInt();
        final long centroidOffset = input.readLong();
        final long centroidLength = input.readLong();
        final float[] globalCentroid = new float[info.getVectorDimension()];
        long postingListOffset = -1;
        long postingListLength = 0;
        float globalCentroidDp = 0;
        if (centroidLength > 0) {
            postingListOffset = input.readLong();
            postingListLength = input.readLong();
            input.readFloats(globalCentroid, 0, globalCentroid.length);
            globalCentroidDp = Float.intBitsToFloat(input.readInt());
        }
        return doReadField(
            input,
            rawVectorFormat,
            useDirectIOReads,
            similarityFunction,
            vectorEncoding,
            numCentroids,
            centroidOffset,
            centroidLength,
            postingListOffset,
            postingListLength,
            globalCentroid,
            globalCentroidDp
        );
    }

    protected abstract E doReadField(
        IndexInput input,
        String rawVectorFormat,
        boolean useDirectIOReads,
        VectorSimilarityFunction similarityFunction,
        VectorEncoding vectorEncoding,
        int numCentroids,
        long centroidOffset,
        long centroidLength,
        long postingListOffset,
        long postingListLength,
        float[] globalCentroid,
        float globalCentroidDp
    ) throws IOException;

    private static VectorSimilarityFunction readSimilarityFunction(DataInput input) throws IOException {
        final int i = input.readInt();
        if (i < 0 || i >= SIMILARITY_FUNCTIONS.size()) {
            throw new IllegalArgumentException("invalid distance function: " + i);
        }
        return SIMILARITY_FUNCTIONS.get(i);
    }

    private static VectorEncoding readVectorEncoding(DataInput input) throws IOException {
        final int encodingId = input.readInt();
        if (encodingId < 0 || encodingId >= VectorEncoding.values().length) {
            throw new CorruptIndexException("Invalid vector encoding id: " + encodingId, input);
        }
        return VectorEncoding.values()[encodingId];
    }

    @Override
    public final void checkIntegrity() throws IOException {
        for (var reader : genericReaders.allReaders()) {
            reader.checkIntegrity();
        }
        CodecUtil.checksumEntireFile(ivfCentroids);
        CodecUtil.checksumEntireFile(ivfClusters);
    }

    @Override
    public final KnnVectorsReader getMergeInstance() throws IOException {
        return mergeInstance(genericReaders.getMergeInstance());
    }

    /** Builds a merge instance of this reader backed by the given flat vector merge readers. */
    protected abstract IVFVectorsReader<E> mergeInstance(GenericFlatVectorReaders genericReaders);

    @Override
    public final void finishMerge() throws IOException {
        for (var reader : genericReaders.allReaders()) {
            reader.finishMerge();
        }
    }

    protected FlatVectorsReader getReaderForField(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        if (info == null) throw new IllegalArgumentException("Could not find field [" + field + "]");
        return genericReaders.getReaderForField(info.number);
    }

    @Override
    public final FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return getReaderForField(field).getFloatVectorValues(field);
    }

    @Override
    public final ByteVectorValues getByteVectorValues(String field) throws IOException {
        return getReaderForField(field).getByteVectorValues(field);
    }

    /**
     * Returns true if this field has an IVF structure (centroids), false if it should fall back to the raw delegate.
     */
    private boolean hasIvfStructure(String field) {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo == null || fieldInfo.getVectorDimension() == 0) {
            return false;
        }
        FieldEntry entry = fields.get(fieldInfo.number);
        return entry != null && entry.numCentroids > 0;
    }

    @Override
    public final void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        if (hasIvfStructure(field)) {
            doSearch(field, new QueryTarget.FloatQuery(target), knnCollector, acceptDocs);
        } else {
            getReaderForField(field).search(field, target, knnCollector, acceptDocs);
        }
    }

    @Override
    public final void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        if (hasIvfStructure(field)) {
            doSearch(field, new QueryTarget.ByteQuery(target), knnCollector, acceptDocs);
        } else {
            // No IVF structure — byte fields on legacy codecs (which skip byte IVF indexing) fall
            // through here. Inline brute-force matches main's behavior and is required for CheckIndex
            // compatibility (delegating to getReaderForField().search() fails CheckIndex validation).
            final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
            final ByteVectorValues values = getReaderForField(field).getByteVectorValues(field);
            for (int i = 0; i < values.size(); i++) {
                final float score = fieldInfo.getVectorSimilarityFunction().compare(target, values.vectorValue(i));
                knnCollector.collect(values.ordToDoc(i), score);
                if (knnCollector.earlyTerminated()) {
                    return;
                }
            }
        }
    }

    /**
     * Shared IVF search implementation. Uses the sealed {@link QueryTarget} to dispatch between
     * native byte[] quantization and float[] quantization paths.
     */
    private void doSearch(String field, QueryTarget queryTarget, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        final E entry = fields.get(fieldInfo.number);
        if (hasNoVectors(fieldInfo, entry)) {
            return;
        }
        final int queryDimension = queryTarget.dimension();
        if (fieldInfo.getVectorDimension() != queryDimension) {
            throw new IllegalArgumentException(
                "vector query dimension: " + queryDimension + " differs from field dimension: " + fieldInfo.getVectorDimension()
            );
        }

        final ESAcceptDocs esAcceptDocs;
        if (acceptDocs instanceof ESAcceptDocs) {
            esAcceptDocs = (ESAcceptDocs) acceptDocs;
        } else {
            esAcceptDocs = null;
        }

        final KnnVectorValues values;
        if (fieldInfo.getVectorEncoding().equals(VectorEncoding.BYTE)) {
            values = getByteVectorValues(field);
        } else {
            values = getFloatVectorValues(field);
        }
        if (values == null) {
            return;
        }
        final IndexInput centroids = entry.centroidSlice(ivfCentroids);
        final int numVectors = getNumberOfVectors(entry, values, centroids, esAcceptDocs);
        if (numVectors == 0) {
            return; // nothing more to do if there are no vectors in this segment / slice
        }
        final float approximateCost;
        if (esAcceptDocs instanceof ESAcceptDocs.ESAcceptDocsAll) {
            approximateCost = numVectors;
        } else {
            approximateCost = esAcceptDocs == null ? acceptDocs.cost() : esAcceptDocs.approximateCost();
        }
        float percentFiltered = Math.clamp(approximateCost / numVectors, 0f, 1f);
        int k = knnCollector.k();
        int numCands = k;
        float visitRatio = dynamicVisitRatio;
        KnnSearchProfileData profileData = null;
        // Search strategy may be null if this is being called from checkIndex (e.g. from a test)
        final IVFKnnSearchStrategy ivfSearchStrategy = knnCollector.getSearchStrategy() instanceof IVFKnnSearchStrategy s ? s : null;
        if (ivfSearchStrategy != null) {
            visitRatio = ivfSearchStrategy.getVisitRatio();
            numCands = ivfSearchStrategy.getNumCands();
            k = ivfSearchStrategy.getK();
            profileData = ivfSearchStrategy.getProfileData();
        }

        if (visitRatio == dynamicVisitRatio) {
            visitRatio = Math.min(
                computeDynamicVisitRatio(numCands, k) * computeSmallSegmentBoost(k, numVectors),
                computeSegmentSizeCap(numVectors)
            );
        }
        if (ivfSearchStrategy != null) {
            // Report the ratio back on the strategy rather than straight onto the shared profile data: one
            // strategy exists per leaf search, so the query can attribute it to the right segment.
            ivfSearchStrategy.setResolvedVisitRatio(visitRatio);
        }
        long maxVectorVisited = maxVectorsToVisit(entry, visitRatio, numVectors);
        IndexInput postListSlice = entry.postingListSlice(ivfClusters);
        long centroidIteratorStartNs = profileData != null ? System.nanoTime() : 0;
        CentroidIterator centroidPrefetchingIterator = getCentroidIterator(
            fieldInfo,
            entry.numCentroids,
            centroids,
            queryTarget,
            postListSlice,
            acceptDocs,
            approximateCost,
            values,
            visitRatio
        );
        if (profileData != null) {
            profileData.addCentroidIteratorCreateTimeNs(System.nanoTime() - centroidIteratorStartNs);
        }
        Bits acceptDocsBits = acceptDocs.bits();
        PostingVisitor scorer = getPostingVisitor(
            fieldInfo,
            values,
            postListSlice,
            queryTarget,
            acceptDocsBits,
            entry.centroidSlice(ivfCentroids),
            esAcceptDocs
        );
        if (profileData != null) {
            scorer.enableProfiling();
        }
        long expectedDocs = 0;
        long actualDocs = 0;
        int centroidsEvaluated = 0;
        long postingVisitTimeNs = 0;
        long resetScorerTimeNs = 0;
        // initially we visit only the "centroids to search"
        // Note, numCollected is doing the bare minimum here.
        // TODO do we need to handle nested doc counts similarly to how we handle
        // filtering? E.g. keep exploring until we hit an expected number of parent documents vs. child vectors?
        while (centroidPrefetchingIterator.hasNext()
            && (maxVectorVisited > expectedDocs || knnCollector.minCompetitiveSimilarity() == Float.NEGATIVE_INFINITY)) {
            PostingMetadata postingMetadata = centroidPrefetchingIterator.nextPosting();
            long resetStartNs = profileData != null ? System.nanoTime() : 0;
            expectedDocs += scorer.resetPostingsScorer(postingMetadata);
            if (profileData != null) {
                resetScorerTimeNs += System.nanoTime() - resetStartNs;
            }
            long visitStartNs = profileData != null ? System.nanoTime() : 0;
            actualDocs += scorer.visit(knnCollector);
            if (profileData != null) {
                postingVisitTimeNs += System.nanoTime() - visitStartNs;
            }
            centroidsEvaluated++;
            if (knnCollector.getSearchStrategy() != null) {
                knnCollector.getSearchStrategy().nextVectorsBlock();
            }
        }
        if (acceptDocsBits != null) {
            // TODO Adjust the value here when using centroid filtering
            float unfilteredRatioVisited = (float) expectedDocs / numVectors;
            int filteredVectors = (int) Math.ceil(numVectors * percentFiltered);
            float expectedScored = Math.min(2 * filteredVectors * unfilteredRatioVisited, expectedDocs / 2f);
            while (centroidPrefetchingIterator.hasNext() && (actualDocs < expectedScored || actualDocs < knnCollector.k())) {
                PostingMetadata postingMetadata = centroidPrefetchingIterator.nextPosting();
                long resetStartNs = profileData != null ? System.nanoTime() : 0;
                scorer.resetPostingsScorer(postingMetadata);
                if (profileData != null) {
                    resetScorerTimeNs += System.nanoTime() - resetStartNs;
                }
                long visitStartNs = profileData != null ? System.nanoTime() : 0;
                actualDocs += scorer.visit(knnCollector);
                if (profileData != null) {
                    postingVisitTimeNs += System.nanoTime() - visitStartNs;
                }
                centroidsEvaluated++;
                if (knnCollector.getSearchStrategy() != null) {
                    knnCollector.getSearchStrategy().nextVectorsBlock();
                }
            }
        }
        if (profileData != null) {
            profileData.addCentroidsEvaluated(centroidsEvaluated);
            profileData.addResetPostingsScorerTimeNs(resetScorerTimeNs);
            profileData.addPostingVisitTimeNs(postingVisitTimeNs);
            profileData.addPostingsScored(actualDocs);
            profileData.addExpectedDocs(expectedDocs);
            PostingVisitor.Profile visitorProfile = scorer.profile();
            if (visitorProfile != null) {
                profileData.addDocIdReadTimeNs(visitorProfile.docIdReadTimeNs());
                profileData.addScoringTimeNs(visitorProfile.scoringTimeNs());
                profileData.addQueryQuantizationTimeNs(visitorProfile.queryQuantizationTimeNs());
                profileData.addCentroidReadTimeNs(visitorProfile.centroidReadTimeNs());
                profileData.setScorer(visitorProfile.scorerImplementation());
            }
        }
    }

    /**
     * The cap on the number of (posting-member) vectors the search loop may visit. The default accounts for
     * SOAR overspill, which can place a vector in up to two postings, by allowing 2x the visit-ratio budget.
     * Subclasses may override to use a different budgeting model (e.g. an experiment-only posting/head-count
     * budget where the centroid iterator's own bound governs how many postings are drained).
     */
    protected long maxVectorsToVisit(E entry, float visitRatio, int numVectors) {
        return (long) (2.0 * visitRatio * numVectors);
    }

    private static boolean hasNoVectors(FieldInfo fieldInfo, FieldEntry fieldEntry) {
        return fieldInfo.getVectorDimension() == 0
            || fieldEntry == null
            || (fieldEntry.numCentroids() == 0 && fieldEntry.postingListLength == 0L && fieldEntry.centroidLength == 0L);
    }

    /**
     * Computes the dynamic visit ratio using the Two-Signal model.
     * The formula blends the num_candidates/k ratio signal with the k magnitude signal.
     */
    static float computeDynamicVisitRatio(int numCands, int k) {
        double r = (double) numCands / Math.max(k, 1);
        double z = RATIO_WEIGHT * logScale(r - 1.0, LOG1P_R_MAX) + K_WEIGHT * logScale(k, LOG1P_K_MAX);
        return (float) (V_MIN + (V_MAX - V_MIN) * z);
    }

    private static double logScale(double value, double log1pMax) {
        return Math.clamp(Math.log1p(value) / log1pMax, 0.0, 1.0);
    }

    /**
     * Computes a small-segment boost multiplier for the dynamic visit ratio.
     * Segments below {@link #BOOST_REF_SIZE} vectors have less well-formed IVF clusters, so the base
     * dynamic formula under-provisions. This multiplier compensates with a power-law that decays to 1.0
     * at the reference size. A mild k-scaling factor accounts for higher k needing slightly more budget.
     * <p>
     * Formula: boost = max(1.0, (BOOST_REF_SIZE / N)^0.3 * (k / 10)^0.1 * recallFactor)
     *
     * @param k the number of nearest neighbors requested
     * @param numVectors number of vectors in the segment
     * @return the boost multiplier (>= 1.0)
     */
    static float computeSmallSegmentBoost(int k, int numVectors) {
        // numVectors <= 0 is already guarded at the call site (search returns early on an empty
        // segment); handle it defensively here too, mirroring computeSegmentSizeCap, so the
        // division below can never see a zero divisor. Returning 1.0f means "no boost".
        if (numVectors <= 0 || numVectors >= BOOST_REF_SIZE) {
            return 1.0f;
        }
        double sizeScale = Math.pow((double) BOOST_REF_SIZE / numVectors, BOOST_EXPONENT);
        double kScale = Math.pow((double) Math.max(k, 1) / BOOST_K_REF, BOOST_K_EXPONENT);
        double recallScale = 0.1 / (1.0 - DEFAULT_TARGET_RECALL);
        return (float) Math.max(1.0, sizeScale * kScale * recallScale);
    }

    /**
     * Computes a segment-size-aware cap on the visit ratio.
     * Larger segments have better-formed IVF clusters and need a lower visit ratio to achieve the target recall.
     * The power-law curve is calibrated on multi-dataset experiments (GIST-1M, Wiki-Cohere, MSMarco-130M).
     * <p>
     * Formula: cap = {@link #CAP_COEFFICIENT} * ({@link #CAP_REF_SIZE} / numVectors)^{@link #CAP_EXPONENT}
     *              * (0.1 / (1 - targetRecall))
     *
     * @param numVectors number of vectors in the segment
     * @return the upper-bound visit ratio for this segment size
     */
    static float computeSegmentSizeCap(int numVectors) {
        if (numVectors <= 0) {
            return (float) V_MAX;
        }
        double sizeScale = Math.pow((double) CAP_REF_SIZE / numVectors, CAP_EXPONENT);
        double recallScale = 0.1 / (1.0 - DEFAULT_TARGET_RECALL);
        return (float) Math.min(1.0, CAP_COEFFICIENT * sizeScale * recallScale);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        var raw = getReaderForField(fieldInfo.name).getOffHeapByteSize(fieldInfo);
        FieldEntry fe = fields.get(fieldInfo.number);
        if (fe == null) {
            return raw;
        }

        var centroidsClusters = Map.of(centroidExtension, fe.centroidLength, clusterExtension, fe.postingListLength);
        return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, centroidsClusters);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>(genericReaders.allReaders());
        Collections.addAll(closeables, ivfCentroids, ivfClusters);
        IOUtils.close(closeables);
    }

    protected static class FieldEntry implements GenericFlatVectorReaders.Field {
        protected final String rawVectorFormatName;
        protected final boolean useDirectIOReads;
        protected final VectorSimilarityFunction similarityFunction;
        protected final VectorEncoding vectorEncoding;
        protected final int numCentroids;
        protected final long centroidOffset;
        protected final long centroidLength;
        protected final long postingListOffset;
        protected final long postingListLength;
        protected final float[] globalCentroid;
        protected final float globalCentroidDp;
        protected final int bulkSize;

        public FieldEntry(
            String rawVectorFormatName,
            boolean useDirectIOReads,
            VectorSimilarityFunction similarityFunction,
            VectorEncoding vectorEncoding,
            int numCentroids,
            long centroidOffset,
            long centroidLength,
            long postingListOffset,
            long postingListLength,
            float[] globalCentroid,
            float globalCentroidDp,
            int bulkSize
        ) {
            this.rawVectorFormatName = rawVectorFormatName;
            this.useDirectIOReads = useDirectIOReads;
            this.similarityFunction = similarityFunction;
            this.vectorEncoding = vectorEncoding;
            this.numCentroids = numCentroids;
            this.centroidOffset = centroidOffset;
            this.centroidLength = centroidLength;
            this.postingListOffset = postingListOffset;
            this.postingListLength = postingListLength;
            this.globalCentroid = globalCentroid;
            this.globalCentroidDp = globalCentroidDp;
            this.bulkSize = bulkSize;
        }

        @Override
        public String rawVectorFormatName() {
            return rawVectorFormatName;
        }

        @Override
        public boolean useDirectIOReads() {
            return useDirectIOReads;
        }

        public int numCentroids() {
            return numCentroids;
        }

        public float[] globalCentroid() {
            return globalCentroid;
        }

        public float globalCentroidDp() {
            return globalCentroidDp;
        }

        public VectorSimilarityFunction similarityFunction() {
            return similarityFunction;
        }

        public IndexInput centroidSlice(IndexInput centroidFile) throws IOException {
            return centroidFile.slice("centroids", centroidOffset, centroidLength);
        }

        public IndexInput postingListSlice(IndexInput postingListFile) throws IOException {
            return postingListFile.slice("postingLists", postingListOffset, postingListLength);
        }

        public int getBulkSize() {
            return bulkSize;
        }

        public int numSlices() {
            return -1;
        }
    }

    /**
     * Read the raw centroids and cluster sizes for the given field from this segment.
     * Used by the adaptive merge strategy to bootstrap K-means with prior segment centroids.
     * Implementations may return {@code null} if the format does not support reading centroid data
     * (e.g. because the layout differs from the writer that consumes this data).
     *
     * @param fieldName the vector field to read centroids for
     * @return centroid data, or {@code null} if unavailable
     */
    public abstract CentroidData<?> readCentroidData(String fieldName) throws IOException;

    /**
     * Container for centroid data read from an existing segment. The centroid vectors are
     * exposed as a streaming {@link ClusteringFloatVectorValues}
     * so the merge path can iterate them without materializing the full {@code float[N][dim]}
     * on the heap. The optional {@code backing} {@link IndexInput} owns any sliced resources
     * required by the streaming view; {@link #close()} releases it.
     */
    public static final class CentroidData<V> implements Closeable {
        private final int numCentroids;
        private final ClusteringVectorValues<V> centroids;
        private final int[] clusterSizes;
        private final float[] globalCentroid;
        private final IndexInput backing;

        // Note: ESNextDiskBBQVectorsReader.readCentroidData() handles type dispatch (float vs byte) correctly.
        // Other reader implementations (ES940, ES920) should follow the same pattern if they add byte support.
        public CentroidData(ClusteringVectorValues<V> centroids, int[] clusterSizes, float[] globalCentroid, IndexInput backing) {
            assert centroids.size() == clusterSizes.length;
            this.numCentroids = centroids.size();
            this.centroids = centroids;
            this.clusterSizes = clusterSizes;
            this.globalCentroid = globalCentroid;
            this.backing = backing;
        }

        public int numCentroids() {
            return numCentroids;
        }

        public ClusteringVectorValues<V> centroids() {
            return centroids;
        }

        public int[] clusterSizes() {
            return clusterSizes;
        }

        public float[] globalCentroid() {
            return globalCentroid;
        }

        @Override
        public void close() throws IOException {
            if (backing != null) {
                backing.close();
            }
        }
    }

    public abstract PostingVisitor getPostingVisitor(
        FieldInfo fieldInfo,
        KnnVectorValues values,
        IndexInput postingsLists,
        QueryTarget queryTarget,
        Bits needsScoring,
        IndexInput centroidSlice,
        ESAcceptDocs acceptDocs
    ) throws IOException;

    public interface PostingVisitor {
        /** returns the number of documents in the posting list */
        int resetPostingsScorer(PostingMetadata metadata) throws IOException;

        /** returns the number of scored documents */
        int visit(KnnCollector collector) throws IOException;

        /**
         * Enables collection of per-visitor timing breakdowns. Called at most once, before any
         * {@link #visit}/{@link #resetPostingsScorer} call, and only when detailed profiling is active,
         * so timing accumulation stays off the hot path for non-profiled queries.
         */
        default void enableProfiling() {}

        /**
         * The timings accumulated since {@link #enableProfiling()}, or {@code null} when this visitor does
         * not collect them. Read once, after the search loop has finished draining postings.
         */
        @Nullable
        default Profile profile() {
            return null;
        }

        /**
         * A visitor's timing breakdown, harvested once at the end of a search.
         *
         * @param docIdReadTimeNs         accumulated time reading and decoding doc IDs
         * @param scoringTimeNs           accumulated time in quantized scoring (SIMD bulk + individual)
         * @param queryQuantizationTimeNs accumulated time quantizing the query vector against each centroid
         * @param centroidReadTimeNs      accumulated time reading centroid vectors in resetPostingsScorer
         * @param scorerImplementation    the scorer implementation family that ran: {@code native},
         *                                {@code panama}, or {@code scalar}
         */
        record Profile(
            long docIdReadTimeNs,
            long scoringTimeNs,
            long queryQuantizationTimeNs,
            long centroidReadTimeNs,
            String scorerImplementation
        ) {}
    }

}

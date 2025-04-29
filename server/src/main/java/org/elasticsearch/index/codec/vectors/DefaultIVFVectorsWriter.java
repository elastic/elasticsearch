/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.INDEX_BITS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.discretize;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.packAsBinary;
import static org.elasticsearch.index.codec.vectors.IVFVectorsFormat.IVF_VECTOR_COMPONENT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

/**
 * Default implementation of {@link IVFVectorsWriter}. It uses {@link KMeans} algorithm to
 * partition the vector space, and then stores the centroids an posting list in a sequential
 * fashion.
 */
public class DefaultIVFVectorsWriter extends IVFVectorsWriter {

  static final float SOAR_LAMBDA = 1.0f;
  // What percentage of the centroids do we do a second check on for SOAR assignment
  static final float EXT_SOAR_LIMIT_CHECK_RATIO = 0.10f;

  private final int vectorPerCluster;

  private final OptimizedScalarQuantizer.QuantizationResult[] corrections =
      new OptimizedScalarQuantizer.QuantizationResult[ES91OSQVectorsScorer.BULK_SIZE];

  public DefaultIVFVectorsWriter(
      SegmentWriteState state, FlatVectorsWriter rawVectorDelegate, int vectorPerCluster)
      throws IOException {
    super(state, rawVectorDelegate);
    this.vectorPerCluster = vectorPerCluster;
  }

  @Override
  CentroidAssignmentScorer calculateAndWriteCentroids(
      FieldInfo fieldInfo,
      FloatVectorValues floatVectorValues,
      IndexOutput centroidOutput,
      float[] globalCentroid)
      throws IOException {
    if (floatVectorValues.size() == 0) {
      return CentroidAssignmentScorer.EMPTY;
    }
    // calculate the centroids
    int maxNumClusters = ((floatVectorValues.size() - 1) / vectorPerCluster) + 1;
    int desiredClusters =
        (int)
            Math.max(
                maxNumClusters / 16.0,
                Math.max(Math.sqrt(floatVectorValues.size()), maxNumClusters));
    if (floatVectorValues.size() / desiredClusters > vectorPerCluster) {
      desiredClusters = ((floatVectorValues.size() - 1) / vectorPerCluster) + 1;
    }
    final KMeans.Results kMeans =
        KMeans.cluster(
            floatVectorValues,
            desiredClusters,
            false,
            42L,
            KMeans.KmeansInitializationMethod.PLUS_PLUS,
            null,
            fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE,
            1,
            15,
            desiredClusters * 256);
    float[][] centroids = kMeans.centroids();
    // write them
    writeCentroids(centroids, fieldInfo, globalCentroid, centroidOutput);
    return new OnHeapCentroidAssignmentScorer(centroids);
  }

  @Override
  long[] buildAndWritePostingsLists(
      FieldInfo fieldInfo,
      InfoStream infoStream,
      CentroidAssignmentScorer randomCentroidScorer,
      FloatVectorValues floatVectorValues,
      IndexOutput postingsOutput)
      throws IOException {
    IntArrayList[] clusters = new IntArrayList[randomCentroidScorer.size()];
    for (int i = 0; i < randomCentroidScorer.size(); i++) {
      clusters[i] = new IntArrayList(floatVectorValues.size() / randomCentroidScorer.size() / 4);
    }
    assignCentroids(randomCentroidScorer, floatVectorValues, clusters);
    if (infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      printClusterQualityStatistics(clusters, infoStream);
    }
    // write the posting lists
    final long[] offsets = new long[randomCentroidScorer.size()];
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    BinarizedFloatVectorValues binarizedByteVectorValues =
        new BinarizedFloatVectorValues(floatVectorValues, quantizer);
    DocIdsWriter docIdsWriter = new DocIdsWriter();
    for (int i = 0; i < randomCentroidScorer.size(); i++) {
      float[] centroid = randomCentroidScorer.centroid(i);
      binarizedByteVectorValues.centroid = centroid;
      // TODO sort by distance to the centroid
      IntArrayList cluster = clusters[i];
      // TODO align???
      offsets[i] = postingsOutput.getFilePointer();
      int size = cluster.size();
      postingsOutput.writeVInt(size);
      postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
      // TODO we might want to consider putting the docIds in a separate file
      //  to aid with only having to fetch vectors from slower storage when they are required
      //  keeping them in the same file indicates we pull the entire file into cache
      docIdsWriter.writeDocIds(
          j -> floatVectorValues.ordToDoc(cluster.get(j)), cluster.size(), postingsOutput);
      writePostingList(cluster, postingsOutput, binarizedByteVectorValues);
    }
    return offsets;
  }

  private void writePostingList(
      IntArrayList cluster,
      IndexOutput postingsOutput,
      BinarizedFloatVectorValues binarizedByteVectorValues)
      throws IOException {
    int limit = cluster.size() - ES91OSQVectorsScorer.BULK_SIZE + 1;
    int cidx = 0;
    // Write vectors in bulks of ES91OSQVectorsScorer.BULK_SIZE.
    for (; cidx < limit; cidx += ES91OSQVectorsScorer.BULK_SIZE) {
      for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
        int ord = cluster.get(cidx + j);
        byte[] binaryValue = binarizedByteVectorValues.vectorValue(ord);
        // write vector
        postingsOutput.writeBytes(binaryValue, 0, binaryValue.length);
        corrections[j] = binarizedByteVectorValues.getCorrectiveTerms(ord);
      }
      // write corrections
      for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
        postingsOutput.writeInt(Float.floatToIntBits(corrections[j].lowerInterval()));
      }
      for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
        postingsOutput.writeInt(Float.floatToIntBits(corrections[j].upperInterval()));
      }
      for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
        int targetComponentSum = corrections[j].quantizedComponentSum();
        assert targetComponentSum >= 0 && targetComponentSum <= 0xffff;
        postingsOutput.writeShort((short) targetComponentSum);
      }
      for (int j = 0; j < ES91OSQVectorsScorer.BULK_SIZE; j++) {
        postingsOutput.writeInt(Float.floatToIntBits(corrections[j].additionalCorrection()));
      }
    }
    // write tail
    for (; cidx < cluster.size(); cidx++) {
      int ord = cluster.get(cidx);
      // write vector
      byte[] binaryValue = binarizedByteVectorValues.vectorValue(ord);
      OptimizedScalarQuantizer.QuantizationResult corrections =
          binarizedByteVectorValues.getCorrectiveTerms(ord);
      writeQuantizedValue(postingsOutput, binaryValue, corrections);
      binarizedByteVectorValues.getCorrectiveTerms(ord);
      postingsOutput.writeBytes(binaryValue, 0, binaryValue.length);
      postingsOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
      postingsOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
      postingsOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
      assert corrections.quantizedComponentSum() >= 0
          && corrections.quantizedComponentSum() <= 0xffff;
      postingsOutput.writeShort((short) corrections.quantizedComponentSum());
    }
  }

  @Override
  CentroidAssignmentScorer createCentroidScorer(
      IndexInput centroidsInput, int numCentroids, FieldInfo fieldInfo, float[] globalCentroid)
      throws IOException {
    return new OffHeapCentroidAssignmentScorer(centroidsInput, numCentroids, fieldInfo);
  }

  static void writeCentroids(
      float[][] centroids, FieldInfo fieldInfo, float[] globalCentroid, IndexOutput centroidOutput)
      throws IOException {
    final OptimizedScalarQuantizer osq =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    byte[] quantizedScratch = new byte[fieldInfo.getVectorDimension()];
    float[] centroidScratch = new float[fieldInfo.getVectorDimension()];
    // TODO do we want to store these distances as well for future use?
    float[] distances = new float[centroids.length];
    for (int i = 0; i < centroids.length; i++) {
      distances[i] = VectorUtil.squareDistance(centroids[i], globalCentroid);
    }
    // sort the centroids by distance to globalCentroid, nearest (smallest distance), to furthest
    // (largest)
    for (int i = 0; i < centroids.length; i++) {
      for (int j = i + 1; j < centroids.length; j++) {
        if (distances[i] > distances[j]) {
          float[] tmp = centroids[i];
          centroids[i] = centroids[j];
          centroids[j] = tmp;
          float tmpDistance = distances[i];
          distances[i] = distances[j];
          distances[j] = tmpDistance;
        }
      }
    }
    for (float[] centroid : centroids) {
      System.arraycopy(centroid, 0, centroidScratch, 0, centroid.length);
      OptimizedScalarQuantizer.QuantizationResult result =
          osq.scalarQuantize(centroidScratch, quantizedScratch, (byte) 4, globalCentroid);
      writeQuantizedValue(centroidOutput, quantizedScratch, result);
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(fieldInfo.getVectorDimension() * Float.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN);
    for (float[] centroid : centroids) {
      buffer.asFloatBuffer().put(centroid);
      centroidOutput.writeBytes(buffer.array(), buffer.array().length);
    }
  }

  record SegmentCentroid(int segment, int centroid, int centroidSize) {}

  @Override
  protected int calculateAndWriteCentroids(
      FieldInfo fieldInfo,
      FloatVectorValues floatVectorValues,
      IndexOutput temporaryCentroidOutput,
      MergeState mergeState,
      float[] globalCentroid)
      throws IOException {
    if (floatVectorValues.size() == 0) {
      return 0;
    }
    int desiredClusters = ((floatVectorValues.size() - 1) / vectorPerCluster) + 1;
    // init centroids from merge state
    List<FloatVectorValues> centroidList = new ArrayList<>();
    List<SegmentCentroid> segmentCentroids = new ArrayList<>(desiredClusters);

    int segmentIdx = 0;
    long startTime = System.nanoTime();
    for (var reader : mergeState.knnVectorsReaders) {
      IVFVectorsReader ivfVectorsReader = IVFVectorsFormat.getIVFReader(reader, fieldInfo.name);
      if (ivfVectorsReader == null) {
        continue;
      }

      FloatVectorValues centroid = ivfVectorsReader.getCentroids(fieldInfo);
      centroidList.add(centroid);
      for (int i = 0; i < centroid.size(); i++) {
        int size = ivfVectorsReader.centroidSize(fieldInfo.name, i);
        segmentCentroids.add(new SegmentCentroid(segmentIdx, i, size));
      }
      segmentIdx++;
    }

    // sort centroid list by floatvector size
    FloatVectorValues baseSegment = centroidList.get(0);
    for (var l : centroidList) {
      if (l.size() > baseSegment.size()) {
        baseSegment = l;
      }
    }
    float[] scratch = new float[fieldInfo.getVectorDimension()];
    float minimumDistance = Float.MAX_VALUE;
    for (int j = 0; j < baseSegment.size(); j++) {
      System.arraycopy(baseSegment.vectorValue(j), 0, scratch, 0, baseSegment.dimension());
      for (int k = j + 1; k < baseSegment.size(); k++) {
        float d = VectorUtil.squareDistance(scratch, baseSegment.vectorValue(k));
        if (d < minimumDistance) {
          minimumDistance = d;
        }
      }
    }
    if (mergeState.infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      mergeState.infoStream.message(
          IVF_VECTOR_COMPONENT,
          "Agglomerative cluster min distance: "
              + minimumDistance
              + " From biggest segment: "
              + baseSegment.size());
    }
    int[] labels = new int[segmentCentroids.size()];
    // loop over segments
    int clusterIdx = 0;
    // keep track of all inter-centroid distances,
    // using less than centroid * centroid space (e.g. not keeping track of duplicates)
    for (int i = 0; i < segmentCentroids.size(); i++) {
      if (labels[i] == 0) {
        clusterIdx += 1;
        labels[i] = clusterIdx;
      }
      SegmentCentroid segmentCentroid = segmentCentroids.get(i);
      System.arraycopy(
          centroidList.get(segmentCentroid.segment()).vectorValue(segmentCentroid.centroid),
          0,
          scratch,
          0,
          baseSegment.dimension());
      for (int j = i + 1; j < segmentCentroids.size(); j++) {
        float d =
            VectorUtil.squareDistance(
                scratch,
                centroidList
                    .get(segmentCentroids.get(j).segment())
                    .vectorValue(segmentCentroids.get(j).centroid()));
        if (d < minimumDistance / 2) {
          if (labels[j] == 0) {
            labels[j] = labels[i];
          } else {
            for (int k = 0; k < labels.length; k++) {
              if (labels[k] == labels[j]) {
                labels[k] = labels[i];
              }
            }
          }
        }
      }
    }
    float[][] initCentroids = new float[clusterIdx][fieldInfo.getVectorDimension()];
    int[] sum = new int[clusterIdx];
    for (int i = 0; i < segmentCentroids.size(); i++) {
      SegmentCentroid segmentCentroid = segmentCentroids.get(i);
      int label = labels[i];
      FloatVectorValues segment = centroidList.get(segmentCentroid.segment());
      float[] vector = segment.vectorValue(segmentCentroid.centroid);
      for (int j = 0; j < vector.length; j++) {
        initCentroids[label - 1][j] += (vector[j] * segmentCentroid.centroidSize);
      }
      sum[label - 1] += segmentCentroid.centroidSize;
    }
    for (int i = 0; i < initCentroids.length; i++) {
      for (int j = 0; j < initCentroids[i].length; j++) {
        initCentroids[i][j] /= sum[i];
      }
    }
    if (mergeState.infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      mergeState.infoStream.message(
          IVF_VECTOR_COMPONENT,
          "Agglomerative cluster time ms: " + ((System.nanoTime() - startTime) / 1000000.0));
      mergeState.infoStream.message(
          IVF_VECTOR_COMPONENT,
          "Gathered initCentroids:" + initCentroids.length + " for desired: " + desiredClusters);
    }

    // FIXME: still split to get to desired cluster count?
    // FIXME: need a way to maintain the original mapping ... update KMeans to allow maintaining
    // that mapping
    // FIXME: go update the assignCentroids code to respect that mapping from prior centroid to next
    // centroid (via the scorer?)
    // FIXME: run a custom version of kmeans that adjusts the centroids that were split related to
    // only the sets of vectors that were previously associated with the prior centroids
    // FIXME: compare this kmeans outcome with a lot of iterations with the outcome of the process
    // detailed above; ideally a large run of kmeans is approximated by the above algorithm
    long nanoTime = System.nanoTime();
    final KMeans.Results kMeans =
        KMeans.cluster(
            floatVectorValues,
            desiredClusters,
            false,
            42L,
            KMeans.KmeansInitializationMethod.PLUS_PLUS,
            initCentroids,
            fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE,
            1,
            5,
            desiredClusters * 64);
    if (mergeState.infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      mergeState.infoStream.message(
          IVF_VECTOR_COMPONENT, "KMeans time ms: " + ((System.nanoTime() - nanoTime) / 1000000.0));
    }
    float[][] centroids = kMeans.centroids();

    // write them
    writeCentroids(centroids, fieldInfo, globalCentroid, temporaryCentroidOutput);
    return centroids.length;
  }

  @Override
  long[] buildAndWritePostingsLists(
      FieldInfo fieldInfo,
      CentroidAssignmentScorer centroidAssignmentScorer,
      FloatVectorValues floatVectorValues,
      IndexOutput postingsOutput,
      MergeState mergeState)
      throws IOException {
    IntArrayList[] clusters = new IntArrayList[centroidAssignmentScorer.size()];
    for (int i = 0; i < centroidAssignmentScorer.size(); i++) {
      clusters[i] =
          new IntArrayList(floatVectorValues.size() / centroidAssignmentScorer.size() / 4);
    }
    long nanoTime = System.nanoTime();
    assignCentroidsMerge(
        centroidAssignmentScorer, floatVectorValues, mergeState, fieldInfo.name, clusters);
    if (mergeState.infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      mergeState.infoStream.message(
          IVF_VECTOR_COMPONENT,
          "assignCentroids time ms: " + ((System.nanoTime() - nanoTime) / 1000000.0));
    }

    if (mergeState.infoStream.isEnabled(IVF_VECTOR_COMPONENT)) {
      printClusterQualityStatistics(clusters, mergeState.infoStream);
    }
    // write the posting lists
    final long[] offsets = new long[centroidAssignmentScorer.size()];
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    BinarizedFloatVectorValues binarizedByteVectorValues =
        new BinarizedFloatVectorValues(floatVectorValues, quantizer);
    DocIdsWriter docIdsWriter = new DocIdsWriter();
    for (int i = 0; i < centroidAssignmentScorer.size(); i++) {
      float[] centroid = centroidAssignmentScorer.centroid(i);
      binarizedByteVectorValues.centroid = centroid;
      // TODO: sort by distance to the centroid
      IntArrayList cluster = clusters[i];
      // TODO align???
      offsets[i] = postingsOutput.getFilePointer();
      int size = cluster.size();
      postingsOutput.writeVInt(size);
      postingsOutput.writeInt(Float.floatToIntBits(VectorUtil.dotProduct(centroid, centroid)));
      // TODO we might want to consider putting the docIds in a separate file
      //  to aid with only having to fetch vectors from slower storage when they are required
      //  keeping them in the same file indicates we pull the entire file into cache
      docIdsWriter.writeDocIds(
          j -> floatVectorValues.ordToDoc(cluster.get(j)), size, postingsOutput);
      writePostingList(cluster, postingsOutput, binarizedByteVectorValues);
    }
    return offsets;
  }

  private static void printClusterQualityStatistics(
      IntArrayList[] clusters, InfoStream infoStream) {
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    float mean = 0;
    float m2 = 0;
    // iteratively compute the variance & mean
    int count = 0;
    for (IntArrayList cluster : clusters) {
      count += 1;
      if (cluster == null) {
        continue;
      }
      float delta = cluster.size() - mean;
      mean += delta / count;
      m2 += delta * (cluster.size() - mean);
      min = Math.min(min, cluster.size());
      max = Math.max(max, cluster.size());
    }
    float variance = m2 / (clusters.length - 1);
    infoStream.message(
        IVF_VECTOR_COMPONENT,
        "Centroid count: "
            + clusters.length
            + " min: "
            + min
            + " max: "
            + max
            + " mean: "
            + mean
            + " stdDev: "
            + Math.sqrt(variance)
            + " variance: "
            + variance);
  }

  static void assignCentroids(
      CentroidAssignmentScorer scorer, FloatVectorValues vectors, IntArrayList[] clusters)
      throws IOException {
    short numCentroids = (short) scorer.size();
    // If soar > 0, then we actually need to apply the projection, otherwise, its just the second
    // nearest centroid
    // we at most will look at the EXT_SOAR_LIMIT_CHECK_RATIO nearest centroids if possible
    int soarToCheck = (int) (numCentroids * EXT_SOAR_LIMIT_CHECK_RATIO);
    int soarClusterCheckCount = Math.min(numCentroids - 1, soarToCheck);
    // if lambda is `0`, that just means overspill to the second nearest, so we will only check the
    // second nearest
    if (SOAR_LAMBDA == 0) {
      soarClusterCheckCount = Math.min(1, soarClusterCheckCount);
    }
    NeighborQueue neighborsToCheck = new NeighborQueue(soarClusterCheckCount + 1, true);
    OrdScoreIterator ordScoreIterator = new OrdScoreIterator(soarClusterCheckCount + 1);
    float[] scratch = new float[vectors.dimension()];
    for (int docID = 0; docID < vectors.size(); docID++) {
      float[] vector = vectors.vectorValue(docID);
      scorer.setScoringVector(vector);
      int bestCentroid = 0;
      float bestScore = Float.MAX_VALUE;
      if (numCentroids > 1) {
        for (short c = 0; c < numCentroids; c++) {
          float squareDist = scorer.score(c);
          neighborsToCheck.insertWithOverflow(c, squareDist);
        }
        // pop the best
        int sz = neighborsToCheck.size();
        int best =
            neighborsToCheck.consumeNodesAndScoresMin(
                ordScoreIterator.ords, ordScoreIterator.scores);
        // TODO yikes....
        ordScoreIterator.idx = sz;
        bestScore = ordScoreIterator.getScore(best);
        bestCentroid = ordScoreIterator.getOrd(best);
      }
      if (clusters[bestCentroid] == null) {
        clusters[bestCentroid] = new IntArrayList(16);
      }
      clusters[bestCentroid].add(docID);
      if (soarClusterCheckCount > 0) {
        assignCentroidSOAR(
            ordScoreIterator,
            docID,
            bestCentroid,
            scorer.centroid(bestCentroid),
            bestScore,
            scratch,
            scorer,
            vectors,
            clusters);
      }
      neighborsToCheck.clear();
    }
  }

  static int prefilterCentroidAssignment(
      int centroidOrd,
      FloatVectorValues segmentCentroids,
      CentroidAssignmentScorer scorer,
      NeighborQueue neighborsToCheck,
      int[] prefilteredCentroids)
      throws IOException {
    float[] segmentCentroid = segmentCentroids.vectorValue(centroidOrd);
    scorer.setScoringVector(segmentCentroid);
    neighborsToCheck.clear();
    for (short c = 0; c < scorer.size(); c++) {
      float squareDist = scorer.score(c);
      neighborsToCheck.insertWithOverflow(c, squareDist);
    }
    int size = neighborsToCheck.size();
    neighborsToCheck.consumeNodes(prefilteredCentroids);
    return size;
  }

  static void assignCentroidsMerge(
      CentroidAssignmentScorer scorer,
      FloatVectorValues vectors,
      MergeState state,
      String fieldName,
      IntArrayList[] clusters)
      throws IOException {
    FixedBitSet assigned = new FixedBitSet(vectors.size() + 1);
    short numCentroids = (short) scorer.size();
    // If soar > 0, then we actually need to apply the projection, otherwise, its just the second
    // nearest centroid
    // we at most will look at the EXT_SOAR_LIMIT_CHECK_RATIO nearest centroids if possible
    int soarToCheck = (int) (numCentroids * EXT_SOAR_LIMIT_CHECK_RATIO);
    int soarClusterCheckCount = Math.min(numCentroids - 1, soarToCheck);
    // TODO is this the right to check?
    //   If cluster quality is higher, maybe we can reduce this...
    int prefilteredCentroidCount =
        Math.max(soarClusterCheckCount + 1, numCentroids / state.knnVectorsReaders.length);
    NeighborQueue prefilteredCentroidsToCheck = new NeighborQueue(prefilteredCentroidCount, true);
    NeighborQueue neighborsToCheck = new NeighborQueue(soarClusterCheckCount + 1, true);
    OrdScoreIterator ordScoreIterator = new OrdScoreIterator(soarClusterCheckCount + 1);
    int[] prefilteredCentroids = new int[prefilteredCentroidCount];
    float[] scratch = new float[vectors.dimension()];
    // Can we do a pre-filter by finding the nearest centroids to the original vector centroids?
    for (int idx = 0; idx < state.knnVectorsReaders.length; idx++) {
      KnnVectorsReader reader = state.knnVectorsReaders[idx];
      IVFVectorsReader vectorsReader = getIVFReader(reader, fieldName);
      // No reader, skip
      if (vectorsReader == null) {
        continue;
      }
      MergeState.DocMap docMap = state.docMaps[idx];
      var segmentCentroids = vectorsReader.getCentroids(state.fieldInfos[idx].fieldInfo(fieldName));
      for (int i = 0; i < segmentCentroids.size(); i++) {
        IVFVectorsReader.CentroidInfo info = vectorsReader.centroidVectors(fieldName, i, docMap);
        // Rare, but empty centroid, no point in doing comparisons
        if (info.vectors().size == 0) {
          continue;
        }
        prefilteredCentroidsToCheck.clear();
        int prefiltedCount =
            prefilterCentroidAssignment(
                i, segmentCentroids, scorer, prefilteredCentroidsToCheck, prefilteredCentroids);
        int centroidVectorDocId = -1;
        while ((centroidVectorDocId = info.vectors().nextVectorDocId()) != NO_MORE_DOCS) {
          if (assigned.getAndSet(centroidVectorDocId)) {
            continue;
          }
          neighborsToCheck.clear();
          float[] vector = info.vectors().vectorValue();
          scorer.setScoringVector(vector);
          int bestCentroid;
          float bestScore;
          for (int c = 0; c < prefiltedCount; c++) {
            float squareDist = scorer.score(prefilteredCentroids[c]);
            neighborsToCheck.insertWithOverflow(prefilteredCentroids[c], squareDist);
          }
          int centroidCount = neighborsToCheck.size();
          int best =
              neighborsToCheck.consumeNodesAndScoresMin(
                  ordScoreIterator.ords, ordScoreIterator.scores);
          // yikes
          ordScoreIterator.idx = centroidCount;
          bestScore = ordScoreIterator.getScore(best);
          bestCentroid = ordScoreIterator.getOrd(best);
          if (clusters[bestCentroid] == null) {
            clusters[bestCentroid] = new IntArrayList(16);
          }
          clusters[bestCentroid].add(info.vectors().docId());
          if (soarClusterCheckCount > 0) {
            assignCentroidSOAR(
                ordScoreIterator,
                info.vectors().docId(),
                bestCentroid,
                scorer.centroid(bestCentroid),
                bestScore,
                scratch,
                scorer,
                vectors,
                clusters);
          }
        }
      }
    }

    for (int vecOrd = 0; vecOrd < vectors.size(); vecOrd++) {
      if (assigned.get(vecOrd)) {
        continue;
      }
      float[] vector = vectors.vectorValue(vecOrd);
      scorer.setScoringVector(vector);
      int bestCentroid = 0;
      float bestScore = Float.MAX_VALUE;
      if (numCentroids > 1) {
        for (short c = 0; c < numCentroids; c++) {
          float squareDist = scorer.score(c);
          neighborsToCheck.insertWithOverflow(c, squareDist);
        }
        int centroidCount = neighborsToCheck.size();
        int bestIdx =
            neighborsToCheck.consumeNodesAndScoresMin(
                ordScoreIterator.ords, ordScoreIterator.scores);
        ordScoreIterator.idx = centroidCount;
        bestCentroid = ordScoreIterator.getOrd(bestIdx);
        bestScore = ordScoreIterator.getScore(bestIdx);
      }
      if (clusters[bestCentroid] == null) {
        clusters[bestCentroid] = new IntArrayList(16);
      }
      int docID = vectors.ordToDoc(vecOrd);
      clusters[bestCentroid].add(docID);
      if (soarClusterCheckCount > 0) {
        assignCentroidSOAR(
            ordScoreIterator,
            docID,
            bestCentroid,
            scorer.centroid(bestCentroid),
            bestScore,
            scratch,
            scorer,
            vectors,
            clusters);
      }
      neighborsToCheck.clear();
    }
  }

  static void assignCentroidSOAR(
      OrdScoreIterator centroidsToCheck,
      int docId,
      int bestCentroidId,
      float[] bestCentroid,
      float bestScore,
      float[] scratch,
      CentroidAssignmentScorer scorer,
      FloatVectorValues vectors,
      IntArrayList[] clusters)
      throws IOException {
    float[] vector = vectors.vectorValue(docId);
    ESVectorUtil.subtract(vector, bestCentroid, scratch);
    int bestSecondaryCentroid = -1;
    float minDist = Float.MAX_VALUE;
    for (int i = 0; i < centroidsToCheck.size(); i++) {
      float score = centroidsToCheck.getScore(i);
      int centroidOrdinal = centroidsToCheck.getOrd(i);
      if (centroidOrdinal == bestCentroidId) {
        continue;
      }
      if (SOAR_LAMBDA > 0) {
        float proj = ESVectorUtil.soarResidual(vector, scorer.centroid(centroidOrdinal), scratch);
        score += SOAR_LAMBDA * proj * proj / bestScore;
      }
      if (score < minDist) {
        bestSecondaryCentroid = centroidOrdinal;
        minDist = score;
      }
    }
    if (bestSecondaryCentroid != -1) {
      clusters[bestSecondaryCentroid].add(docId);
    }
  }

  static class OrdScoreIterator {
    private final int[] ords;
    private final float[] scores;
    private int idx = 0;

    OrdScoreIterator(int size) {
      this.ords = new int[size];
      this.scores = new float[size];
    }

    void add(int ord, float score) {
      ords[idx] = ord;
      scores[idx] = score;
      idx++;
    }

    int getOrd(int idx) {
      return ords[idx];
    }

    float getScore(int idx) {
      return scores[idx];
    }

    void reset() {
      idx = 0;
    }

    int size() {
      return idx;
    }
  }

  // TODO unify with OSQ format
  static class BinarizedFloatVectorValues {
    private OptimizedScalarQuantizer.QuantizationResult corrections;
    private final byte[] binarized;
    private final byte[] initQuantized;
    private float[] centroid;
    private final FloatVectorValues values;
    private final OptimizedScalarQuantizer quantizer;

    private int lastOrd = -1;

    BinarizedFloatVectorValues(FloatVectorValues delegate, OptimizedScalarQuantizer quantizer) {
      this.values = delegate;
      this.quantizer = quantizer;
      this.binarized = new byte[discretize(delegate.dimension(), 64) / 8];
      this.initQuantized = new byte[delegate.dimension()];
    }

    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int ord) {
      if (ord != lastOrd) {
        throw new IllegalStateException(
            "attempt to retrieve corrective terms for different ord "
                + ord
                + " than the quantization was done for: "
                + lastOrd);
      }
      return corrections;
    }

    public byte[] vectorValue(int ord) throws IOException {
      if (ord != lastOrd) {
        binarize(ord);
        lastOrd = ord;
      }
      return binarized;
    }

    private void binarize(int ord) throws IOException {
      corrections =
          quantizer.scalarQuantize(values.vectorValue(ord), initQuantized, INDEX_BITS, centroid);
      packAsBinary(initQuantized, binarized);
    }
  }

  static class OffHeapCentroidAssignmentScorer implements CentroidAssignmentScorer {
    private final IndexInput centroidsInput;
    private final int numCentroids;
    private final int dimension;
    private final float[] scratch;
    private float[] q;
    private final long centroidByteSize;
    private int currOrd = -1;

    OffHeapCentroidAssignmentScorer(IndexInput centroidsInput, int numCentroids, FieldInfo info) {
      this.centroidsInput = centroidsInput;
      this.numCentroids = numCentroids;
      this.dimension = info.getVectorDimension();
      this.scratch = new float[dimension];
      this.centroidByteSize = dimension + 3 * Float.BYTES + Short.BYTES;
    }

    @Override
    public int size() {
      return numCentroids;
    }

    @Override
    public float[] centroid(int centroidOrdinal) throws IOException {
      if (centroidOrdinal == currOrd) {
        return scratch;
      }
      centroidsInput.seek(
          numCentroids * centroidByteSize + (long) centroidOrdinal * dimension * Float.BYTES);
      centroidsInput.readFloats(scratch, 0, dimension);
      this.currOrd = centroidOrdinal;
      return scratch;
    }

    @Override
    public void setScoringVector(float[] vector) {
      q = vector;
    }

    @Override
    public float score(int centroidOrdinal) throws IOException {
      return VectorUtil.squareDistance(centroid(centroidOrdinal), q);
    }
  }

  // TODO throw away rawCentroids
  static class OnHeapCentroidAssignmentScorer implements CentroidAssignmentScorer {
    private final float[][] centroids;
    private float[] q;

    OnHeapCentroidAssignmentScorer(float[][] centroids) {
      this.centroids = centroids;
    }

    @Override
    public int size() {
      return centroids.length;
    }

    @Override
    public void setScoringVector(float[] vector) {
      q = vector;
    }

    @Override
    public float[] centroid(int centroidOrdinal) throws IOException {
      return centroids[centroidOrdinal];
    }

    @Override
    public float score(int centroidOrdinal) throws IOException {
      return VectorUtil.squareDistance(centroid(centroidOrdinal), q);
    }
  }

  static void writeQuantizedValue(
      IndexOutput indexOutput,
      byte[] binaryValue,
      OptimizedScalarQuantizer.QuantizationResult corrections)
      throws IOException {
    indexOutput.writeBytes(binaryValue, binaryValue.length);
    indexOutput.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
    indexOutput.writeInt(Float.floatToIntBits(corrections.upperInterval()));
    indexOutput.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
    assert corrections.quantizedComponentSum() >= 0
        && corrections.quantizedComponentSum() <= 0xffff;
    indexOutput.writeShort((short) corrections.quantizedComponentSum());
  }
}

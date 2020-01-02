/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.codecs.DocValuesProducer;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class KMeansClustering {
    private static final Random random = new Random(42L);
    private static final BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

    public static void runPQKMeansClustering(
            int algorithm,
            DocValuesProducer valuesProducer,
            FieldInfo field,
            float[][][] pcentroids,
            ByteArray[] docCentroids,
            DoubleArray[] docCentroidDists,
            int pcentroidsCount,
            int pqCount,
            int pdims,
            int numIters,
            float sampleFraction) throws IOException {

        // initialize centroids
        int numDocs = 0;
        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytes = values.binaryValue();
            if (numDocs < pcentroidsCount) {
                float[][] pvectors = decodeVector(bytes, pqCount, pdims);
                for (int pq = 0; pq < pqCount; pq++) {
                    pcentroids[pq][numDocs] = pvectors[pq];
                }
            } else if (random.nextDouble() < pcentroidsCount * (1.0 / numDocs)) {
                int c = random.nextInt(pcentroidsCount);
                float[][] pvectors = decodeVector(bytes, pqCount, pdims);
                for (int pq = 0; pq < pqCount; pq++) {
                    pcentroids[pq][c] = pvectors[pq];
                }
            }
            numDocs++;
        }

        //System.out.println("Running k-means with on [" + numDocs + "] docs...");
        if (algorithm == 1) { // Lloyds algorithm
            for (int pq = 0; pq < pqCount; pq++) {
                docCentroids[pq] = bigArrays.newByteArray(numDocs);
            }
            for (int itr = 0; itr < numIters; itr++) {
                float fraction = (itr < numIters - 1) && numDocs > 100_000 ? sampleFraction : 1.0f;
                pcentroids = runKMeansStepLloyds(itr, fraction, valuesProducer, field, pcentroids, docCentroids);
            }
        } else { // KMeans sort algorithm
            double[][][] cdists = new double[pqCount][pcentroidsCount][pcentroidsCount]; // inter-centroid distances
            int[][][] cdistIndexes = new int[pqCount][pcentroidsCount][pcentroidsCount]; // indexes of other centroids sorted by distances
            for (int pq = 0; pq < pqCount; pq++) {
                docCentroids[pq] = bigArrays.newByteArray(numDocs);
                docCentroidDists[pq] = bigArrays.newDoubleArray(numDocs);
            }
            for (int itr = 0; itr < numIters; itr++) {
                float fraction = (itr < numIters - 1) && numDocs > 100_000 ? sampleFraction : 1.0f;
                pcentroids = runKMeansStepSort(itr, fraction, valuesProducer, field,
                    pcentroids, docCentroids, docCentroidDists, cdists, cdistIndexes) ;
            }
        }
    }

    /**
     * Runs one iteration of k-means Lloyds. For each document vector, we first find the
     * nearest centroid, then update the location of the new centroid.
     */
    private static float[][][] runKMeansStepLloyds(int iter,
            float sampleFraction,
            DocValuesProducer valuesProducer,
            FieldInfo field,
            float[][][] centroids,
            ByteArray[] docCentroids) throws IOException {

        int pqCount = centroids.length; // number of quantizers
        int cCount = centroids[0].length; // number of product centroids in each quantizer
        int pdims = centroids[0][0].length; // number of dims in each product centroid
        float[][][] newCentroids = new float[pqCount][cCount][pdims];
        int[][] newCentroidSize = new int[pqCount][cCount];

        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;
        int doc;
        int numDocs = 0;
        BinaryDocValues values = valuesProducer.getBinary(field);
        while ((doc = values.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (random.nextFloat() > sampleFraction) {
                continue;
            }
            numDocs++;
            BytesRef bytes = values.binaryValue();
            float[][] vector = decodeVector(bytes, pqCount, pdims);
            // find best centroid for each vector part
            for (int pq = 0; pq < pqCount; pq++) {
                short bestCentroid = -1;
                double bestDist = Double.MAX_VALUE;
                for (short c = 0; c < cCount; c++) {
                    double dist = l2Squared(centroids[pq][c], vector[pq]);
                    distToOtherCentroids += dist;
                    if (dist < bestDist) {
                        bestCentroid = c;
                        bestDist = dist;
                    }
                }
                newCentroidSize[pq][bestCentroid]++;
                for (int dim = 0; dim < pdims; dim++) {
                    newCentroids[pq][bestCentroid][dim] += vector[pq][dim];
                }
                distToCentroid += bestDist;
                distToOtherCentroids -= bestDist;
                docCentroids[pq].set(doc, (byte) bestCentroid);
            }
        }
        for (int pq = 0; pq < pqCount; pq++) {
            for (int c = 0; c < cCount; c++) {
                if (newCentroidSize[pq][c] > 0) {
                    for (int dim = 0; dim < pdims; dim++) {
                        newCentroids[pq][c][dim] /= newCentroidSize[pq][c];
                    }
                } else {
                    newCentroids[pq][c] = centroids[pq][c]; // keep old centroid
                }
            }
        }
        distToCentroid /= numDocs;
        distToOtherCentroids /= numDocs * (centroids.length - 1);

//        System.out.println("Finished iteration [" + iter + "]. Squared dist to centroid [" + distToCentroid +
//            "], squared dist to other centroids [" + distToOtherCentroids + "].");
        return newCentroids;
    }



    /**
     * Based on the paper:
     * Phillips, Steven J. "Acceleration of k-means and related clustering algorithms."
     * Workshop on Algorithm Engineering and Experimentation. Springer, Berlin, Heidelberg, 2002.
     */
    private static float[][][] runKMeansStepSort(int itr,
           float sampleFraction,
           DocValuesProducer valuesProducer,
           FieldInfo field,
           float[][][] centroids,
           ByteArray[] docCentroids,
           DoubleArray[] docCentroidDists,
           double[][][] cdists,
           int[][][] cdistIndexes) throws IOException {

        int pqCount = centroids.length; // number of quantizers
        int cCount = centroids[0].length; // number of product centroids in each quantizer
        int pdims = centroids[0][0].length; // number of dims in each product centroid
        float[][][] newCentroids = new float[pqCount][cCount][pdims];
        int[][] newCentroidSize = new int[pqCount][cCount];

        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;
        int doc;
        int numDocs = 0;
        BinaryDocValues values = valuesProducer.getBinary(field);
        while ((doc = values.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (random.nextFloat() > sampleFraction) {
                continue;
            }
            numDocs++;
            BytesRef bytes = values.binaryValue();
            float[][] vector = decodeVector(bytes, pqCount, pdims);

            // find best centroid for each vector part
            for (int pq = 0; pq < pqCount; pq++) {
                int bestCentroid = itr == 0 ? -1 : docCentroids[pq].get(doc) & 0xFF; // centroids are stored as unsigned bytes
                double bestDist = itr == 0 ? Double.MAX_VALUE: docCentroidDists[pq].get(doc);
                double prevBestDist = bestDist;
                for (int c = 0; c < cCount; c++) {
                    int cIndex;
                    if (itr == 0) {
                        cIndex = c;
                    } else {
                        cIndex = cdistIndexes[pq][bestCentroid][c]; // consider next closest centroid to bestCentroid
                        if (cdists[pq][bestCentroid][cIndex] >= (4 * prevBestDist)) {
                            // we stop considering all other centroids, as squared distances to them exceed 4 * prevBestDist
                            break;
                        }
                    }
                    double dist = l2Squared(centroids[pq][cIndex], vector[pq]);
                    distToOtherCentroids += dist;
                    if (dist < bestDist) {
                        bestCentroid = cIndex;
                        bestDist = dist;
                    }
                }
                newCentroidSize[pq][bestCentroid]++;
                for (int dim = 0; dim < pdims; dim++) {
                    newCentroids[pq][bestCentroid][dim] += vector[pq][dim];
                }
                distToCentroid += bestDist;
                distToOtherCentroids -= bestDist;
                docCentroids[pq].set(values.docID(), (byte) bestCentroid);
                docCentroidDists[pq].set(values.docID(), bestDist);
            }
        }
        for (int pq = 0; pq < pqCount; pq++) {
            for (int c = 0; c < cCount; c++) {
                if (newCentroidSize[pq][c] > 0) {
                    for (int dim = 0; dim < pdims; dim++) {
                        newCentroids[pq][c][dim] /= newCentroidSize[pq][c];
                    }
                } else {
                    newCentroids[pq][c] = centroids[pq][c]; // keep old centroid
                }
            }
            for (int i = 0; i < cCount; i++) {
                for (int j = 0; j < i; j++) {
                    cdists[pq][i][j] = cdists[pq][j][i];
                }
                for (int j = i+1; j < cCount; j++) {
                    cdists[pq][i][j] = l2Squared(newCentroids[pq][i], newCentroids[pq][j]);
                }
                sortDistIndexes(cdists[pq][i], cdistIndexes[pq][i], cCount);
            }
        }
        distToCentroid /= numDocs;
        distToOtherCentroids /= numDocs * (cCount - 1);


//        System.out.println("Finished iteration [" + itr + "]. Dist to centroid [" + distToCentroid +
//            "], dist to other centroids [" + distToOtherCentroids + "].");
        return newCentroids;
    }

    public static void sortDistIndexes(double[] srcDists, int[] indexes, int n) {
        double[] dists = new double[srcDists.length];
        System.arraycopy(srcDists, 0, dists, 0, dists.length);
        for (int i = 0 ; i < indexes.length; i++) {
            indexes[i] = i;
        }

        new InPlaceMergeSorter() {
            @Override
            public int compare(int i, int j) {
                return Double.compare(dists[i], dists[j]);
            }

            @Override
            public void swap(int i, int j) {
                double tempDist = dists[i];
                dists[i] = dists[j];
                dists[j] = tempDist;

                int tempIndex = indexes[i];
                indexes[i] = indexes[j];
                indexes[j] = tempIndex;
            }
        }.sort(0, n);
    }


    // break vector into <pqCount> parts
    private static float[][] decodeVector(BytesRef bytes, int pqCount, int pdims) {
        float[][] vector = new float[pqCount][pdims];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
        for (int pq = 0; pq < pqCount; pq ++) {
            for (int dim = 0; dim < pdims; dim++) {
                vector[pq][dim] = byteBuffer.getFloat();
            }
        }
        return vector;
    }

    private static double l2Squared(float[] v1, float[] v2) {
        double dist = 0;
        for (int dim = 0; dim < v1.length; dim++) {
            float dif = v1[dim] - v2[dim];
            dist += dif * dif;
        }
        return dist;
    }

}

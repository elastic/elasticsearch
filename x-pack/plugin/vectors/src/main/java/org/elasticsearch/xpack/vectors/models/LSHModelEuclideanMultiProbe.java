/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.models;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.Randomness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * Implementation of the multi-probe LSH based on the papers:
 *  1. Datar, Mayur, et al. "Locality-sensitive hashing scheme based on p-stable distributions."
 *  Proceedings of the twentieth annual symposium on Computational geometry. ACM, 2004.
 *  2. Lv, Qin, et al. "Multi-probe LSH: efficient indexing for high-dimensional similarity search."
 *  Proceedings of the 33rd international conference on Very large data bases. VLDB Endowment, 2007.
 *
 *  Given the user's provided parameters: D (number of dims), L(number of hash tables),
 *  K(number of hash functions in each hash table) and W (with of the projection), we:
 *  - generate L * K number of random a and b parameters for the L * K number of hash functions in the form (a * X + b)/W.
 *      Each hash function hashes a vector to integer.
 *  - compute pertSets — sets of indexes in increasing order of their scores precomputed based on the scores' expectations
 *  Thus, the model contains and stores the following numbers: D, L, K, W, array for a, array for b, and array for pertSets.
 *
 *  Hashing a document vector:
 *  - each hash function hashes a vector to an integer
 *  - to hash a vector, we combine results from K hash functions to get a byte array of K integers.
 *      Increasing K increases precision, as for 2 vectors to match, all their K integers must match.
 *  - we hash each vector this way L times (each time using different K hash functions). This corresponds to L hash tables.
 *      We add to the vector's K integer bytes another 2 bytes to encode the hash table number (0,1,..L-1) and get a BytesRef from it.
 *      Increasing L increases recall. During matching we use Bool SHOULD condition for each of L hashes.
 *  - the resulted L hashes(BytesRef) is how the vector will be indexed into a Lucene field.
 *
 *  Hashing a query vector:
 *  - a query vector is hashed like a document vector: L number of hashes
 *      plus L * (numProbes - 1) number of other hashes.
 *
 */

public class LSHModelEuclideanMultiProbe {
    private static final byte MAX_PERT_SET_LENGTH = 20; // max numb of neighboring hash cells in each hash table to consider as candidates
    private static final byte INT_BYTES = 4;

    private final int D; // number of vector dimensions
    private final short L; // number of hash tables
    private final int K; // number of hash functions in each hash table
    private final float W; // width of the projection
    private final float[][][] a; // parameters for LSH functions
    private final float[][] b; // parameters for LSH functions
    private int[][] pertSets; // perturbation sets

    public LSHModelEuclideanMultiProbe(short lp, int kp, float wp, int dp) {
        this.L = lp;
        this.K = kp;
        this.W = wp;
        this.D = dp;

        // generate parameters for L * K  number of LSH hash functions
        // each hash function in the form of h(v) = (a * v + b) / W
        // where a - D-dimensional vector whose entries are chosen independently from a p-stable distribution
        // b - a real number chosen uniformly from the range [0, W]
        final Random rnd = Randomness.get();
        a = new float[L][K][D];
        b = new float[L][K];

        for (int l = 0; l < L; l++) {
            for (int k = 0; k < K; k++) {
                b[l][k] = rnd.nextFloat() * W; // a float value chosen uniformly from the range [0, W]
                for (int dim = 0; dim < D; dim++) {
                    a[l][k][dim] = (float) rnd.nextGaussian();
                }
            }
        }

        // generate 2 * K expected perturbation scores(see Ch 4.5 of the paper Lv, Qin, et al)
        // formula for scores j from [1, K]:  W^2 * j * (j + 1) / (4 * (K + 1) * (K + 2))
        // formula for scores j from [K+1, 2K]: W^2 * (1 - (2*K + 1 - j)/(K + 1) + (2*K + 1 - j)(2*K + 2 - j) / (4 * ( K+ 1) * (K + 2))
        //   which can be reduced as: W^2 * (2 - 2 * K  + 5 * j + j * j) / (4 * ( K+ 1) * (K + 2))
        float temp1 = 1.0f * W * W / (4 * (K + 1) * (K + 2));
        float temp2 = temp1 * 2 * (1 - K);
        float[] expectPertScores = new float[2 * K];
        for (int j = 1; j <= K; j++) {
            expectPertScores[j - 1] = j * (j + 1) * temp1;
        }
        for (int j = K + 1; j <= 2 * K; j++) {
            expectPertScores[j - 1] = temp2 + temp1 * j * (j + 5) ;
        }

        // generate the perturbation set sorted by scores (see algorithm 1 in Ch 4.4 of the paper Lv, Qin, et al)
        pertSets = new int[MAX_PERT_SET_LENGTH][];
        boolean validPSFound;
        PriorityQueue<Pair> meanHeap = new PriorityQueue<>(Comparator.comparingDouble(Pair::getScore));
        float score = expectPertScores[0];
        List<Integer> pertSet = List.of(0);
        Pair pair = new Pair(score, pertSet);
        meanHeap.add(pair);
        for (int i = 0; i < MAX_PERT_SET_LENGTH; i++) {
            do {
                pair = meanHeap.poll();
                pertSet = pair.getPertSet();
                validPSFound = isValidPertSet(pertSet);

                List<Integer> pertSetS = shift(pertSet);
                if (pertSetS != null) {
                    pair = new Pair(getScore(expectPertScores, pertSetS), pertSetS);
                    meanHeap.add(pair);
                }
                List<Integer> pertSetE = expand(pertSet);
                if (pertSetE != null) {
                    pair = new Pair(getScore(expectPertScores, pertSetE), pertSetE);
                    meanHeap.add(pair);
                }
            } while ((validPSFound == false) && (meanHeap.peek() != null));
            if (validPSFound) {
                pertSets[i] = pertSet.stream().mapToInt(e -> e).toArray();
            } else {
                // we have found only i number of pert sets; resize pertSets
                pertSets = Arrays.copyOf(pertSets, i);
                break;
            }
        }
    }

    public LSHModelEuclideanMultiProbe(short l, int k, float w, int d, float[][][] a, float[][] b, int[][] pertSets) {
        this.L = l;
        this.K = k;
        this.W = w;
        this.D = d;
        this.a = a;
        this.b = b;
        this.pertSets = pertSets;
    }


    /**
     * Hash a vector
     * each hash is in the form: LiLi|H1H1H1|H2H2H2H2|....HkHkHkHk
     * @param vector - vector to hash
     * @return L hashes for L hash tables
     */
    public BytesRef[] hash(float[] vector) {
        BytesRef[] hashes = new BytesRef[L];
        for (int l = 0; l < L; l++) {
            int h[] = new int[K];
            // for each hash function calculate value as h(vector) = (a * vector + b) / W
            for (int k = 0; k < K; k++) {
                float dotProduct = 0;
                for (int dim = 0; dim < D; dim++) {
                    dotProduct += a[l][k][dim] * vector[dim];
                }
                h[k] = (int) Math.floor((dotProduct + b[l][k]) / W);
            }
            hashes[l] = encodeHash(l, h);
        }
        return hashes;
    }


    /**
     * Hash a query vector
     * each hash is in the form: LiLi|H1H1H1|H2H2H2H2|....HkHkHkHk
     * - first two bytes to encode i-th L table
     * - followed by K number of 4 bytes hashes for each of K-hash functions
     * @param numProbes - number of probing hashes per table.
     *    numProbes = 1 means that the only search candidate terms are the hashes from the vector
     *    numProbes greater than 1 means in addition to the vector hashes we add best neighboring hashes to the candidate terms list
     * @param vector - vector to hash
     * @return L*numProbes hashes for L hash tables
     */
    public BytesRef[] hashMultiProbe(float[] vector, int numProbes) {
        assert numProbes >= 1;
        if (numProbes == 1) {
            return hash(vector);
        }
        if (numProbes > (pertSets.length + 1)) {
            throw new IllegalArgumentException("Number of probes can't be greater than [" + (pertSets.length + 1) + "]");
        }

        BytesRef[] hashes = new BytesRef[L * numProbes];
        int hashIndex = 0;
        for (int l = 0; l < L; l++) {
            float[] scores = new float[K * 2]; // scores for query-directed probing
            int [] indexes = new int[K * 2]; // scores' indexes
            int h[] = new int[K]; // hashes from K hash functions

            for (int k = 0; k < K; k++) {
                // calculate hash h(vector) = (a * vector + b) / W
                float dotProduct = 0;
                for (int dim = 0; dim < D; dim++) {
                    dotProduct += a[l][k][dim] * vector[dim];
                }
                float f = dotProduct + b[l][k];
                h[k] = (int) Math.floor(f / W);

                // calculate scores for query-directed probing
                scores[k] = f - h[k] * W; // how close query to the right boundary
                indexes[k] = k;
                int oppositeIndx = 2 * K - 1 - k;
                scores[oppositeIndx] = W - scores[k]; // how close query to the left boundary
                indexes[oppositeIndx] = oppositeIndx;
            }
            hashes[hashIndex++] = encodeHash(l, h);

            //sort indexes in the increasing order of their corresponding scores
            sortScoresIndexes(indexes, scores);

            // for the current hash find <numProbes-1> number of best neighboring hashes (hashes that could be the best search candidates)
            // best candidates are neighbours of <h[]> obtained by +/-1 to h's values beginning with indexes with the least scores
            // pertSets represents sets of indexes in increasing order of their scores, we map them to real indexes
            for (int prob = 0; prob < numProbes - 1; prob++) {
                int hNeigb[] = Arrays.copyOf(h, h.length);
                for (int index : pertSets[prob]) {
                    if ((indexes[index]) < K) {
                        hNeigb[indexes[index]] -= 1;
                    } else {
                        hNeigb[2 * K - 1 - indexes[index]] += 1;
                    }
                }
                hashes[hashIndex++] = encodeHash(l, hNeigb);
            }
        }
        return hashes;
    }

    /**
     * Construct a hash of the vector for the given hash table
     * each hash is in the form: LiLi|H1H1H1|H2H2H2H2|....HkHkHkHk
     * - first two bytes to encode the hash table number //TODO: investigate if this is necessary
     * - followed by K number of 4 bytes hashes for each of K-hash functions
     * @return BytesRef representing a hash
     */
    private BytesRef encodeHash(int tableIdx, int[] hashes) {
        byte[] buf = new byte[ 2 + K * INT_BYTES];
        int offset = 0;

        // first encode the index of the LSH table
        buf[offset] = (byte) (tableIdx >> 8);
        buf[++offset] = (byte) tableIdx;
        // encode values from hash functions
        for (int k = 0; k < K; k++) {
            buf[++offset] = (byte) (hashes[k] >> 24);
            buf[++offset] = (byte) (hashes[k] >> 16);
            buf[++offset] = (byte) (hashes[k] >>  8);
            buf[++offset] = (byte) hashes[k];
        }
        return new BytesRef(buf, 0, buf.length);
    }


    // get number of dims
    public int dims() {
        return D;
    }
    public short l() {
        return L;
    }
    public int k() {
        return K;
    }
    public float w() {
        return W;
    }
    public float[][][] a() {
        return a;
    }
    public float[][] b() {
        return b;
    }
    public int[][] pertSets() {
        return pertSets;
    }

    //********* Private Helper functions and Classes
    private class Pair {
        private final float score;
        private final List<Integer> pertSet;
        Pair(float score, List<Integer> pertSet) {
            this.score = score;
            this.pertSet = pertSet;
        }
        float getScore() {
            return score;
        }
        List<Integer> getPertSet() {
            return pertSet;
        }
    }

    private float getScore(float[] scores, List<Integer> pertSet) {
        float score = 0;
        for (Integer index : pertSet) {
            score += scores[index];
        }
        return score;
    }

    private List<Integer> shift(List<Integer> s1) {
        int last = s1.get(s1.size() - 1) + 1;
        if (last > (2 * K - 1)) return null; // invalid perturbation

        List<Integer> s2 = new ArrayList<>(s1.size());
        for (int i = 0; i < s1.size() - 1; i++) {
            s2.add(s1.get(i));
        }
        s2.add(last);
        return s2;
    }

    private List<Integer> expand(List<Integer> s1) {
        int last = s1.get(s1.size() - 1) + 1;
        if (last > (2 * K - 1)) return null; // invalid perturbation

        List<Integer> s2 = new ArrayList<>(s1.size() + 1);
        for (int i = 0; i < s1.size(); i++) {
            s2.add(s1.get(i));
        }
        s2.add(last);
        return s2;
    }

    private boolean isValidPertSet(List<Integer> pertSet) {
        // check that pertSet doesn't have indexes of <j> and <2K -1 - j> at the same time.
        // as these indexes represent opposite perturbation of the same coordinate
        for (Integer j : pertSet) {
            Integer oppositePert = 2 * K - 1 - j;
            if (pertSet.contains(oppositePert)) return false;
        }
        return true;
    }

    public static void sortScoresIndexes(int[] indexes, float[] scores) {
        new InPlaceMergeSorter() {
            @Override
            public int compare(int i, int j) {
                return Float.compare(scores[i], scores[j]);
            }

            @Override
            public void swap(int i, int j) {
                int tempIndex = indexes[i];
                indexes[i] = indexes[j];
                indexes[j] = tempIndex;

                float tempScore = scores[j];
                scores[j] = scores[i];
                scores[i] = tempScore;
            }
        }.sort(0, indexes.length);
    }
}

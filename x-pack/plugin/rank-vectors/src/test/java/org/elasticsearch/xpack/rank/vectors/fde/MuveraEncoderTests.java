/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.fde;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Random;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MuveraEncoderTests extends ESTestCase {

    /** Published SplitMix64 outputs for seed 0. */
    public void testSplitMix64ReferenceVector() {
        MuveraEncoder.SplitMix64 rng = new MuveraEncoder.SplitMix64(0);
        assertEquals(0xE220A8397B1DCDAFL, rng.next());
        assertEquals(0x6E789E6AA1B965F4L, rng.next());
        assertEquals(0x06C45D188009454FL, rng.next());
    }

    public void testDimensions() {
        int tokenDims = randomIntBetween(8, 64);
        int kSim = randomIntBetween(0, 3);
        int dProj = randomIntBetween(1, Math.min(tokenDims, 16));
        int rReps = randomIntBetween(1, 4);
        int raw = rReps * (1 << kSim) * dProj;
        MuveraEncoder encoder = new MuveraEncoder(tokenDims, kSim, dProj, rReps, randomLong());
        assertEquals(raw, encoder.dimensions());
        float[][] tokens = randomTokens(randomIntBetween(1, 20), tokenDims);
        assertEquals(raw, encoder.encodeDocument(tokens).length);
        assertEquals(raw, encoder.encodeQuery(tokens).length);
    }

    public void testDeterministicPerSeed() {
        long seed = randomLong();
        float[][] tokens = randomTokens(randomIntBetween(1, 30), 32);
        MuveraEncoder a = new MuveraEncoder(32, 3, 8, 4, seed);
        MuveraEncoder b = new MuveraEncoder(32, 3, 8, 4, seed);
        assertArrayEquals(a.encodeDocument(tokens), b.encodeDocument(tokens), 0f);
        assertArrayEquals(a.encodeQuery(tokens), b.encodeQuery(tokens), 0f);

        MuveraEncoder other = new MuveraEncoder(32, 3, 8, 4, seed + 1);
        assertFalse(Arrays.equals(a.encodeDocument(tokens), other.encodeDocument(tokens)));
    }

    /**
     * One document token fills every cell, one query token occupies one cell per repetition, so with an
     * identity projection the FDE inner product is exactly rReps times the token inner product.
     */
    public void testSingleTokenInnerProductIsExact() {
        int dims = randomIntBetween(4, 64);
        int rReps = randomIntBetween(1, 8);
        MuveraEncoder encoder = new MuveraEncoder(dims, randomIntBetween(0, 4), dims, rReps, randomLong());
        float[][] doc = randomTokens(1, dims);
        float[][] query = randomTokens(1, dims);
        float expected = rReps * VectorUtil.dotProduct(doc[0], query[0]);
        assertEquals(expected, VectorUtil.dotProduct(encoder.encodeDocument(doc), encoder.encodeQuery(query)), 1e-4f);
    }

    /** With a single cell the document block is the token mean and the query block the token sum. */
    public void testSingleCellIsMeanAndSum() {
        int dims = randomIntBetween(4, 32);
        MuveraEncoder encoder = new MuveraEncoder(dims, 0, dims, 1, randomLong());
        float[][] tokens = randomTokens(randomIntBetween(2, 10), dims);
        float[] sum = new float[dims];
        for (float[] t : tokens) {
            for (int d = 0; d < dims; d++) {
                sum[d] += t[d];
            }
        }
        float[] mean = new float[dims];
        for (int d = 0; d < dims; d++) {
            mean[d] = sum[d] / tokens.length;
        }
        assertArrayEquals(sum, encoder.encodeQuery(tokens), 1e-5f);
        assertArrayEquals(mean, encoder.encodeDocument(tokens), 1e-5f);
    }

    /** Documents fill empty cells from a neighbouring cell; queries leave them zero. */
    public void testEmptyCellHandling() {
        int dims = 16;
        int kSim = 4;
        MuveraEncoder encoder = new MuveraEncoder(dims, kSim, dims, 3, randomLong());
        float[][] tokens = randomTokens(1, dims);
        float[] doc = encoder.encodeDocument(tokens);
        float[] query = encoder.encodeQuery(tokens);
        int cells = 1 << kSim;
        int zeroQueryBlocks = 0;
        for (int b = 0; b < 3 * cells; b++) {
            float[] docBlock = Arrays.copyOfRange(doc, b * dims, (b + 1) * dims);
            float[] queryBlock = Arrays.copyOfRange(query, b * dims, (b + 1) * dims);
            assertArrayEquals("doc block " + b + " must hold the single token", tokens[0], docBlock, 0f);
            if (allZero(queryBlock)) {
                zeroQueryBlocks++;
            }
        }
        assertEquals("one occupied cell per repetition", 3 * (cells - 1), zeroQueryBlocks);
    }

    public void testEmptyCellsUseNearestOccupiedCellWithLowestIdTieBreak() {
        int dims = 32;
        int kSim = 3;
        int cells = 1 << kSim;
        MuveraEncoder encoder = new MuveraEncoder(dims, kSim, dims, 1, 0xC0FFEE);
        float[][] tokenByCell = new float[cells][];
        Random random = new Random(0xBAD5EED);
        for (int attempt = 0; attempt < 1_000; attempt++) {
            float[] candidate = randomUnitTokens(random, 1, dims)[0];
            int cell = occupiedCell(encoder.encodeQuery(new float[][] { candidate }), dims);
            tokenByCell[cell] = candidate;
        }
        int firstCell = -1;
        int secondCell = -1;
        for (int first = 0; first < cells && secondCell == -1; first++) {
            if (tokenByCell[first] == null) {
                continue;
            }
            for (int second = first + 1; second < cells; second++) {
                if (tokenByCell[second] != null && Integer.bitCount(first ^ second) % 2 == 0) {
                    firstCell = first;
                    secondCell = second;
                    break;
                }
            }
        }
        assertNotEquals("expected two occupied cells with even Hamming distance", -1, secondCell);
        float[] firstToken = tokenByCell[firstCell];
        float[] secondToken = tokenByCell[secondCell];

        float[] doc = encoder.encodeDocument(new float[][] { firstToken, secondToken });
        float[] query = encoder.encodeQuery(new float[][] { firstToken, secondToken });
        int ties = 0;
        for (int cell = 0; cell < cells; cell++) {
            int distanceToFirst = Integer.bitCount(cell ^ firstCell);
            int distanceToSecond = Integer.bitCount(cell ^ secondCell);
            int expectedCell;
            if (distanceToFirst < distanceToSecond) {
                expectedCell = firstCell;
            } else if (distanceToSecond < distanceToFirst) {
                expectedCell = secondCell;
            } else {
                expectedCell = Math.min(firstCell, secondCell);
                ties++;
            }
            assertArrayEquals(
                "document cell " + cell,
                Arrays.copyOfRange(query, expectedCell * dims, (expectedCell + 1) * dims),
                Arrays.copyOfRange(doc, cell * dims, (cell + 1) * dims),
                0f
            );
        }
        assertThat("the deterministic tie branch must be exercised", ties, greaterThan(0));
    }

    /**
     * The point of the encoding: when a query's tokens are near-copies of one document's tokens, that
     * document should rank at or near the top by FDE inner product, as it does by exact MaxSim.
     */
    public void testFdeRanksPlantedDocumentNearTop() {
        int dims = 128;
        int numDocs = 50;
        Random random = new Random(0x5EED5EEDL);
        MuveraEncoder encoder = new MuveraEncoder(dims, 4, 16, 16, 0x123456789ABCDEFL);
        float[][][] docs = new float[numDocs][][];
        float[][] docFdes = new float[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = randomUnitTokens(random, 64, dims);
            docFdes[i] = encoder.encodeDocument(docs[i]);
        }
        int trials = 20;
        int inTop3 = 0;
        for (int t = 0; t < trials; t++) {
            int target = random.nextInt(numDocs);
            float[][] query = new float[16][];
            for (int q = 0; q < query.length; q++) {
                query[q] = noisyCopy(random, docs[target][random.nextInt(docs[target].length)], 0.1f);
            }
            float[] queryFde = encoder.encodeQuery(query);
            float targetScore = VectorUtil.dotProduct(queryFde, docFdes[target]);
            int better = 0;
            for (int i = 0; i < numDocs; i++) {
                float fdeScore = VectorUtil.dotProduct(queryFde, docFdes[i]);
                if (i != target && fdeScore > targetScore) {
                    better++;
                }
            }
            if (better < 3) {
                inTop3++;
            }
        }
        assertThat(inTop3, greaterThanOrEqualTo(16));
    }

    public void testFdeTopKOverlapWithExactMaxSim() {
        int dims = 64;
        int numDocs = 40;
        int topK = 5;
        Random random = new Random(0xA11B1C0DEL);
        float[][] concepts = randomUnitTokens(random, 80, dims);
        float[][][] docs = new float[numDocs][24][];
        MuveraEncoder encoder = new MuveraEncoder(dims, 3, 8, 32, 0x1234ABCDL);
        float[][] docFdes = new float[numDocs][];
        for (int doc = 0; doc < numDocs; doc++) {
            for (int token = 0; token < docs[doc].length; token++) {
                docs[doc][token] = noisyCopy(random, concepts[random.nextInt(concepts.length)], 0.2f);
            }
            docFdes[doc] = encoder.encodeDocument(docs[doc]);
        }

        int overlap = 0;
        int queries = 20;
        for (int q = 0; q < queries; q++) {
            float[][] query = new float[8][];
            for (int token = 0; token < query.length; token++) {
                query[token] = noisyCopy(random, concepts[random.nextInt(concepts.length)], 0.2f);
            }
            float[] queryFde = encoder.encodeQuery(query);
            float[] exactScores = new float[numDocs];
            float[] fdeScores = new float[numDocs];
            for (int doc = 0; doc < numDocs; doc++) {
                exactScores[doc] = maxSim(query, docs[doc]);
                fdeScores[doc] = VectorUtil.dotProduct(queryFde, docFdes[doc]);
            }
            int[] exactTop = topK(exactScores, topK);
            int[] fdeTop = topK(fdeScores, topK);
            for (int exactDoc : exactTop) {
                for (int fdeDoc : fdeTop) {
                    if (exactDoc == fdeDoc) {
                        overlap++;
                        break;
                    }
                }
            }
        }
        assertThat("FDE should retain at least half of ambiguous exact top-k results", overlap, greaterThanOrEqualTo(queries * topK / 2));
    }

    public void testValidation() {
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new MuveraEncoder(0, 2, 1, 1, 0)).getMessage(),
            containsString("token dims")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new MuveraEncoder(8, 11, 4, 1, 0)).getMessage(),
            containsString("k_sim")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new MuveraEncoder(8, 2, 9, 1, 0)).getMessage(),
            containsString("d_proj")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new MuveraEncoder(8, 2, 4, 0, 0)).getMessage(),
            containsString("r_reps")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new MuveraEncoder(128, 10, 128, 2, 0)).getMessage(),
            containsString("exceeds the maximum")
        );
        MuveraEncoder encoder = new MuveraEncoder(8, 2, 4, 2, 0);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> encoder.encodeDocument(null)).getMessage(),
            containsString("must not be null")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> encoder.encodeDocument(new float[0][])).getMessage(),
            containsString("at least one token")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> encoder.encodeDocument(new float[][] { null })).getMessage(),
            containsString("token vector [0] must not be null")
        );
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> encoder.encodeQuery(new float[][] { new float[7] })).getMessage(),
            containsString("expected token dims [8]")
        );
        for (float nonFinite : new float[] { Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY }) {
            float[] token = unitVector(8, 0);
            token[3] = nonFinite;
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> encoder.encodeQuery(new float[][] { token })).getMessage(),
                containsString("non-finite value at dimension [3]")
            );
        }
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> encoder.encodeDocument(new float[][] { new float[8] })).getMessage(),
            containsString("unit magnitude")
        );

        MuveraEncoder expensive = new MuveraEncoder(1024, 4, 1, 256, 0);
        float[][] tooMuchWork = new float[MuveraEncoder.MAX_TOKEN_VECTORS][];
        Arrays.fill(tooMuchWork, unitVector(1024, 0));
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> expensive.encodeDocument(tooMuchWork)).getMessage(),
            containsString("operations")
        );
    }

    private static float[][] randomTokens(int n, int dims) {
        float[][] tokens = new float[n][dims];
        for (float[] t : tokens) {
            for (int d = 0; d < dims; d++) {
                t[d] = randomFloatBetween(-1f, 1f, true);
            }
            VectorUtil.l2normalize(t);
        }
        return tokens;
    }

    private static float[][] randomUnitTokens(Random random, int n, int dims) {
        float[][] tokens = new float[n][dims];
        for (float[] t : tokens) {
            for (int d = 0; d < dims; d++) {
                t[d] = (float) random.nextGaussian();
            }
            VectorUtil.l2normalize(t);
        }
        return tokens;
    }

    private static float[] noisyCopy(Random random, float[] v, float noise) {
        float[] out = new float[v.length];
        for (int d = 0; d < v.length; d++) {
            out[d] = v[d] + noise * (float) random.nextGaussian() / (float) Math.sqrt(v.length);
        }
        return VectorUtil.l2normalize(out);
    }

    private static float maxSim(float[][] query, float[][] document) {
        float score = 0f;
        for (float[] queryToken : query) {
            float max = Float.NEGATIVE_INFINITY;
            for (float[] documentToken : document) {
                max = Math.max(max, VectorUtil.dotProduct(queryToken, documentToken));
            }
            score += max;
        }
        return score;
    }

    private static int[] topK(float[] scores, int k) {
        Integer[] docs = new Integer[scores.length];
        Arrays.setAll(docs, i -> i);
        Arrays.sort(docs, (left, right) -> Float.compare(scores[right], scores[left]));
        return Arrays.stream(docs).limit(k).mapToInt(Integer::intValue).toArray();
    }

    private static float[] unitVector(int dims, int dimension) {
        float[] vector = new float[dims];
        vector[dimension] = 1f;
        return vector;
    }

    private static int occupiedCell(float[] encodedQuery, int dims) {
        int occupied = -1;
        for (int cell = 0; cell < encodedQuery.length / dims; cell++) {
            float[] block = Arrays.copyOfRange(encodedQuery, cell * dims, (cell + 1) * dims);
            if (allZero(block) == false) {
                assertEquals("one query cell must be occupied", -1, occupied);
                occupied = cell;
            }
        }
        assertNotEquals("one query cell must be occupied", -1, occupied);
        return occupied;
    }

    private static boolean allZero(float[] v) {
        for (float f : v) {
            if (f != 0f) {
                return false;
            }
        }
        return true;
    }

}

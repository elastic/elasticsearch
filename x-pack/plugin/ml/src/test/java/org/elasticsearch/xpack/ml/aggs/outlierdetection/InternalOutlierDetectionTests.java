/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class InternalOutlierDetectionTests extends ESTestCase {

    public void testComputeKthNnDistance() {
        float[][] sample = {
            { 0, 0 },
            { 1, 0 },
            { 0, 1 },
            { 1, 1 },
            { 10, 10 }
        };

        double outlierDist = InternalOutlierDetection.computeKthNnDistance(sample[4], sample, 1);
        assertEquals(162.0, outlierDist, 0.01);

        double normalDist = InternalOutlierDetection.computeKthNnDistance(sample[0], sample, 1);
        assertEquals(1.0, normalDist, 0.01);

        assertTrue("Outlier distance should be much larger", outlierDist > normalDist * 10);
    }

    public void testComputeKthNnDistanceWithLargerK() {
        float[][] sample = {
            { 0, 0 },
            { 1, 0 },
            { 0, 1 },
            { 1, 1 },
            { 100, 100 }
        };

        double dist = InternalOutlierDetection.computeKthNnDistance(sample[4], sample, 3);
        assertEquals(19801.0, dist, 0.01);
    }

    public void testComputeKthNnDistanceSkipsSelf() {
        float[][] sample = {
            { 0, 0 },
            { 5, 5 },
        };

        double dist = InternalOutlierDetection.computeKthNnDistance(sample[0], sample, 1);
        assertEquals(50.0, dist, 0.01);
    }

    public void testComputeKthNnDistanceEmptySample() {
        float[] point = { 1, 2 };
        float[][] emptySample = {};
        double dist = InternalOutlierDetection.computeKthNnDistance(point, emptySample, 3);
        assertEquals(0.0, dist, 0.01);
    }

    public void testKnnDistancesInAggregator() {
        float[][] vectors = {
            { 0, 0 },
            { 1, 0 },
            { 0, 1 },
            { 1, 1 },
            { 50, 50 }
        };

        double[] scores = OutlierDetectionAggregator.computeKnnDistances(vectors, 2);

        int maxIdx = 0;
        for (int i = 1; i < scores.length; i++) {
            if (scores[i] > scores[maxIdx]) {
                maxIdx = i;
            }
        }
        assertEquals("Outlier should have highest kNN distance", 4, maxIdx);
    }

    public void testSquaredEuclidean() {
        float[] a = { 1, 2, 3 };
        float[] b = { 4, 5, 6 };
        assertEquals(27.0, OutlierDetectionAggregator.squaredEuclidean(a, b), 0.001);
    }

    public void testSquaredEuclideanIdentical() {
        float[] a = { 1, 2, 3 };
        assertEquals(0.0, OutlierDetectionAggregator.squaredEuclidean(a, a), 0.001);
    }

    public void testReduceMergesAndRescores() {
        float[][] shard1Samples = {
            { 0, 0 },
            { 1, 0 },
            { 0, 1 }
        };

        float[][] shard2Samples = {
            { 1, 1 },
            { 50, 50 }
        };

        OutlierCandidate c1 = new OutlierCandidate("doc1", new float[] { 0, 0 }, 1.0, 0);
        OutlierCandidate c2 = new OutlierCandidate("doc_outlier", new float[] { 50, 50 }, 100.0, 1);

        InternalOutlierDetection shard1Result = new InternalOutlierDetection(
            "test", null, List.of(c1), shard1Samples, 3, 1, 1, 42L, OutlierDetectionMethod.KTH_NN, ScoreNormalization.NONE
        );

        InternalOutlierDetection shard2Result = new InternalOutlierDetection(
            "test", null, List.of(c2), shard2Samples, 2, 1, 1, 42L, OutlierDetectionMethod.KTH_NN, ScoreNormalization.NONE
        );

        var reducer = shard1Result.getReducer(
            new org.elasticsearch.search.aggregations.AggregationReduceContext.ForFinal(
                null, null, () -> false, null, b -> {}
            ),
            2
        );
        reducer.accept(shard1Result);
        reducer.accept(shard2Result);
        InternalOutlierDetection result = (InternalOutlierDetection) reducer.get();

        assertEquals(1, result.getCandidates().size());
        assertEquals("doc_outlier", result.getCandidates().get(0).getDocId());
    }

    public void testOutlierCandidateOrdering() {
        OutlierCandidate low = new OutlierCandidate("low", new float[] {}, 1.0, 0);
        OutlierCandidate high = new OutlierCandidate("high", new float[] {}, 10.0, 0);

        List<OutlierCandidate> list = new ArrayList<>(List.of(low, high));
        list.sort(null);

        assertEquals("high", list.get(0).getDocId());
        assertEquals("low", list.get(1).getDocId());
    }
}

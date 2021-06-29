/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import java.util.List;

public class RunningStatsTests extends BaseMatrixStatsTestCase {

    /** test running stats */
    public void testRunningStats() throws Exception {
        final MatrixStatsResults results = new MatrixStatsResults(createRunningStats(fieldA, fieldB));
        actualStats.assertNearlyEqual(results);
    }

    /** Test merging stats across observation shards */
    public void testMergedStats() throws Exception {
        // slice observations into shards
        int numShards = randomIntBetween(2, 10);
        double obsPerShard = Math.floor(numObs / numShards);
        int start = 0;
        RunningStats stats = null;
        List<Double> fieldAShard, fieldBShard;
        for (int s = 0; s < numShards-1; start = ++s * (int)obsPerShard) {
            fieldAShard = fieldA.subList(start, start + (int)obsPerShard);
            fieldBShard = fieldB.subList(start, start + (int)obsPerShard);
            if (stats == null) {
                stats = createRunningStats(fieldAShard, fieldBShard);
            } else {
                stats.merge(createRunningStats(fieldAShard, fieldBShard));
            }
        }
        stats.merge(createRunningStats(fieldA.subList(start, fieldA.size()), fieldB.subList(start, fieldB.size())));

        final MatrixStatsResults results = new MatrixStatsResults(stats);
        actualStats.assertNearlyEqual(results);
    }

    private RunningStats createRunningStats(List<Double> fieldAObs, List<Double> fieldBObs) {
        RunningStats stats = new RunningStats();
        // create a document with two numeric fields
        final String[] fieldNames = new String[2];
        fieldNames[0] = fieldAKey;
        fieldNames[1] = fieldBKey;
        final double[] fieldVals = new double[2];

        // running stats computation
        for (int n = 0; n < fieldAObs.size(); ++n) {
            fieldVals[0] = fieldAObs.get(n);
            fieldVals[1] = fieldBObs.get(n);
            stats.add(fieldNames, fieldVals);
        }
        return stats;
    }

}

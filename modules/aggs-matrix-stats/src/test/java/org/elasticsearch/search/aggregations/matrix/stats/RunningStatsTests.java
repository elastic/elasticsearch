/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import java.util.Arrays;
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
        for (int s = 0; s < numShards - 1; start = ++s * (int) obsPerShard) {
            fieldAShard = fieldA.subList(start, start + (int) obsPerShard);
            fieldBShard = fieldB.subList(start, start + (int) obsPerShard);
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

    public void testMergeRunningStats() throws Exception {
        final List<RunningStats> runningStats = Arrays.asList(
            new RunningStats(new String[] { "b", "a", "c" }, new double[] { 10.0d, 30.0d, 25.0d }), // if moving this item change last two
                                                                                                    // assertions
            new RunningStats(new String[] { "a", "b", "c", "d" }, new double[] { 1.0d, 2.0d, 3.0d, 4.0d }),
            new RunningStats(new String[] { "a", "a", "x", "x" }, new double[] { 17.0d, 28.0d, 32.0d, 44.0d }),
            new RunningStats(new String[] { "a", "c" }, new double[] { 100.0d, 200.0d }),
            new RunningStats(new String[] { "x", "y", "z" }, new double[] { 11.0d, 35.0d, 20.0d }),
            new RunningStats(new String[] { "A", "B", "C" }, new double[] { 11.0d, 35.0d, 20.0d }),
            new RunningStats(new String[] { "a" }, new double[] { 15.0d })
        );
        final RunningStats otherRunningStat = new RunningStats(new String[] { "a", "b", "c" }, new double[] { -12.3, 0.0, 203.56d });
        final RunningStats emptyStats = new RunningStats();

        assertTrue(otherRunningStat.canMerge(null));
        assertTrue(emptyStats.canMerge(otherRunningStat));
        assertTrue(otherRunningStat.canMerge(emptyStats));

        for (int i = 0; i < runningStats.size(); i++) {
            final RunningStats a = runningStats.get(i);
            for (int j = 0; j < runningStats.size(); j++) {
                final RunningStats b = runningStats.get(j);
                assertEquals("Error while merging running stats " + i + " and " + j, i == j, a.canMerge(b));
                assertEquals("Error while merging running stats " + i + " and " + j, i == j, b.canMerge(a));
            }
            assertEquals(i == 0, a.canMerge(otherRunningStat));
            assertEquals(i == 0, otherRunningStat.canMerge(a));
        }
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

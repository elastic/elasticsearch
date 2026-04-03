/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class PartitionDataGeneratorTests extends ESTestCase {

    public void testUniformDistribution() {
        int numDocs = 1000;
        int numPartitions = 10;
        var generator = new PartitionDataGenerator(numDocs, 8, numPartitions, DatasetConfig.PartitionDistribution.UNIFORM, 42L);

        Map<String, List<Integer>> assignments = generator.getPartitionAssignments();
        assertEquals(numPartitions, assignments.size());

        int totalAssigned = assignments.values().stream().mapToInt(List::size).sum();
        assertEquals(numDocs, totalAssigned);
    }

    public void testZipfDistribution() {
        int numDocs = 1000;
        int numPartitions = 10;
        var generator = new PartitionDataGenerator(numDocs, 8, numPartitions, DatasetConfig.PartitionDistribution.ZIPF, 42L);

        Map<String, List<Integer>> assignments = generator.getPartitionAssignments();
        assertEquals(numPartitions, assignments.size());

        int totalAssigned = assignments.values().stream().mapToInt(List::size).sum();
        assertEquals(numDocs, totalAssigned);

        // Zipf: first partition should have more docs than last
        int firstPartitionSize = assignments.values().iterator().next().size();
        int lastPartitionSize = assignments.values().stream().reduce((a, b) -> b).orElseThrow().size();
        assertTrue("Zipf first partition should be larger than last", firstPartitionSize > lastPartitionSize);
    }

    public void testUniformDistributionUnevenSplit() {
        int numDocs = 103;
        int numPartitions = 10;
        var generator = new PartitionDataGenerator(numDocs, 8, numPartitions, DatasetConfig.PartitionDistribution.UNIFORM, 42L);

        Map<String, List<Integer>> assignments = generator.getPartitionAssignments();
        assertEquals(numPartitions, assignments.size());

        int totalAssigned = assignments.values().stream().mapToInt(List::size).sum();
        assertEquals(numDocs, totalAssigned);
    }
}

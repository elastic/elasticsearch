/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn.data;

import org.apache.lucene.search.SortField;
import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.knn.IndexVectorReader;
import org.elasticsearch.test.knn.KnnIndexer;

import java.util.List;
import java.util.Map;

public class PartitionDataGeneratorTests extends ESTestCase {

    public void testIndexSortUsesSliceKeyOnlyForSlicedLayout() {
        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(10, 2, DatasetConfig.PartitionDistribution.UNIFORM);
        IOSupplier<IndexVectorReader> vectors = () -> new IndexVectorReader.RandomVectorReader(42L, 8, false);

        var sliced = new PartitionDataGenerator(vectors, 10, vectors, 0, partitionConfiguration, true);
        assertEquals(SliceIndexing.SLICE_KEY_FIELD_NAME, sliced.getIndexSort().getSort()[0].getField());
        assertEquals(SortField.STRING_LAST, sliced.getIndexSort().getSort()[0].getMissingValue());

        var unsliced = new PartitionDataGenerator(vectors, 10, vectors, 0, partitionConfiguration, false);
        assertEquals(KnnIndexer.PARTITION_ID_FIELD, unsliced.getIndexSort().getSort()[0].getField());
    }

    public void testUniformDistribution() {
        int numDocs = 1000;
        int numPartitions = 10;
        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(
            numDocs,
            numPartitions,
            DatasetConfig.PartitionDistribution.UNIFORM
        );
        IOSupplier<IndexVectorReader> vectors = () -> new IndexVectorReader.RandomVectorReader(42L, 8, false);
        var generator = new PartitionDataGenerator(vectors, numDocs, vectors, 0, partitionConfiguration, false);

        Map<String, List<Integer>> assignments = generator.getPartitionAssignments();
        assertEquals(numPartitions, assignments.size());

        int totalAssigned = assignments.values().stream().mapToInt(List::size).sum();
        assertEquals(numDocs, totalAssigned);
    }

    public void testZipfDistribution() {
        int numDocs = 1000;
        int numPartitions = 10;
        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(
            numDocs,
            numPartitions,
            DatasetConfig.PartitionDistribution.ZIPF
        );
        IOSupplier<IndexVectorReader> vectors = () -> new IndexVectorReader.RandomVectorReader(42L, 8, false);
        var generator = new PartitionDataGenerator(vectors, numDocs, vectors, 0, partitionConfiguration, false);

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
        PartitionConfiguration partitionConfiguration = new PartitionConfiguration(
            numDocs,
            numPartitions,
            DatasetConfig.PartitionDistribution.UNIFORM
        );
        IOSupplier<IndexVectorReader> vectors = () -> new IndexVectorReader.RandomVectorReader(42L, 8, false);
        var generator = new PartitionDataGenerator(vectors, numDocs, vectors, 0, partitionConfiguration, false);

        Map<String, List<Integer>> assignments = generator.getPartitionAssignments();
        assertEquals(numPartitions, assignments.size());

        int totalAssigned = assignments.values().stream().mapToInt(List::size).sum();
        assertEquals(numDocs, totalAssigned);
    }
}

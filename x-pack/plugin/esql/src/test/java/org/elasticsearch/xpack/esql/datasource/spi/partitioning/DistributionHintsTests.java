/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

import org.elasticsearch.test.ESTestCase;

import java.util.OptionalLong;
import java.util.Set;

/**
 * Tests for {@link DistributionHints}.
 */
public class DistributionHintsTests extends ESTestCase {

    public void testMinimalConstructor() {
        var hints = new DistributionHints(4);
        assertEquals(4, hints.targetPartitions());
        assertEquals(Set.of(), hints.availableNodes());
        assertEquals(OptionalLong.empty(), hints.maxPartitionBytes());
        assertFalse(hints.preferDataLocality());
    }

    public void testTwoArgConstructor() {
        var nodes = Set.of("node-1", "node-2");
        var hints = new DistributionHints(4, nodes);
        assertEquals(4, hints.targetPartitions());
        assertEquals(nodes, hints.availableNodes());
        assertFalse(hints.preferDataLocality());
    }

    public void testBuilder() {
        var hints = DistributionHints.builder()
            .targetPartitions(8)
            .availableNodes(Set.of("n1", "n2", "n3"))
            .maxPartitionBytes(128 * 1024 * 1024)
            .preferDataLocality(true)
            .build();
        assertEquals(8, hints.targetPartitions());
        assertEquals(3, hints.availableNodes().size());
        assertEquals(OptionalLong.of(128 * 1024 * 1024), hints.maxPartitionBytes());
        assertTrue(hints.preferDataLocality());
    }

    public void testNodeCount() {
        assertEquals(1, new DistributionHints(4).nodeCount());
        assertEquals(3, new DistributionHints(4, Set.of("a", "b", "c")).nodeCount());
    }

    public void testMaxPartitionBytesOr() {
        var withMax = DistributionHints.builder().targetPartitions(1).maxPartitionBytes(100).build();
        assertEquals(100, withMax.maxPartitionBytesOr(999));

        var withoutMax = new DistributionHints(1);
        assertEquals(999, withoutMax.maxPartitionBytesOr(999));
    }

    public void testCoordinatorOnly() {
        var hints = DistributionHints.coordinatorOnly();
        assertEquals(1, hints.targetPartitions());
        assertEquals(Set.of(), hints.availableNodes());
        assertFalse(hints.preferDataLocality());
    }
}

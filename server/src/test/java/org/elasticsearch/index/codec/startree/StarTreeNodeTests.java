/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.elasticsearch.index.mapper.startree.StarTreeAggregationType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class StarTreeNodeTests extends ESTestCase {

    public void testCreateLeafNode() {
        double[][] metrics = {
            { 100.0, 10.0, 5.0, 50.0 }, // SUM=100, COUNT=10, MIN=5, MAX=50
            { 200.0, 20.0, 1.0, 100.0 }  // SUM=200, COUNT=20, MIN=1, MAX=100
        };

        StarTreeNode node = new StarTreeNode(
            42L,  // dimension ordinal
            0,    // depth
            false, // not star node
            true,  // is leaf
            0,     // no children
            metrics,
            10L   // doc count
        );

        assertThat(node.getGroupingFieldOrdinal(), equalTo(42L));
        assertThat(node.getDepth(), equalTo(0));
        assertFalse(node.isStarNode());
        assertTrue(node.isLeaf());
        assertThat(node.getNumChildren(), equalTo(0));
        assertThat(node.getDocCount(), equalTo(10L));

        // Check aggregated values
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.SUM), equalTo(100.0));
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.COUNT), equalTo(10.0));
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.MIN), equalTo(5.0));
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.MAX), equalTo(50.0));

        assertThat(node.getAggregatedValue(1, StarTreeAggregationType.SUM), equalTo(200.0));
        assertThat(node.getAggregatedValue(1, StarTreeAggregationType.COUNT), equalTo(20.0));
    }

    public void testCreateStarNode() {
        double[][] metrics = {
            { 500.0, 50.0, 1.0, 100.0 }
        };

        StarTreeNode node = new StarTreeNode(
            StarTreeConstants.STAR_NODE_ORDINAL,
            1,    // depth
            true,  // is star node
            false, // not leaf
            5,     // 5 children
            metrics,
            50L
        );

        assertThat(node.getGroupingFieldOrdinal(), equalTo(StarTreeConstants.STAR_NODE_ORDINAL));
        assertTrue(node.isStarNode());
        assertFalse(node.isLeaf());
        assertThat(node.getNumChildren(), equalTo(5));
    }

    public void testBuilder() {
        StarTreeNode.Builder builder = new StarTreeNode.Builder()
            .groupingFieldOrdinal(10L)
            .depth(2)
            .isStarNode(false)
            .isLeaf(true)
            .numChildren(0)
            .docCount(100L)
            .aggregatedValues(new double[][] {
                { 1000.0, 100.0, 10.0, 500.0 }
            });

        StarTreeNode node = builder.build();

        assertThat(node.getGroupingFieldOrdinal(), equalTo(10L));
        assertThat(node.getDepth(), equalTo(2));
        assertFalse(node.isStarNode());
        assertTrue(node.isLeaf());
        assertThat(node.getDocCount(), equalTo(100L));
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.SUM), equalTo(1000.0));
    }

    public void testBuilderAggregateFrom() {
        double[][] metrics1 = {
            { 100.0, 10.0, 5.0, 50.0 }
        };
        StarTreeNode node1 = new StarTreeNode(1L, 0, false, true, 0, metrics1, 10L);

        double[][] metrics2 = {
            { 200.0, 20.0, 2.0, 80.0 }
        };
        StarTreeNode node2 = new StarTreeNode(2L, 0, false, true, 0, metrics2, 20L);

        // Create array with correct initial values for aggregation
        // MIN starts at positive infinity, MAX starts at negative infinity, SUM and COUNT start at 0
        double[][] initialMetrics = new double[][] {
            { 0.0, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }
        };

        StarTreeNode.Builder builder = new StarTreeNode.Builder()
            .groupingFieldOrdinal(StarTreeConstants.STAR_NODE_ORDINAL)
            .isStarNode(true)
            .aggregatedValues(initialMetrics);

        builder.aggregateFrom(node1);
        builder.aggregateFrom(node2);

        StarTreeNode result = builder.build();

        // SUM: 100 + 200 = 300
        assertThat(result.getAggregatedValue(0, StarTreeAggregationType.SUM), equalTo(300.0));
        // COUNT: 10 + 20 = 30
        assertThat(result.getAggregatedValue(0, StarTreeAggregationType.COUNT), equalTo(30.0));
        // MIN: min(5, 2) = 2
        assertThat(result.getAggregatedValue(0, StarTreeAggregationType.MIN), equalTo(2.0));
        // MAX: max(50, 80) = 80
        assertThat(result.getAggregatedValue(0, StarTreeAggregationType.MAX), equalTo(80.0));
        // Doc count: 10 + 20 = 30
        assertThat(result.getDocCount(), equalTo(30L));
    }

    public void testWithFilePosition() {
        StarTreeNode original = new StarTreeNode(42L, 0, false, true, 0, null, 10L);

        long[] childOffsets = new long[] { 2000L, 3000L };
        StarTreeNode withPosition = original.withFilePosition(1000L, childOffsets);

        assertThat(withPosition.getFileOffset(), equalTo(1000L));
        assertThat(withPosition.getChildrenOffsets(), equalTo(childOffsets));
        // Other fields should remain the same
        assertThat(withPosition.getGroupingFieldOrdinal(), equalTo(42L));
        assertThat(withPosition.getDocCount(), equalTo(10L));
    }

    public void testGetValueAggregationsArray() {
        double[][] metrics = {
            { 100.0, 10.0, 5.0, 50.0 }
        };
        StarTreeNode node = new StarTreeNode(1L, 0, false, true, 0, metrics, 10L);

        double[] values = node.getValueAggregations(0);

        assertThat(values.length, equalTo(4));
        assertThat(values[StarTreeAggregationType.SUM.ordinal()], equalTo(100.0));
        assertThat(values[StarTreeAggregationType.COUNT.ordinal()], equalTo(10.0));
        assertThat(values[StarTreeAggregationType.MIN.ordinal()], equalTo(5.0));
        assertThat(values[StarTreeAggregationType.MAX.ordinal()], equalTo(50.0));
    }

    public void testGetAggregatedValuesCopiesArray() {
        double[][] metrics = {
            { 100.0, 10.0, 5.0, 50.0 }
        };
        StarTreeNode node = new StarTreeNode(1L, 0, false, true, 0, metrics, 10L);

        double[][] copy = node.getAggregatedValues();

        // Modify the copy
        copy[0][0] = 999.0;

        // Original should be unchanged
        assertThat(node.getAggregatedValue(0, StarTreeAggregationType.SUM), equalTo(100.0));
    }

    public void testToString() {
        StarTreeNode leafNode = new StarTreeNode(42L, 1, false, true, 0, null, 10L);
        String leafStr = leafNode.toString();
        assertTrue(leafStr.contains("ordinal=42"));
        assertTrue(leafStr.contains("leaf=true"));
        assertTrue(leafStr.contains("docs=10"));

        StarTreeNode starNode = new StarTreeNode(StarTreeConstants.STAR_NODE_ORDINAL, 0, true, false, 5, null, 100L);
        String starStr = starNode.toString();
        assertTrue(starStr.contains("ordinal=*"));
        assertTrue(starStr.contains("children=5"));
    }
}

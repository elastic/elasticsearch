/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi.partitioning;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests for {@link NodeAffinity}.
 */
public class NodeAffinityTests extends ESTestCase {

    public void testNoneHasNoAffinity() {
        assertFalse(NodeAffinity.NONE.hasAffinity());
        assertNull(NodeAffinity.NONE.nodeId());
        assertFalse(NodeAffinity.NONE.required());
    }

    public void testPreferHasSoftAffinity() {
        NodeAffinity soft = NodeAffinity.prefer("node-1");
        assertTrue(soft.hasAffinity());
        assertEquals("node-1", soft.nodeId());
        assertFalse(soft.required());
    }

    public void testRequireHasHardAffinity() {
        NodeAffinity hard = NodeAffinity.require("node-1");
        assertTrue(hard.hasAffinity());
        assertEquals("node-1", hard.nodeId());
        assertTrue(hard.required());
    }

    public void testRecordEquality() {
        assertEquals(NodeAffinity.prefer("node-1"), NodeAffinity.prefer("node-1"));
        assertEquals(NodeAffinity.require("node-1"), NodeAffinity.require("node-1"));
        assertNotEquals(NodeAffinity.prefer("node-1"), NodeAffinity.require("node-1"));
        assertNotEquals(NodeAffinity.prefer("node-1"), NodeAffinity.prefer("node-2"));
    }

    public void testNoneEqualsEquivalentRecord() {
        // Records have structural equality — NONE equals any (null, false) instance
        assertEquals(NodeAffinity.NONE, new NodeAffinity(null, false));
        assertEquals(NodeAffinity.NONE.hashCode(), new NodeAffinity(null, false).hashCode());
    }
}

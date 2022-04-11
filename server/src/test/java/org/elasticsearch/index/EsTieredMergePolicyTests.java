/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.test.ESTestCase;

public class EsTieredMergePolicyTests extends ESTestCase {

    public void testDefaults() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        assertEquals(new TieredMergePolicy().getMaxMergedSegmentMB(), policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetMaxMergedSegmentMB() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setMaxMergedSegmentMB(10 * 1024);
        assertEquals(10 * 1024, policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
        assertEquals(Long.MAX_VALUE / 1024.0 / 1024.0, policy.forcedMergePolicy.getMaxMergedSegmentMB(), 0d);
    }

    public void testSetForceMergeDeletesPctAllowed() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setForceMergeDeletesPctAllowed(42);
        assertEquals(42, policy.forcedMergePolicy.getForceMergeDeletesPctAllowed(), 0);
        assertEquals(42, policy.regularMergePolicy.getForceMergeDeletesPctAllowed(), 0);
    }

    public void testSetFloorSegmentMB() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setFloorSegmentMB(42);
        assertEquals(42, policy.regularMergePolicy.getFloorSegmentMB(), 0);
        assertEquals(42, policy.forcedMergePolicy.getFloorSegmentMB(), 0);
    }

    public void testSetMaxMergeAtOnce() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setMaxMergeAtOnce(42);
        assertEquals(42, policy.regularMergePolicy.getMaxMergeAtOnce());
    }

    public void testSetSegmentsPerTier() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setSegmentsPerTier(42);
        assertEquals(42, policy.regularMergePolicy.getSegmentsPerTier(), 0);
    }

    public void testSetDeletesPctAllowed() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setDeletesPctAllowed(42);
        assertEquals(42, policy.regularMergePolicy.getDeletesPctAllowed(), 0);
    }
}

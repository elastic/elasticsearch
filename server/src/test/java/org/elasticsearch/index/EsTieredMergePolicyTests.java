/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index;

import org.apache.lucene.index.TieredMergePolicy;
import org.elasticsearch.test.ESTestCase;

public class EsTieredMergePolicyTests extends ESTestCase {

    public void testDefaults() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        assertEquals(
                new TieredMergePolicy().getMaxMergedSegmentMB(),
                policy.regularMergePolicy.getMaxMergedSegmentMB(), 0d);
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

    public void testSetMaxMergeAtOnceExplicit() {
        EsTieredMergePolicy policy = new EsTieredMergePolicy();
        policy.setMaxMergeAtOnceExplicit(42);
        assertEquals(42, policy.forcedMergePolicy.getMaxMergeAtOnceExplicit());
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

/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.search;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

public class ReplicasScaleDownStateTests extends ESTestCase {

    public void testAllOperations() {
        ReplicasScaleDownState state = new ReplicasScaleDownState();
        assertTrue(state.isEmpty());

        ReplicasScaleDownState.PerIndexState result = state.updateMaxReplicasRecommended("index1", 5);
        assertEquals(1, result.signalCount());
        assertEquals(5, result.maxReplicasRecommended());
        assertFalse(state.isEmpty());

        ReplicasScaleDownState.PerIndexState retrieved = state.getState("index1");
        assertNotNull(retrieved);
        assertEquals(1, retrieved.signalCount());
        assertEquals(5, retrieved.maxReplicasRecommended());

        result = state.updateMaxReplicasRecommended("index1", 3);
        assertEquals(2, result.signalCount());
        assertEquals(5, result.maxReplicasRecommended()); // max of 5 and 3

        result = state.updateMaxReplicasRecommended("index1", 8);
        assertEquals(3, result.signalCount());
        assertEquals(8, result.maxReplicasRecommended());

        state.updateMaxReplicasRecommended("index2", 2);
        state.updateMaxReplicasRecommended("index3", 1);
        assertNotNull(state.getState("index2"));
        assertNotNull(state.getState("index3"));

        state.clearStateForIndices(List.of("index1"));
        assertNull(state.getState("index1"));
        assertNotNull(state.getState("index2"));
        assertNotNull(state.getState("index3"));

        state.clearStateExceptForIndices(Set.of("index2"));
        assertNotNull(state.getState("index2"));
        assertNull(state.getState("index3"));

        state.clearState();
        assertTrue(state.isEmpty());
    }
}

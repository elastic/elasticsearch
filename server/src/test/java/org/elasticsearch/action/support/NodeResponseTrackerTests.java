/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.test.ESTestCase;

public class NodeResponseTrackerTests extends ESTestCase {

    public void testAllResponsesReceived() throws Exception {
        int nodes = randomIntBetween(1, 10);
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(nodes);
        for (int i = 0; i < nodes; i++) {
            boolean isLast = i == nodes - 1;
            assertEquals(
                isLast,
                intermediateNodeResponses.trackResponseAndCheckIfLast(i, randomBoolean() ? i : new Exception("from node " + i))
            );
        }

        assertFalse(intermediateNodeResponses.responsesDiscarded());
        assertEquals(nodes, intermediateNodeResponses.getExpectedResponseCount());
        for (int i = 0; i < nodes; i++) {
            assertNotNull(intermediateNodeResponses.getResponse(i));
            if (intermediateNodeResponses.getResponse(i) instanceof Integer) {
                Integer nodeResponse = (Integer) intermediateNodeResponses.getResponse(i);
                assertEquals(i, nodeResponse.intValue());
            }
        }
    }

    public void testDiscardingResults() {
        int nodes = randomIntBetween(1, 10);
        int cancelAt = randomIntBetween(0, Math.max(0, nodes - 2));
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(nodes);
        for (int i = 0; i < nodes; i++) {
            if (i == cancelAt) {
                intermediateNodeResponses.discardIntermediateResponses(new Exception("simulated"));
            }
            boolean isLast = i == nodes - 1;
            assertEquals(
                isLast,
                intermediateNodeResponses.trackResponseAndCheckIfLast(i, randomBoolean() ? i : new Exception("from node " + i))
            );
        }

        assertTrue(intermediateNodeResponses.responsesDiscarded());
        assertEquals(nodes, intermediateNodeResponses.getExpectedResponseCount());
        expectThrows(NodeResponseTracker.DiscardedResponsesException.class, () -> intermediateNodeResponses.getResponse(0));
    }

    public void testResponseIsRegisteredOnlyOnce() {
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(1);
        assertTrue(intermediateNodeResponses.trackResponseAndCheckIfLast(0, "response1"));
        expectThrows(AssertionError.class, () -> intermediateNodeResponses.trackResponseAndCheckIfLast(0, "response2"));
    }
}

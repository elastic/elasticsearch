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
        int size = randomIntBetween(1, 10);
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(size);
        for (int i = 0; i < size; i++) {
            assertTrue(intermediateNodeResponses.maybeAddResponse(i, randomBoolean() ? i : new Exception("from node " + i)));
        }

        assertTrue(intermediateNodeResponses.allNodesResponded());
        assertFalse(intermediateNodeResponses.responsesDiscarded());
        assertEquals(size, intermediateNodeResponses.size());
        for (int i = 0; i < size; i++) {
            assertNotNull(intermediateNodeResponses.getResponse(i));
            if (intermediateNodeResponses.getResponse(i)instanceof Integer nodeResponse) {
                assertEquals(i, nodeResponse.intValue());
            }
        }
    }

    public void testDiscardingResults() {
        int size = randomIntBetween(1, 10);
        int cancelAt = randomIntBetween(0, Math.max(0, size - 2));
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(size);
        for (int i = 0; i < size; i++) {
            if (i == cancelAt) {
                intermediateNodeResponses.discardIntermediateResponses(new Exception("simulated"));
            }
            boolean added = intermediateNodeResponses.maybeAddResponse(i, randomBoolean() ? i : new Exception("from node " + i));
            if (i < cancelAt) {
                assertTrue(added);
            } else {
                assertFalse(added);
            }
        }

        assertTrue(intermediateNodeResponses.responsesDiscarded());
        assertTrue(intermediateNodeResponses.allNodesResponded());
        expectThrows(NodeResponseTracker.DiscardedResponsesException.class, intermediateNodeResponses::size);
        expectThrows(NodeResponseTracker.DiscardedResponsesException.class, () -> intermediateNodeResponses.getResponse(0));
    }

    public void testResponseIsRegisteredOnlyOnce() throws Exception {
        NodeResponseTracker intermediateNodeResponses = new NodeResponseTracker(2);
        assertTrue(intermediateNodeResponses.maybeAddResponse(0, "response1"));
        assertFalse(intermediateNodeResponses.maybeAddResponse(0, "response2"));
        assertEquals("response1", intermediateNodeResponses.getResponse(0));
    }
}

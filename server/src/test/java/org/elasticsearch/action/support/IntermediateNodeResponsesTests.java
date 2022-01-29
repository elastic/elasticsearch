/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.test.ESTestCase;

public class IntermediateNodeResponsesTests extends ESTestCase {

    public void testCompletion() throws Exception {
        int size = randomIntBetween(1, 10);
        IntermediateNodeResponses intermediateNodeResponses = new IntermediateNodeResponses(size);
        for (int i = 0; i < size; i++) {
            assertTrue(intermediateNodeResponses.maybeAddResponse(i, randomBoolean() ? i : new Exception("from node " + i)));
        }

        assertTrue(intermediateNodeResponses.isComplete());
        assertFalse(intermediateNodeResponses.isDiscarded());
        assertEquals(size, intermediateNodeResponses.size());
        for (int i = 0; i < size; i++) {
            assertNotNull(intermediateNodeResponses.getResponse(i));
            if (intermediateNodeResponses.getResponse(i)instanceof Integer nodeResponse) {
                assertEquals(i, nodeResponse.intValue());
            }
        }
    }

    public void testCancellation() {
        int size = randomIntBetween(2, 10);
        int cancelAt = randomIntBetween(0, size - 2);
        IntermediateNodeResponses intermediateNodeResponses = new IntermediateNodeResponses(size);
        for (int i = 0; i < size; i++) {
            if (i == cancelAt) {
                intermediateNodeResponses.discard();
            }
            boolean added = intermediateNodeResponses.maybeAddResponse(i, randomBoolean() ? i : new Exception("from node " + i));
            if (i < cancelAt) {
                assertTrue(added);
            } else {
                assertFalse(added);
            }
        }

        assertTrue(intermediateNodeResponses.isDiscarded());
        assertFalse(intermediateNodeResponses.isComplete());
        expectThrows(IntermediateNodeResponses.AlreadyDiscardedException.class, intermediateNodeResponses::size);
        expectThrows(IntermediateNodeResponses.AlreadyDiscardedException.class, () -> intermediateNodeResponses.getResponse(0));
    }

    public void testResponseIsRegistredOnlyOnce() throws Exception {
        IntermediateNodeResponses intermediateNodeResponses = new IntermediateNodeResponses(1);
        assertTrue(intermediateNodeResponses.maybeAddResponse(0, "response1"));
        assertFalse(intermediateNodeResponses.maybeAddResponse(0, "response2"));
        assertEquals("response1", intermediateNodeResponses.getResponse(0));
    }
}

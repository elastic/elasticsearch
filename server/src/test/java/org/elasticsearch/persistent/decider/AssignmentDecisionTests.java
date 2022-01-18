/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent.decider;

import org.elasticsearch.test.ESTestCase;

public class AssignmentDecisionTests extends ESTestCase {

    public void testConstantsTypes() {
        assertEquals(AssignmentDecision.Type.YES, AssignmentDecision.YES.getType());
    }

    public void testResolveFromType() {
        final AssignmentDecision.Type expected = randomFrom(AssignmentDecision.Type.values());
        assertEquals(expected, AssignmentDecision.Type.resolve(expected.toString()));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.YES;

/**
 * A class for unit testing the {@link Decision} class.
 */
public class DecisionTests extends ESTestCase {

    /**
     * Tests {@link Type#higherThan(Type)}
     */
    public void testHigherThan() {
        // test YES type
        assertTrue(YES.higherThan(NO));
        assertTrue(YES.higherThan(THROTTLE));
        assertFalse(YES.higherThan(YES));

        // test THROTTLE type
        assertTrue(THROTTLE.higherThan(NO));
        assertFalse(THROTTLE.higherThan(THROTTLE));
        assertFalse(THROTTLE.higherThan(YES));

        // test NO type
        assertFalse(NO.higherThan(NO));
        assertFalse(NO.higherThan(THROTTLE));
        assertFalse(NO.higherThan(YES));
    }
}

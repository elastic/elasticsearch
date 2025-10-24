/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EnumSerializationTestUtils;

import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NO;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.NOT_PREFERRED;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.THROTTLE;
import static org.elasticsearch.cluster.routing.allocation.decider.Decision.Type.YES;

/**
 * A class for unit testing the {@link Decision} class.
 */
public class DecisionTests extends ESTestCase {

    public void testTypeEnumOrder() {
        EnumSerializationTestUtils.assertEnumSerialization(Decision.Type.class, NO, THROTTLE, NOT_PREFERRED, YES);
    }

    public void testTypeHigherThan() {
        assertTrue(YES.higherThan(NOT_PREFERRED) && NOT_PREFERRED.higherThan(THROTTLE) && THROTTLE.higherThan(NO));
    }

    public void testTypeAllowed() {
        List.of(NOT_PREFERRED, YES).forEach(d -> assertTrue(d.allowed()));
        List.of(NO, THROTTLE).forEach(d -> assertFalse(d.allowed()));
    }

}

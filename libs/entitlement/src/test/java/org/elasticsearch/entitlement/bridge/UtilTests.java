/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.entitlement.bridge.UtilTests.MockSensitiveClass.mockSensitiveMethod;

public class UtilTests extends ESTestCase {

    public void testCallerClass() {
        assertEquals(UtilTests.class, mockSensitiveMethod());
    }

    /**
     * A separate class so the stack walk can discern the sensitive method's own class
     * from that of its caller.
     */
    static class MockSensitiveClass {
        public static Class<?> mockSensitiveMethod() {
            return Util.getCallerClass();
        }
    }

}

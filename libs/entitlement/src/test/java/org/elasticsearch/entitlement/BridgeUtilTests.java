/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement;

import org.elasticsearch.entitlement.bridge.Util;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.entitlement.BridgeUtilTests.MockSensitiveClass.mockSensitiveMethod;

/**
 * Note: this is not in the bridge package because that is a uniquely bad one to use for tests.
 * Since:
 * <ol>
 *     <li>
 *         we must patch the bridge module into {@code java.base} for it to be reachable
 *         from JDK methods,
 *     </li>
 *     <li>
 *         the bridge module exports {@code org.elasticsearch.entitlement.bridge}, and
 *     </li>
 *     <li>
 *         Java forbids split packages
 *     </li>
 * </ol>
 *
 * ...therefore, we'll be unable to load any tests in the {@code org.elasticsearch.entitlement.bridge}
 * package from the classpath.
 * <p>
 * Hence, we put this test in another package. It's still accessible during testing, though,
 * because we export the bridge to `ALL-UNNAMED` anyway.
 */
public class BridgeUtilTests extends ESTestCase {

    public void testCallerClass() {
        assertEquals(BridgeUtilTests.class, mockSensitiveMethod());
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

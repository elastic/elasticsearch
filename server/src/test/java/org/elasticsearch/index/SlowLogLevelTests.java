/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.test.ESTestCase;

public class SlowLogLevelTests extends ESTestCase {

    public void testTracePrecedence() {
        assertTrue(SlowLogLevel.TRACE.isLevelEnabledFor(SlowLogLevel.TRACE));
        assertTrue(SlowLogLevel.TRACE.isLevelEnabledFor(SlowLogLevel.DEBUG));
        assertTrue(SlowLogLevel.TRACE.isLevelEnabledFor(SlowLogLevel.INFO));
        assertTrue(SlowLogLevel.TRACE.isLevelEnabledFor(SlowLogLevel.WARN));
    }

    public void testDebugPrecedence() {
        assertFalse(SlowLogLevel.DEBUG.isLevelEnabledFor(SlowLogLevel.TRACE));

        assertTrue(SlowLogLevel.DEBUG.isLevelEnabledFor(SlowLogLevel.DEBUG));
        assertTrue(SlowLogLevel.DEBUG.isLevelEnabledFor(SlowLogLevel.INFO));
        assertTrue(SlowLogLevel.DEBUG.isLevelEnabledFor(SlowLogLevel.WARN));
    }

    public void testInfoPrecedence() {
        assertFalse(SlowLogLevel.INFO.isLevelEnabledFor(SlowLogLevel.TRACE));
        assertFalse(SlowLogLevel.INFO.isLevelEnabledFor(SlowLogLevel.DEBUG));

        assertTrue(SlowLogLevel.INFO.isLevelEnabledFor(SlowLogLevel.INFO));
        assertTrue(SlowLogLevel.INFO.isLevelEnabledFor(SlowLogLevel.WARN));
    }

    public void testWarnPrecedence() {
        assertFalse(SlowLogLevel.WARN.isLevelEnabledFor(SlowLogLevel.TRACE));
        assertFalse(SlowLogLevel.WARN.isLevelEnabledFor(SlowLogLevel.DEBUG));
        assertFalse(SlowLogLevel.WARN.isLevelEnabledFor(SlowLogLevel.INFO));

        assertTrue(SlowLogLevel.WARN.isLevelEnabledFor(SlowLogLevel.WARN));
    }
}

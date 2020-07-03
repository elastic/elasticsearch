/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

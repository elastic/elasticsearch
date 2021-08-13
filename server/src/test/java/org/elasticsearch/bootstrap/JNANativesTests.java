/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class JNANativesTests extends ESTestCase {
    public void testMlockall() {
        if (Constants.MAC_OS_X) {
            assertFalse("Memory locking is not available on OS X platforms", JNANatives.LOCAL_MLOCKALL);
        }
    }

    public void testConsoleCtrlHandler() {
        if (Constants.WINDOWS) {
            assertNotNull(JNAKernel32Library.getInstance());
            assertThat(JNAKernel32Library.getInstance().getCallbacks().size(), equalTo(1));
        } else {
            assertNotNull(JNAKernel32Library.getInstance());
            assertThat(JNAKernel32Library.getInstance().getCallbacks().size(), equalTo(0));
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.test.ESTestCase;

public class JNANativesTests extends ESTestCase {
    public void testMlockall() {
        if (Constants.MAC_OS_X) {
            assertFalse("Memory locking is not available on OS X platforms", NativeAccess.instance().isMemoryLocked());
        }
    }
}

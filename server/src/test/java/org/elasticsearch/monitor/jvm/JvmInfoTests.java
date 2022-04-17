/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

public class JvmInfoTests extends ESTestCase {

    public void testUseG1GC() {
        // if we are running on HotSpot, and the test JVM was started
        // with UseG1GC, then JvmInfo should successfully report that
        // G1GC is enabled
        if (Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK")) {
            assertEquals(Boolean.toString(isG1GCEnabled()), JvmInfo.jvmInfo().useG1GC());
        } else {
            assertEquals("unknown", JvmInfo.jvmInfo().useG1GC());
        }
    }

    private boolean isG1GCEnabled() {
        final String argline = System.getProperty("tests.jvm.argline");
        final boolean g1GCEnabled = flagIsEnabled(argline, "UseG1GC");
        final boolean noOtherCollectorSpecified = argline == null
            || (flagIsEnabled(argline, "UseParNewGC") == false
                && flagIsEnabled(argline, "UseParallelGC") == false
                && flagIsEnabled(argline, "UseParallelOldGC") == false
                && flagIsEnabled(argline, "UseSerialGC") == false
                && flagIsEnabled(argline, "UseConcMarkSweepGC") == false);
        return g1GCEnabled || noOtherCollectorSpecified;
    }

    private boolean flagIsEnabled(String argline, String flag) {
        final boolean containsPositiveFlag = argline != null && argline.contains("-XX:+" + flag);
        if (containsPositiveFlag == false) {
            return false;
        }
        final int index = argline.lastIndexOf(flag);
        return argline.charAt(index - 1) == '+';
    }
}

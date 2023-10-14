/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.test.ESTestCase;

public class FrameGroupIDTests extends ESTestCase {
    public void testEmptyFunctionName() {
        String frameGroupID = FrameGroupID.create("FEDCBA9876543210", 177863, "", "", "");
	assertEquals("FEDCBA9876543210177863", frameGroupID);
    }

    public void testFunctionNameAndEmptySourceFilename() {
        String frameGroupID = FrameGroupID.create("FEDCBA9876543210", 6694, "<main>", "", "void jdk.internal.misc.Unsafe.park(boolean, long)");
	assertEquals("<main>void jdk.internal.misc.Unsafe.park(boolean, long)", frameGroupID);
    }

    public void testFunctionNameAndSourceFilenameWithAbsolutePath() {
        String frameGroupID = FrameGroupID.create("FEDCBA9876543210", 64, "main", "/usr/local/go/src/runtime/lock_futex.go", "futex_wake");
	assertEquals("mainfutex_wakelock_futex.go", frameGroupID);
    }

    public void testFunctionNameAndSourceFilenameWithoutAbsolutePath() {
        String frameGroupID = FrameGroupID.create("FEDCBA9876543210", 29338, "<main>", "bootstrap.java", "void jdk.internal.misc.Unsafe.park(boolean, long)");
	assertEquals("<main>void jdk.internal.misc.Unsafe.park(boolean, long)bootstrap.java", frameGroupID);
    }
}

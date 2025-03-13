/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.test.ESTestCase;

import java.util.Objects;

public class FrameGroupIDTests extends ESTestCase {
    public void testEmptySourceFilename() {
        String given = FrameGroupID.getBasenameAndParent("");
        assertEquals("", given);
    }

    public void testNonPathSourceFilename() {
        String given = FrameGroupID.getBasenameAndParent("void jdk.internal.misc.Unsafe.park(boolean, long)");
        assertEquals("void jdk.internal.misc.Unsafe.park(boolean, long)", given);
    }

    public void testRootSourceFilename() {
        String given = FrameGroupID.getBasenameAndParent("/");
        assertEquals("/", given);
    }

    public void testRelativePathSourceFilename() {
        String given = FrameGroupID.getBasenameAndParent("../src/main.c");
        assertEquals("src/main.c", given);
    }

    public void testAbsolutePathSourceFilename() {
        String given = FrameGroupID.getBasenameAndParent("/usr/local/go/src/runtime/lock_futex.go");
        assertEquals("runtime/lock_futex.go", given);
    }

    public void testEmptyFunctionName() {
        String expected = Integer.toString(Objects.hash("FEDCBA9876543210", 177863));
        String given = FrameGroupID.create("FEDCBA9876543210", 177863, "", "", "");
        assertEquals(expected, given);
    }

    public void testFunctionNameAndEmptySourceFilename() {
        String expected = Integer.toString(Objects.hash("FEDCBA9876543210", "void jdk.internal.misc.Unsafe.park(boolean, long)"));
        String given = FrameGroupID.create("FEDCBA9876543210", 6694, "<main>", "", "void jdk.internal.misc.Unsafe.park(boolean, long)");
        assertEquals(expected, given);
    }

    public void testFunctionNameAndSourceFilenameWithAbsolutePath() {
        String expected = Integer.toString(
            Objects.hash("main", "futex_wake", FrameGroupID.getBasenameAndParent("/usr/local/go/src/runtime/lock_futex.go"))
        );
        String given = FrameGroupID.create("FEDCBA9876543210", 64, "main", "/usr/local/go/src/runtime/lock_futex.go", "futex_wake");
        assertEquals(expected, given);
    }

    public void testFunctionNameAndSourceFilenameWithoutAbsolutePath() {
        String expected = Integer.toString(
            Objects.hash("<main>", "void jdk.internal.misc.Unsafe.park(boolean, long)", FrameGroupID.getBasenameAndParent("bootstrap.java"))
        );
        String given = FrameGroupID.create(
            "FEDCBA9876543210",
            29338,
            "<main>",
            "bootstrap.java",
            "void jdk.internal.misc.Unsafe.park(boolean, long)"
        );
        assertEquals(expected, given);
    }
}

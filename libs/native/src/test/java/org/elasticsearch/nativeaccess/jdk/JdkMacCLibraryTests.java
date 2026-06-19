/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import org.apache.lucene.util.Constants;
import org.elasticsearch.foreign.MemorySegmentUtil;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.jdk.JdkMacCLibrary.JdkErrorReference;
import org.elasticsearch.nativeaccess.lib.MacCLibrary;
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class JdkMacCLibraryTests extends ESTestCase {

    NativeAccess nativeAccess;
    MacCLibrary macCLibrary;

    @Before
    public void setup() {
        nativeAccess = NativeAccess.instance();
        if (Constants.MAC_OS_X) {
            macCLibrary = NativeLibraryProvider.instance().getLibrary(MacCLibrary.class);
            assertNotNull(macCLibrary);
        } else {
            assumeFalse("macCLibrary only available on Mac", Constants.WINDOWS && Constants.LINUX);
        }
    }

    /**
     * Simulates the sandbox_init failure path: allocate a native C string,
     * write its pointer into the ErrorReference (as sandbox_init would),
     * then call toString() — which hits the buggy getUtf8String(0) call.
     */
    public void testErrorReferenceToString() {
        try (Arena arena = Arena.ofConfined()) {
            String expected = "sandbox error: syntax error";
            MemorySegment nativeString = MemorySegmentUtil.allocateString(arena, expected);

            JdkErrorReference errorRef = (JdkErrorReference) macCLibrary.newErrorReference();
            // Simulate what sandbox_init does on failure: write a char* pointer
            // into the error reference's pointer-sized segment.
            errorRef.segment.set(ValueLayout.ADDRESS, 0, nativeString);

            assertEquals(expected, errorRef.toString());
        }
    }
}

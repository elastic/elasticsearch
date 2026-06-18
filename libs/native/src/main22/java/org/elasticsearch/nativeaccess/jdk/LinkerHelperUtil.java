/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess.jdk;

import java.lang.foreign.Linker;

public class LinkerHelperUtil {

    static final Linker.Option[] ALLOW_HEAP_ACCESS = new Linker.Option[] { Linker.Option.critical(true) };

    /** Returns a linker option used to mark a foreign function as critical. */
    static Linker.Option[] critical() {
        return ALLOW_HEAP_ACCESS;
    }

    /**
     * Whether downcalls may receive heap {@link java.lang.foreign.MemorySegment}s directly via
     * {@code Linker.Option.critical(boolean)}. Always true on JDK 22+.
     */
    static boolean heapAccessAvailable() {
        return true;
    }

    private LinkerHelperUtil() {}
}

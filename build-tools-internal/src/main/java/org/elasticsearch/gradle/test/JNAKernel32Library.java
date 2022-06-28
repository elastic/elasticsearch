/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.test;

import com.sun.jna.Native;
import com.sun.jna.WString;

import org.apache.tools.ant.taskdefs.condition.Os;

public class JNAKernel32Library {

    private static final class Holder {
        private static final JNAKernel32Library instance = new JNAKernel32Library();
    }

    static JNAKernel32Library getInstance() {
        return Holder.instance;
    }

    private JNAKernel32Library() {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            Native.register("kernel32");
        }
    }

    native int GetShortPathNameW(WString lpszLongPath, char[] lpszShortPath, int cchBuffer);

}

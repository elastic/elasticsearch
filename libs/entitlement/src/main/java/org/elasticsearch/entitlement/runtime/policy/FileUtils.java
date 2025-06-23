/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import static java.lang.Character.isLetter;

public class FileUtils {

    private FileUtils() {}

    /**
     * Tests if a path is absolute or relative, taking into consideration both Unix and Windows conventions.
     * Note that this leads to a conflict, resolved in favor of Unix rules: `/foo` can be either a Unix absolute path, or a Windows
     * relative path with "wrong" directory separator (using non-canonical / in Windows).
     * This method is intended to be used as validation for different file entitlements format: therefore it is preferable to reject a
     * relative path that is definitely absolute on Unix, rather than accept it as a possible relative path on Windows (if that is the case,
     * the developer can easily fix the path by using the correct platform separators).
     */
    public static boolean isAbsolutePath(String path) {
        if (path.isEmpty()) {
            return false;
        }
        if (path.charAt(0) == '/') {
            // Unix/BSD absolute
            return true;
        }

        return isWindowsAbsolutePath(path);
    }

    /**
     * When testing for path separators in a platform-agnostic way, we may encounter both kinds of slashes, especially when
     * processing windows paths. The JDK parses paths the same way under Windows.
     */
    static boolean isSlash(char c) {
        return (c == '\\') || (c == '/');
    }

    private static boolean isWindowsAbsolutePath(String input) {
        // if a prefix is present, we expected (long) UNC or (long) absolute
        if (input.startsWith("\\\\?\\")) {
            return true;
        }

        if (input.length() > 1) {
            char c0 = input.charAt(0);
            char c1 = input.charAt(1);
            if (isSlash(c0) && isSlash(c1)) {
                // Two slashes or more: UNC
                return true;
            }
            if (isLetter(c0) && c1 == ':') {
                // A drive: absolute
                return true;
            }
        }
        // Otherwise relative
        return false;
    }
}

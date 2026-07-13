/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Comparator;

abstract class FileAccessTreeComparison {

    private static final Logger logger = LogManager.getLogger(FileAccessTreeComparison.class);

    interface CharComparator {
        int compare(char c1, char c2);
    }

    private final Comparator<String> pathComparator;

    private final char separatorChar;

    /**
     * For our lexicographic sort trick to work correctly, we must have path separators sort before
     * any other character so that files in a directory appear immediately after that directory.
     * For example, we require [/a, /a/b, /a.xml] rather than the natural order [/a, /a.xml, /a/b].
     */
    FileAccessTreeComparison(CharComparator charComparator, char separatorChar) {
        pathComparator = (s1, s2) -> {
            int len1 = s1.length();
            int len2 = s2.length();
            int lim = Math.min(len1, len2);
            for (int k = 0; k < lim; k++) {
                char c1 = s1.charAt(k);
                char c2 = s2.charAt(k);
                var comp = charComparator.compare(c1, c2);
                if (comp == 0) {
                    continue;
                }
                boolean c1IsSeparator = c1 == separatorChar;
                boolean c2IsSeparator = c2 == separatorChar;
                if (c1IsSeparator == false || c2IsSeparator == false) {
                    if (c1IsSeparator) {
                        return -1;
                    }
                    if (c2IsSeparator) {
                        return 1;
                    }
                    return comp;
                }
            }
            return len1 - len2;
        };
        this.separatorChar = separatorChar;
    }

    Comparator<String> pathComparator() {
        return pathComparator;
    }

    boolean isParent(String maybeParent, String path) {
        logger.trace(() -> Strings.format("checking isParent [%s] for [%s]", maybeParent, path));
        return pathStartsWith(path, maybeParent)
            && (path.length() > maybeParent.length() && path.charAt(maybeParent.length()) == separatorChar);
    }

    boolean samePath(String currentPath, String nextPath) {
        return pathsAreEqual(currentPath, nextPath);
    }

    protected abstract boolean pathStartsWith(String pathString, String pathPrefix);

    protected abstract boolean pathsAreEqual(String path1, String path2);
}

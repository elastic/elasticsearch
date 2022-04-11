/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.core;

import java.util.Iterator;

//TODO PG remove.. a copy of regex from server
public class RegexCopy {
    private RegexCopy() {}

    public static boolean simpleMatch(String pattern, String str) {
        return simpleMatch(pattern, str, false);
    }

    /**
     * Match a String against the given pattern, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     *
     * @param pattern the pattern to match against
     * @param str     the String to match
     * @param caseInsensitive  true if ASCII case differences should be ignored
     * @return whether the String matches the given pattern
     */
    public static boolean simpleMatch(String pattern, String str, boolean caseInsensitive) {
        if (pattern == null || str == null) {
            return false;
        }
        if (caseInsensitive) {
            pattern = toLowercaseAscii(pattern);
            str = toLowercaseAscii(str);
        }
        return simpleMatchWithNormalizedStrings(pattern, str);
    }

    public static String toLowercaseAscii(String in) {
        StringBuilder out = new StringBuilder();
        Iterator<Integer> iter = in.codePoints().iterator();
        while (iter.hasNext()) {
            int codepoint = iter.next();
            if (codepoint > 128) {
                out.appendCodePoint(codepoint);
            } else {
                out.appendCodePoint(Character.toLowerCase(codepoint));
            }
        }
        return out.toString();
    }

    private static boolean simpleMatchWithNormalizedStrings(String pattern, String str) {
        final int firstIndex = pattern.indexOf('*');
        if (firstIndex == -1) {
            return pattern.equals(str);
        }
        if (firstIndex == 0) {
            if (pattern.length() == 1) {
                return true;
            }
            final int nextIndex = pattern.indexOf('*', firstIndex + 1);
            if (nextIndex == -1) {
                // str.endsWith(pattern.substring(1)), but avoiding the construction of pattern.substring(1):
                return str.regionMatches(str.length() - pattern.length() + 1, pattern, 1, pattern.length() - 1);
            } else if (nextIndex == 1) {
                // Double wildcard "**" - skipping the first "*"
                return simpleMatchWithNormalizedStrings(pattern.substring(1), str);
            }
            final String part = pattern.substring(1, nextIndex);
            int partIndex = str.indexOf(part);
            while (partIndex != -1) {
                if (simpleMatchWithNormalizedStrings(pattern.substring(nextIndex), str.substring(partIndex + part.length()))) {
                    return true;
                }
                partIndex = str.indexOf(part, partIndex + 1);
            }
            return false;
        }
        return str.regionMatches(0, pattern, 0, firstIndex)
            && (firstIndex == pattern.length() - 1 // only wildcard in pattern is at the end, so no need to look at the rest of the string
                || simpleMatchWithNormalizedStrings(pattern.substring(firstIndex), str.substring(firstIndex)));
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 * Utility class for glob-like matching
 */
public class Glob {

    private Glob() {}

    /**
     * Match a String against the given pattern, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     *
     * @param pattern the pattern to match against
     * @param str     the String to match
     * @return whether the String matches the given pattern
     */
    public static boolean globMatch(String pattern, String str) {
        if (pattern == null || str == null) {
            return false;
        }

        int patternIndex = pattern.indexOf('*');
        if (patternIndex == -1) {
            // Nothing to glob
            return pattern.equals(str);
        }

        if (patternIndex == 0) {
            // If the pattern is a literal '*' then it matches any input
            if (pattern.length() == 1) {
                return true;
            }
        } else {
            if (str.regionMatches(0, pattern, 0, patternIndex) == false) {
                // If the pattern starts with a literal (i.e. not '*') then the input string must also start with that
                return false;
            }
            if (patternIndex == pattern.length() - 1) {
                // The pattern is "something*", so if the starting region matches, then the whole pattern matches
                return true;
            }
        }

        int strIndex = patternIndex;
        while (strIndex < str.length()) {
            assert pattern.charAt(patternIndex) == '*' : "Expected * at index " + patternIndex + " of [" + pattern + "]";

            // skip over the '*'
            patternIndex++;

            if (patternIndex == pattern.length()) {
                // The pattern ends in '*' (that is, "something*" or "*some*thing*", etc)
                // Since we already matched everything up to the '*' we know the string matches (whatever is left over must match '*')
                // so we're automatically done
                return true;
            }

            // Look for the next '*'
            int nextStar = pattern.indexOf('*', patternIndex);
            while (nextStar == patternIndex) {
                // Two (or more) stars in sequence, just skip the subsequent ones
                patternIndex++;
                nextStar = pattern.indexOf('*', patternIndex);
            }
            if (nextStar == -1) {
                // We've come to the last '*' in a pattern (.e.g the 2nd one in "*some*thing")
                // In this case we match if the input string ends in "thing" (but constrained by the current position)
                final int len = pattern.length() - patternIndex;
                final int strSuffixStart = str.length() - len;
                if (strSuffixStart < strIndex) {
                    // The suffix would start before the current position. That means it's not a match
                    // e.g. "abc" is not a match for "ab*bc" even though "abc" does end with "bc"
                    return false;
                }
                return str.regionMatches(strSuffixStart, pattern, patternIndex, len);
            } else {
                // There is another star, with a literal in between the current position and that '*'
                // That is, we have "*literal*"
                // We want the first '*' to consume everything up until the first occurrence of "literal" in the input string
                int match = str.indexOf(pattern.substring(patternIndex, nextStar), strIndex);
                if (match == -1) {
                    // If "literal" isn't there, then the match fails.
                    return false;
                }
                // Move both index (pointer) values to the end of the literal
                strIndex = match + (nextStar - patternIndex);
                patternIndex = nextStar;
            }
        }

        // We might have trailing '*'s in the pattern after completing a literal match at the end of the input string
        // e.g. a glob of "el*ic*" matching "elastic" - we need to consume that last '*' without it matching anything
        while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
            patternIndex++;
        }

        // The match is successful only if we have consumed the entire pattern.
        return patternIndex == pattern.length();
    }

}

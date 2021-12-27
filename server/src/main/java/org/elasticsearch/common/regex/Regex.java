/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.regex;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class Regex {

    /**
     * This Regex / {@link Pattern} flag is supported from Java 7 on.
     * If set on a Java6 JVM the flag will be ignored.
     */
    public static final int UNICODE_CHARACTER_CLASS = 0x100; // supported in JAVA7

    /**
     * Is the str a simple match pattern.
     */
    public static boolean isSimpleMatchPattern(String str) {
        return str.indexOf('*') != -1;
    }

    public static boolean isMatchAllPattern(String str) {
        return str.equals("*");
    }

    /** Return an {@link Automaton} that matches the given pattern. */
    public static Automaton simpleMatchToAutomaton(String pattern) {
        List<Automaton> automata = new ArrayList<>();
        int previous = 0;
        for (int i = pattern.indexOf('*'); i != -1; i = pattern.indexOf('*', i + 1)) {
            automata.add(Automata.makeString(pattern.substring(previous, i)));
            automata.add(Automata.makeAnyString());
            previous = i + 1;
        }
        automata.add(Automata.makeString(pattern.substring(previous)));
        return Operations.concatenate(automata);
    }

    /**
     * Return an Automaton that matches the union of the provided patterns.
     */
    public static Automaton simpleMatchToAutomaton(String... patterns) {
        if (patterns.length < 1) {
            throw new IllegalArgumentException("There must be at least one pattern, zero given");
        }

        List<BytesRef> simpleStrings = new ArrayList<>();
        List<Automaton> automata = new ArrayList<>();
        for (String pattern : patterns) {
            // Strings longer than 1000 characters aren't supported by makeStringUnion
            if (isSimpleMatchPattern(pattern) || pattern.length() >= 1000) {
                automata.add(simpleMatchToAutomaton(pattern));
            } else {
                simpleStrings.add(new BytesRef(pattern));
            }
        }
        if (false == simpleStrings.isEmpty()) {
            Automaton simpleStringsAutomaton;
            if (simpleStrings.size() > 0) {
                Collections.sort(simpleStrings);
                simpleStringsAutomaton = Automata.makeStringUnion(simpleStrings);
            } else {
                simpleStringsAutomaton = Automata.makeString(simpleStrings.get(0).utf8ToString());
            }
            if (automata.isEmpty()) {
                return simpleStringsAutomaton;
            }
            automata.add(simpleStringsAutomaton);
        }
        return Operations.union(automata);
    }

    /**
     * Match a String against the given pattern, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     * Matching is case sensitive.
     *
     * @param pattern the pattern to match against
     * @param str     the String to match
     * @return whether the String matches the given pattern
     */
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
            pattern = Strings.toLowercaseAscii(pattern);
            str = Strings.toLowercaseAscii(str);
        }
        return simpleMatchWithNormalizedStrings(pattern, str);
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

    /**
     * Match a String against the given patterns, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     *
     * @param patterns the patterns to match against
     * @param str      the String to match
     * @return whether the String matches any of the given patterns
     */
    public static boolean simpleMatch(String[] patterns, String str) {
        if (patterns != null) {
            for (String pattern : patterns) {
                if (simpleMatch(pattern, str)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Similar to {@link #simpleMatch(String[], String)}, but accepts a list of strings instead of an array of strings for the patterns to
     * match.
     */
    public static boolean simpleMatch(final List<String> patterns, final String str) {
        // #simpleMatch(String[], String) is likely to be inlined into this method
        return patterns != null && simpleMatch(patterns.toArray(Strings.EMPTY_ARRAY), str);
    }

    public static boolean simpleMatch(String[] patterns, String[] types) {
        if (patterns != null && types != null) {
            for (String type : types) {
                for (String pattern : patterns) {
                    if (simpleMatch(pattern, type)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static Pattern compile(String regex, String flags) {
        int pFlags = flags == null ? 0 : flagsFromString(flags);
        return Pattern.compile(regex, pFlags);
    }

    public static int flagsFromString(String flags) {
        int pFlags = 0;
        for (String s : Strings.delimitedListToStringArray(flags, "|")) {
            if (s.isEmpty()) {
                continue;
            }
            s = s.toUpperCase(Locale.ROOT);
            if ("CASE_INSENSITIVE".equals(s)) {
                pFlags |= Pattern.CASE_INSENSITIVE;
            } else if ("MULTILINE".equals(s)) {
                pFlags |= Pattern.MULTILINE;
            } else if ("DOTALL".equals(s)) {
                pFlags |= Pattern.DOTALL;
            } else if ("UNICODE_CASE".equals(s)) {
                pFlags |= Pattern.UNICODE_CASE;
            } else if ("CANON_EQ".equals(s)) {
                pFlags |= Pattern.CANON_EQ;
            } else if ("UNIX_LINES".equals(s)) {
                pFlags |= Pattern.UNIX_LINES;
            } else if ("LITERAL".equals(s)) {
                pFlags |= Pattern.LITERAL;
            } else if ("COMMENTS".equals(s)) {
                pFlags |= Pattern.COMMENTS;
            } else if (("UNICODE_CHAR_CLASS".equals(s)) || ("UNICODE_CHARACTER_CLASS".equals(s))) {
                pFlags |= UNICODE_CHARACTER_CLASS;
            } else {
                throw new IllegalArgumentException("Unknown regex flag [" + s + "]");
            }
        }
        return pFlags;
    }

    public static String flagsToString(int flags) {
        StringBuilder sb = new StringBuilder();
        if ((flags & Pattern.CASE_INSENSITIVE) != 0) {
            sb.append("CASE_INSENSITIVE|");
        }
        if ((flags & Pattern.MULTILINE) != 0) {
            sb.append("MULTILINE|");
        }
        if ((flags & Pattern.DOTALL) != 0) {
            sb.append("DOTALL|");
        }
        if ((flags & Pattern.UNICODE_CASE) != 0) {
            sb.append("UNICODE_CASE|");
        }
        if ((flags & Pattern.CANON_EQ) != 0) {
            sb.append("CANON_EQ|");
        }
        if ((flags & Pattern.UNIX_LINES) != 0) {
            sb.append("UNIX_LINES|");
        }
        if ((flags & Pattern.LITERAL) != 0) {
            sb.append("LITERAL|");
        }
        if ((flags & Pattern.COMMENTS) != 0) {
            sb.append("COMMENTS|");
        }
        if ((flags & UNICODE_CHARACTER_CLASS) != 0) {
            sb.append("UNICODE_CHAR_CLASS|");
        }
        return sb.toString();
    }
}

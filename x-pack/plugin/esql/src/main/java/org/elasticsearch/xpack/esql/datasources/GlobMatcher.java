/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.regex.Pattern;

/**
 * Converts glob patterns to Java regex and matches relative paths against them.
 * Supports: {@code *} (single segment), {@code **} (recursive), {@code ?}, {@code {a,b}}, {@code [abc]}.
 */
final class GlobMatcher {

    private static final char PATH_SEP = StoragePath.PATH_SEPARATOR.charAt(0);
    private static final String DOUBLE_STAR = "**";
    private static final String REGEX_ESCAPED_CHARS = ".+^$|()\\";

    private final String glob;
    private final Pattern pattern;
    private final boolean recursive;

    GlobMatcher(String glob) {
        if (glob == null) {
            throw new IllegalArgumentException("glob pattern cannot be null");
        }
        this.glob = glob;
        this.recursive = glob.contains(DOUBLE_STAR);
        this.pattern = Pattern.compile(globToRegex(glob));
    }

    boolean matches(String relativePath) {
        if (relativePath == null) {
            return false;
        }
        return pattern.matcher(relativePath).matches();
    }

    boolean needsRecursion() {
        return recursive;
    }

    String glob() {
        return glob;
    }

    // Converts a glob pattern to a Java regex string.
    // Supports: ** (recursive), * (single segment), ? (one char), {a,b} (alternatives), [abc] (char class)
    @SuppressWarnings("RegexpMultiline")
    static String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder();
        int i = 0;
        int len = glob.length();

        while (i < len) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> {
                    if (i + 1 < len && glob.charAt(i + 1) == '*') {
                        // ** matches everything including path separators
                        // Handle optional surrounding slashes: **/ or / ** /
                        int start = i;
                        i += 2;
                        // Skip trailing slash after **
                        if (i < len && glob.charAt(i) == PATH_SEP) {
                            i++;
                        }
                        // If ** was preceded by a slash, the slash is part of the pattern
                        if (start > 0 && glob.charAt(start - 1) == PATH_SEP) {
                            // Replace the trailing slash we already added with the ** pattern
                            if (regex.length() > 0 && regex.charAt(regex.length() - 1) == PATH_SEP) {
                                regex.deleteCharAt(regex.length() - 1);
                            }
                            regex.append("(?:").append(PATH_SEP).append(".*)?");
                            // If there's more pattern after **, add a slash separator
                            if (i < len) {
                                regex.append(PATH_SEP);
                            }
                        } else {
                            regex.append(".*");
                        }
                    } else {
                        // * matches everything except path separators
                        regex.append("[^").append(PATH_SEP).append("]*");
                        i++;
                    }
                }
                case '?' -> {
                    regex.append("[^").append(PATH_SEP).append("]");
                    i++;
                }
                case '{' -> {
                    regex.append("(?:");
                    i++;
                    while (i < len && glob.charAt(i) != '}') {
                        if (glob.charAt(i) == ',') {
                            regex.append("|");
                        } else {
                            appendEscaped(regex, glob.charAt(i));
                        }
                        i++;
                    }
                    regex.append(")");
                    if (i < len) {
                        i++; // skip closing }
                    }
                }
                case '[' -> {
                    regex.append("[");
                    i++;
                    // Handle negation: [!...] or [^...]
                    if (i < len && (glob.charAt(i) == '!' || glob.charAt(i) == '^')) {
                        regex.append("^");
                        i++;
                    }
                    while (i < len && glob.charAt(i) != ']') {
                        appendEscaped(regex, glob.charAt(i));
                        i++;
                    }
                    regex.append("]");
                    if (i < len) {
                        i++; // skip closing ]
                    }
                }
                default -> {
                    appendEscaped(regex, c);
                    i++;
                }
            }
        }

        return regex.toString();
    }

    private static void appendEscaped(StringBuilder sb, char c) {
        if (REGEX_ESCAPED_CHARS.indexOf(c) >= 0) {
            sb.append('\\');
        }
        sb.append(c);
    }

    @Override
    public String toString() {
        return "GlobMatcher[" + glob + "]";
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

public abstract class StringUtils {
    public static final String EMPTY = "";
    public static final String SLASH = "/";
    public static final String PATH_TOP = "..";
    public static final String PATH_CURRENT = ".";
    public static final String DEFAULT_DELIMITER = ",";

    public static String nullAsEmpty(String string) {
        return string == null ? EMPTY : string;
    }

    public static boolean hasText(CharSequence sequence) {
        if (hasLength(sequence) == false) {
            return false;
        }
        int length = sequence.length();
        for (int i = 0; i < length; i++) {
            if (Character.isWhitespace(sequence.charAt(i)) == false) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasLength(CharSequence sequence) {
        return (sequence != null && sequence.length() > 0);
    }

    public static boolean isUpperCase(CharSequence sequence) {
        for (int i = 0; i < sequence.length(); i++) {
            if (Character.isLetter(sequence.charAt(i)) && Character.isUpperCase(sequence.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    public static String[] splitToIndexAndType(String pattern) {
        List<String> tokens = tokenize(pattern, ".");

        String[] results = new String[2];
        if (tokens.size() == 2) {
            results[0] = tokens.get(0);
            results[1] = tokens.get(1);
        } else {
            results[0] = nullAsEmpty(pattern);
            results[1] = EMPTY;
        }

        return results;
    }

    public static List<String> tokenize(String string) {
        return tokenize(string, DEFAULT_DELIMITER);
    }

    public static List<String> tokenize(String string, String delimiters) {
        return tokenize(string, delimiters, true, true);
    }

    public static List<String> tokenize(String string, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {
        if (hasText(string) == false) {
            return Collections.emptyList();
        }
        StringTokenizer st = new StringTokenizer(string, delimiters);
        List<String> tokens = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (ignoreEmptyTokens == false || token.length() > 0) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    public static String concatenate(Collection<?> list) {
        return concatenate(list, DEFAULT_DELIMITER);
    }

    public static String concatenate(Collection<?> list, String delimiter) {
        if (list == null || list.isEmpty()) {
            return EMPTY;
        }
        if (delimiter == null) {
            delimiter = EMPTY;
        }
        StringBuilder sb = new StringBuilder();

        for (Object object : list) {
            sb.append(object.toString());
            sb.append(delimiter);
        }

        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    public static String normalize(String path) {
        if (path == null) {
            return null;
        }
        String pathToUse = path.replace("\\", SLASH);

        int prefixIndex = pathToUse.indexOf(":");
        String prefix = "";
        if (prefixIndex != -1) {
            prefix = pathToUse.substring(0, prefixIndex + 1);
            if (prefix.contains(SLASH)) {
                prefix = "";
            } else {
                pathToUse = pathToUse.substring(prefixIndex + 1);
            }
        }
        if (pathToUse.startsWith(SLASH)) {
            prefix = prefix + SLASH;
            pathToUse = pathToUse.substring(1);
        }

        List<String> pathList = tokenize(pathToUse, SLASH);
        List<String> pathTokens = new LinkedList<String>();
        int tops = 0;

        for (int i = pathList.size() - 1; i >= 0; i--) {
            String element = pathList.get(i);
            if (PATH_CURRENT.equals(element)) {
                // current folder, ignore it
            } else if (PATH_TOP.equals(element)) {
                // top folder, skip previous element
                tops++;
            } else {
                if (tops > 0) {
                    // should it be skipped?
                    tops--;
                } else {
                    pathTokens.add(0, element);
                }
            }
        }

        for (int i = 0; i < tops; i++) {
            pathTokens.add(0, PATH_TOP);
        }

        return prefix + concatenate(pathTokens, SLASH);
    }

    public static String asUTFString(byte[] content) {
        return asUTFString(content, 0, content.length);
    }

    public static String asUTFString(byte[] content, int offset, int length) {
        return (content == null || length == 0 ? EMPTY : new String(content, offset, length, StandardCharsets.UTF_8));
    }

    public static byte[] toUTF(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    // Based on "Algorithms on Strings, Trees and Sequences by Dan Gusfield".
    // returns -1 if the two strings are within the given threshold of each other, -1 otherwise
    private static int levenshteinDistance(CharSequence one, CharSequence another, int threshold) {
        int n = one.length();
        int m = another.length();

        // if one string is empty, the edit distance is necessarily the length of the other
        if (n == 0) {
            return m <= threshold ? m : -1;
        } else if (m == 0) {
            return n <= threshold ? n : -1;
        }

        if (n > m) {
            // swap the two strings to consume less memory
            final CharSequence tmp = one;
            one = another;
            another = tmp;
            n = m;
            m = another.length();
        }

        int p[] = new int[n + 1]; // 'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; // placeholder to assist in swapping p and d

        // fill in starting table values
        final int boundary = Math.min(n, threshold) + 1;
        for (int i = 0; i < boundary; i++) {
            p[i] = i;
        }

        // these fills ensure that the value above the rightmost entry of our
        // stripe will be ignored in following loop iterations
        Arrays.fill(p, boundary, p.length, Integer.MAX_VALUE);
        Arrays.fill(d, Integer.MAX_VALUE);

        for (int j = 1; j <= m; j++) {
            final char t_j = another.charAt(j - 1);
            d[0] = j;

            // compute stripe indices, constrain to array size
            final int min = Math.max(1, j - threshold);
            final int max = (j > Integer.MAX_VALUE - threshold) ? n : Math.min(n, j + threshold);

            // the stripe may lead off of the table if s and t are of different sizes
            if (min > max) {
                return -1;
            }

            // ignore entry left of leftmost
            if (min > 1) {
                d[min - 1] = Integer.MAX_VALUE;
            }

            // iterates through [min, max] in s
            for (int i = min; i <= max; i++) {
                if (one.charAt(i - 1) == t_j) {
                    // diagonally left and up
                    d[i] = p[i - 1];
                } else {
                    // 1 + minimum of cell to the left, to the top, diagonally left and up
                    d[i] = 1 + Math.min(Math.min(d[i - 1], p[i]), p[i - 1]);
                }
            }

            // copy current distance counts to 'previous row' distance counts
            _d = p;
            p = d;
            d = _d;
        }

        // if p[n] is greater than the threshold, there's no guarantee on it being the correct
        // distance
        if (p[n] <= threshold) {
            return p[n];
        }
        return -1;
    }

    public static List<String> findSimilar(CharSequence match, Collection<String> potential) {
        List<String> list = new ArrayList<String>(3);

        // 1 switches or 1 extra char
        int maxDistance = 5;

        for (String string : potential) {
            int dist = levenshteinDistance(match, string, maxDistance);
            if (dist >= 0) {
                if (dist < maxDistance) {
                    maxDistance = dist;
                    list.clear();
                    list.add(string);
                } else if (dist == maxDistance) {
                    list.add(string);
                }
            }
        }

        return list;
    }

    public static boolean parseBoolean(String input) {
        switch (input) {
            case "true":
                return true;
            case "false":
                return false;
            default:
                throw new IllegalArgumentException("must be [true] or [false]");
        }
    }

    public static String asHexString(byte[] content, int offset, int length) {
        StringBuilder buf = new StringBuilder();
        for (int i = offset; i < length; i++) {
            String hex = Integer.toHexString(0xFF & content[i]);
            if (hex.length() == 1) {
                buf.append('0');
            }
            buf.append(hex);
        }
        return buf.toString();
    }

    public static String repeatString(String in, int count) {
        if (count < 0) {
            throw new IllegalArgumentException("negative count: " + count);
        }
        StringBuffer sb = new StringBuffer(in.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(in);
        }
        return sb.toString();
    }
}

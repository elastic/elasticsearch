/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.stream.Collectors.toList;

public final class StringUtils {

    private StringUtils() {}

    public static final String EMPTY = "";
    public static final String NEW_LINE = "\n";
    public static final String SQL_WILDCARD = "%";

    private static final String[] INTEGER_ORDINALS = new String[] { "th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th" };

    //CamelCase to camel_case
    public static String camelCaseToUnderscore(String string) {
        if (!Strings.hasText(string)) {
            return EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        String s = string.trim();

        boolean previousCharWasUp = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isAlphabetic(ch)) {
                if (Character.isUpperCase(ch)) {
                    if (i > 0 && !previousCharWasUp) {
                        sb.append("_");
                    }
                    previousCharWasUp = true;
                }
                else {
                    previousCharWasUp = (ch == '_');
                }
            }
            else {
                previousCharWasUp = true;
            }
            sb.append(ch);
        }
        return sb.toString().toUpperCase(Locale.ROOT);
    }
    
    //CAMEL_CASE to camelCase
    public static String underscoreToLowerCamelCase(String string) {
        if (!Strings.hasText(string)) {
            return EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        String s = string.trim().toLowerCase(Locale.ROOT);

        boolean previousCharWasUnderscore = false;
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (ch == '_') {
                previousCharWasUnderscore = true;
            }
            else {
                if (previousCharWasUnderscore) {
                    sb.append(Character.toUpperCase(ch));
                    previousCharWasUnderscore = false;
                }
                else {
                    sb.append(ch);
                }
            }
        }
        return sb.toString();
    }

    // % -> .*
    // _ -> .
    // escape character - can be 0 (in which case every regex gets escaped) or
    // should be followed by % or _ (otherwise an exception is thrown)
    public static String likeToJavaPattern(String pattern, char escape) {
        StringBuilder regex = new StringBuilder(pattern.length() + 4);

        boolean escaped = false;
        regex.append('^');
        for (int i = 0; i < pattern.length(); i++) {
            char curr = pattern.charAt(i);
            if (!escaped && (curr == escape) && escape != 0) {
                escaped = true;
                if (i + 1 == pattern.length()) {
                    throw new SqlIllegalArgumentException(
                            "Invalid sequence - escape character is not followed by special wildcard char");
                }
            }
            else {
                switch (curr) {
                    case '%':
                        regex.append(escaped ? SQL_WILDCARD : ".*");
                        break;
                    case '_':
                        regex.append(escaped ? "_" : ".");
                        break;
                    default:
                        if (escaped) {
                            throw new SqlIllegalArgumentException(
                                    "Invalid sequence - escape character is not followed by special wildcard char");
                        }
                        // escape special regex characters
                        switch (curr) {
                            case '\\':
                            case '^':
                            case '$':
                            case '.':
                            case '*':
                            case '?':
                            case '+':
                            case '|':
                            case '(':
                            case ')':
                            case '[':
                            case ']':
                            case '{':
                            case '}':
                                regex.append('\\');
                        }
                        regex.append(curr);
                }
                escaped = false;
            }
        }
        regex.append('$');

        return regex.toString();
    }

    /**
     * Translates a like pattern to a Lucene wildcard.
     * This methods pays attention to the custom escape char which gets converted into \ (used by Lucene).
     * <pre>
     * % -&gt; *
     * _ -&gt; ?
     * escape character - can be 0 (in which case every regex gets escaped) or should be followed by
     * % or _ (otherwise an exception is thrown)
     * </pre>
     */
    public static String likeToLuceneWildcard(String pattern, char escape) {
        StringBuilder wildcard = new StringBuilder(pattern.length() + 4);

        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char curr = pattern.charAt(i);

            if (!escaped && (curr == escape) && escape != 0) {
                if (i + 1 == pattern.length()) {
                    throw new SqlIllegalArgumentException("Invalid sequence - escape character is not followed by special wildcard char");
                }
                escaped = true;
            } else {
                switch (curr) {
                    case '%':
                        wildcard.append(escaped ? SQL_WILDCARD : "*");
                        break;
                    case '_':
                        wildcard.append(escaped ? "_" : "?");
                        break;
                    default:
                        if (escaped) {
                            throw new SqlIllegalArgumentException(
                                    "Invalid sequence - escape character is not followed by special wildcard char");
                        }
                        // escape special regex characters
                        switch (curr) {
                            case '\\':
                            case '*':
                            case '?':
                              wildcard.append('\\');
                        }
                        wildcard.append(curr);
                }
                escaped = false;
            }
        }
        return wildcard.toString();
    }

    /**
     * Translates a like pattern to pattern for ES index name expression resolver.
     *
     * Note the resolver only supports * (not ?) and has no notion of escaping. This is not really an issue since we don't allow *
     * anyway in the pattern.
     */
    public static String likeToIndexWildcard(String pattern, char escape) {
        StringBuilder wildcard = new StringBuilder(pattern.length() + 4);

        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char curr = pattern.charAt(i);

            if (!escaped && (curr == escape) && escape != 0) {
                if (i + 1 == pattern.length()) {
                    throw new SqlIllegalArgumentException("Invalid sequence - escape character is not followed by special wildcard char");
                }
                escaped = true;
            } else {
                switch (curr) {
                    case '%':
                        wildcard.append(escaped ? SQL_WILDCARD : "*");
                        break;
                    case '_':
                        wildcard.append(escaped ? "_" : "*");
                        break;
                    default:
                        if (escaped) {
                            throw new SqlIllegalArgumentException(
                                    "Invalid sequence - escape character is not followed by special wildcard char");
                        }
                        // the resolver doesn't support escaping...
                        wildcard.append(curr);
                }
                escaped = false;
            }
        }
        return wildcard.toString();
    }

    public static String likeToUnescaped(String pattern, char escape) {
        StringBuilder wildcard = new StringBuilder(pattern.length());

        boolean escaped = false;
        for (int i = 0; i < pattern.length(); i++) {
            char curr = pattern.charAt(i);

            if (escaped == false && curr == escape && escape != 0) {
                escaped = true;
            } else {
                if (escaped == true && (curr == '%' || curr == '_' || curr == escape)) {
                    wildcard.append(curr);
                } else {
                    if (escaped) {
                        wildcard.append(escape);
                    }
                    wildcard.append(curr);
                }
                escaped = false;
            }
        }
        // corner-case when the escape char is the last char
        if (escaped == true) {
            wildcard.append(escape);
        }
        return wildcard.toString();
    }

    public static String toString(SearchSourceBuilder source) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true)) {
            source.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException("error rendering", e);
        }
    }

    public static List<String> findSimilar(String match, Iterable<String> potentialMatches) {
        LevenshteinDistance ld = new LevenshteinDistance();
        List<Tuple<Float, String>> scoredMatches = new ArrayList<>();
        for (String potentialMatch : potentialMatches) {
            float distance = ld.getDistance(match, potentialMatch);
            if (distance >= 0.5f) {
                scoredMatches.add(new Tuple<>(distance, potentialMatch));
            }
        }
        CollectionUtil.timSort(scoredMatches, (a,b) -> b.v1().compareTo(a.v1()));
        return scoredMatches.stream()
                .map(a -> a.v2())
                .collect(toList());
    }

    public static double parseDouble(String string) throws SqlIllegalArgumentException {
        double value;
        try {
            value = Double.parseDouble(string);
        } catch (NumberFormatException nfe) {
            throw new SqlIllegalArgumentException("Cannot parse number [{}]", string);
        }

        if (Double.isInfinite(value)) {
            throw new SqlIllegalArgumentException("Number [{}] is too large", string);
        }
        if (Double.isNaN(value)) {
            throw new SqlIllegalArgumentException("[{}] cannot be parsed as a number (NaN)", string);
        }
        return value;
    }

    public static long parseLong(String string) throws SqlIllegalArgumentException {
        try {
            return Long.parseLong(string);
        } catch (NumberFormatException nfe) {
            try {
                BigInteger bi = new BigInteger(string);
                try {
                    bi.longValueExact();
                } catch (ArithmeticException ae) {
                    throw new SqlIllegalArgumentException("Number [{}] is too large", string);
                }
            } catch (NumberFormatException ex) {
                // parsing fails, go through
            }
            throw new SqlIllegalArgumentException("Cannot parse number [{}]", string);
        }
    }

    public static String ordinal(int i) {
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                return i + INTEGER_ORDINALS[i % 10];

        }
    }
}

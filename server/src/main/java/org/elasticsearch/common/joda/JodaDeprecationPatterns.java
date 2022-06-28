/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.joda;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JodaDeprecationPatterns {
    public static final String USE_NEW_FORMAT_SPECIFIERS = "Use new java.time date format specifiers.";
    private static Map<String, String> JODA_PATTERNS_DEPRECATIONS = new LinkedHashMap<>();

    static {
        JODA_PATTERNS_DEPRECATIONS.put("Y", "'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.");
        JODA_PATTERNS_DEPRECATIONS.put("y", "'y' year should be replaced with 'u'. Use 'y' for year-of-era.");
        JODA_PATTERNS_DEPRECATIONS.put("C", "'C' century of era is no longer supported.");
        JODA_PATTERNS_DEPRECATIONS.put("x", "'x' weak-year should be replaced with 'Y'. Use 'x' for zone-offset.");
        JODA_PATTERNS_DEPRECATIONS.put("Z", "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'.");
        JODA_PATTERNS_DEPRECATIONS.put("z", "'z' time zone text. Will print 'Z' for Zulu given UTC timezone.");
    }

    /**
     * Checks if date parsing pattern is deprecated.
     * Deprecated here means: when it was not already prefixed with 8 (meaning already upgraded)
     * and it is not a predefined pattern from <code>FormatNames</code>  like basic_date_time_no_millis
     * and it uses pattern characters which changed meaning from joda to java like Y becomes y.
     * @param pattern - a format to be checked
     * @return true if format is deprecated, otherwise false
     */
    public static boolean isDeprecatedPattern(String pattern) {
        List<String> patterns = DateFormatter.splitCombinedPatterns(pattern);

        for (String subPattern : patterns) {
            boolean isDeprecated = subPattern.startsWith("8") == false
                && FormatNames.exist(subPattern) == false
                && JODA_PATTERNS_DEPRECATIONS.keySet().stream().filter(s -> subPattern.contains(s)).findAny().isPresent();
            if (isDeprecated) {
                return true;
            }
        }
        return false;
    }

    /**
     * Formats deprecation message for suggestion field in a warning header.
     * Joins all warnings in a one message.
     * @param pattern - a pattern to be formatted
     * @return a formatted deprecation message
     */
    public static String formatSuggestion(String pattern) {
        List<String> patterns = DateFormatter.splitCombinedPatterns(pattern);

        Set<String> warnings = new LinkedHashSet<>();
        for (String subPattern : patterns) {
            if (isDeprecatedPattern(subPattern)) {
                String suggestion = JODA_PATTERNS_DEPRECATIONS.entrySet()
                    .stream()
                    .filter(s -> subPattern.contains(s.getKey()))
                    .map(s -> s.getValue())
                    .collect(Collectors.joining("; "));
                warnings.add(suggestion);
            }
        }
        String combinedWarning = warnings.stream().collect(Collectors.joining("; "));
        return combinedWarning;
    }
}

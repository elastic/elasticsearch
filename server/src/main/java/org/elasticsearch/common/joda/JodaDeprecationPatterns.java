/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    public static final String USE_PREFIX_8_WARNING = "Prefix your date format with '8' to use the new specifier.";
    private static Map<String, String> JODA_PATTERNS_DEPRECATIONS = new LinkedHashMap<>();

    static {
        JODA_PATTERNS_DEPRECATIONS.put("Y", "'Y' year-of-era should be replaced with 'y'. Use 'Y' for week-based-year.");
        JODA_PATTERNS_DEPRECATIONS.put("y", "'y' year should be replaced with 'u'. Use 'y' for year-of-era.");
        JODA_PATTERNS_DEPRECATIONS.put("C", "'C' century of era is no longer supported.");
        JODA_PATTERNS_DEPRECATIONS.put("x", "'x' weak-year should be replaced with 'Y'. Use 'x' for zone-offset.");
        JODA_PATTERNS_DEPRECATIONS.put("Z",
            "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'.");
        JODA_PATTERNS_DEPRECATIONS.put("z",
            "'z' time zone text. Will print 'Z' for Zulu given UTC timezone.");
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
            boolean isDeprecated = subPattern.startsWith("8") == false && FormatNames.exist(subPattern) == false &&
                JODA_PATTERNS_DEPRECATIONS.keySet().stream()
                                          .filter(s -> subPattern.contains(s))
                                          .findAny()
                                          .isPresent();
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
                String suggestion = JODA_PATTERNS_DEPRECATIONS.entrySet().stream()
                                                              .filter(s -> subPattern.contains(s.getKey()))
                                                              .map(s -> s.getValue())
                                                              .collect(Collectors.joining("; "));
                warnings.add(suggestion);
            }
        }
        String combinedWarning = warnings.stream()
                                 .collect(Collectors.joining("; "));
        return combinedWarning;
    }
}

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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class JodaDeprecationPatterns {
    private static Map<String, String> JODA_PATTERNS_DEPRECATIONS = new LinkedHashMap<>();

    static {
        JODA_PATTERNS_DEPRECATIONS.put("Y", "'Y' year-of-era becomes 'y'. Use 'Y' for week-based-year.");
        JODA_PATTERNS_DEPRECATIONS.put("y", "'y' year becomes 'u'. Use 'y' for year-of-era.");
        JODA_PATTERNS_DEPRECATIONS.put("C", "'C' century of era is no longer supported.");
        JODA_PATTERNS_DEPRECATIONS.put("x", "'x' weak-year becomes 'Y'. Use 'x' for zone-offset.");
        JODA_PATTERNS_DEPRECATIONS.put("Z",
            "'Z' time zone offset/id fails when parsing 'Z' for Zulu timezone. Consider using 'X'.");
        JODA_PATTERNS_DEPRECATIONS.put("z",
            "'z' time zone text. Will print 'Z' for Zulu given UTC timezone.");
    }

    public static final String USE_PREFIX_8_WARNING = "Prefix your date format with '8' to use the new specifier.";

    public static boolean isDeprecatedFormat(String format) {
        return format.startsWith("8") == false &&
            JODA_PATTERNS_DEPRECATIONS.keySet().stream()
                                      .filter(s -> format.contains(s))
                                      .findAny()
                                      .isPresent();
    }

    public static String formatSuggestion(String format) {
        String suggestion = JODA_PATTERNS_DEPRECATIONS.entrySet().stream()
                                                      .filter(s -> format.contains(s.getKey()))
                                                      .map(s -> s.getValue())
                                                      .collect(Collectors.joining("; "));
        return suggestion;
    }
}

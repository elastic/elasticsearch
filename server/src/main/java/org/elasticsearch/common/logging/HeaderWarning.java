/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.logging;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface HeaderWarning {
    /**
     * Regular expression to test if a string matches the RFC7234 specification for warning headers. This pattern assumes that the warn code
     * is always 299. Further, this pattern assumes that the warn agent represents a version of Elasticsearch including the build hash.
     */
    Pattern WARNING_HEADER_PATTERN = Pattern.compile(
            "299 " + // warn code
                    "Elasticsearch-" + // warn agent
                    "\\d+\\.\\d+\\.\\d+(?:-(?:alpha|beta|rc)\\d+)?(?:-SNAPSHOT)?-" + // warn agent
                    "(?:[a-f0-9]{7}(?:[a-f0-9]{33})?|unknown) " + // warn agent
                    "\"((?:\t| |!|[\\x23-\\x5B]|[\\x5D-\\x7E]|[\\x80-\\xFF]|\\\\|\\\\\")*)\"( " + // quoted warning value, captured
                    // quoted RFC 1123 date format
                    "\"" + // opening quote
                    "(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), " + // weekday
                    "\\d{2} " + // 2-digit day
                    "(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) " + // month
                    "\\d{4} " + // 4-digit year
                    "\\d{2}:\\d{2}:\\d{2} " + // (two-digit hour):(two-digit minute):(two-digit second)
                    "GMT" + // GMT
                    "\")?"); // closing quote (optional, since an older version can still send a warn-date)
    Pattern WARNING_XCONTENT_LOCATION_PATTERN = Pattern.compile("^\\[.*?]\\[-?\\d+:-?\\d+] ");

    /**
     * Extracts the warning value from the value of a warning header that is formatted according to RFC 7234. That is, given a string
     * {@code 299 Elasticsearch-6.0.0 "warning value"}, the return value of this method would be {@code warning value}.
     *
     * @param s the value of a warning header formatted according to RFC 7234.
     * @return the extracted warning value
     */
    static String extractWarningValueFromWarningHeader(final String s, boolean stripXContentPosition) {
        /*
         * We know the exact format of the warning header, so to extract the warning value we can skip forward from the front to the first
         * quote and we know the last quote is at the end of the string
         *
         *   299 Elasticsearch-6.0.0 "warning value"
         *                           ^             ^
         *                           firstQuote    lastQuote
         *
         * We parse this manually rather than using the capturing regular expression because the regular expression involves a lot of
         * backtracking and carries a performance penalty. However, when assertions are enabled, we still use the regular expression to
         * verify that we are maintaining the warning header format.
         */
        final int firstQuote = s.indexOf('\"');
        final int lastQuote = s.length() - 1;
        String warningValue = s.substring(firstQuote + 1, lastQuote);
        assert assertWarningValue(s, warningValue);
        if (stripXContentPosition) {
            Matcher matcher = WARNING_XCONTENT_LOCATION_PATTERN.matcher(warningValue);
            if (matcher.find()) {
                warningValue = warningValue.substring(matcher.end());
            }
        }
        return warningValue;
    }

    /**
     * Assert that the specified string has the warning value equal to the provided warning value.
     *
     * @param s            the string representing a full warning header
     * @param warningValue the expected warning header
     * @return {@code true} if the specified string has the expected warning value
     */
    static boolean assertWarningValue(final String s, final String warningValue) {
        final Matcher matcher = WARNING_HEADER_PATTERN.matcher(s);
        final boolean matches = matcher.matches();
        assert matches;
        return matcher.group(1).equals(warningValue);
    }
}

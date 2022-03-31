/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * A formatter that allows named placeholders e.g. "%(param)" to be replaced.
 */
public class NamedFormatter {
    private static final Pattern PARAM_REGEX = Pattern.compile(
        // Match either any backlash-escaped characters, or a "%(param)" pattern.
        // COMMENTS is specified to allow whitespace in this pattern, for clarity
        "\\\\(.) | (% \\( ([^)]+) \\) )",
        Pattern.COMMENTS
    );

    private NamedFormatter() {}

    /**
     * Replaces named parameters of the form <code>%(param)</code> in format strings. For example:
     *
     * <ul>
     * <li><code>NamedFormatter.format("Hello, %(name)!", Map.of("name", "world"))</code> → <code>"Hello, world!"</code></li>
     * <li><code>NamedFormatter.format("Hello, \%(name)!", Map.of("name", "world"))</code> → <code>"Hello, %(world)!"</code></li>
     * <li><code>NamedFormatter.format("Hello, %(oops)!", Map.of("name", "world"))</code> → {@link IllegalArgumentException}</li>
     * </ul>
     *
     * @param fmt The format string. Any <code>%(param)</code> is replaced by its corresponding value in the <code>values</code> map.
     *             Parameter patterns can be escaped by prefixing with a backslash.
     * @param values a map of parameter names to values.
     * @return The formatted string.
     * @throws IllegalArgumentException if a parameter is found in the format string with no corresponding value
     */
    public static String format(String fmt, Map<String, Object> values) {
        return PARAM_REGEX.matcher(fmt).replaceAll(match -> {
            // Escaped characters are unchanged
            if (match.group(1) != null) {
                return match.group(1);
            }

            final String paramName = match.group(3);
            if (values.containsKey(paramName)) {
                return values.get(paramName).toString();
            }

            throw new IllegalArgumentException("No parameter value for %(" + paramName + ")");
        });
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml;

import java.util.regex.Pattern;

/**
 * Matches blacklist patterns.
 *
 * Currently the following syntax is supported:
 *
 * <ul>
 *  <li>Exact matches, as in <code>cat.aliases/10_basic/Empty cluster</code></li>
 *  <li>Wildcard matches within the same segment of a path , as in <code>indices.get/10_basic/*allow_no_indices*</code>. This will
 *  match <code>indices.get/10_basic/allow_no_indices</code>, <code>indices.get/10_basic/allow_no_indices_at_all</code> but not
 *  <code>indices.get/10_basic/advanced/allow_no_indices</code> (contains an additional segment)</li>
 * </ul>
 *
 * Each blacklist pattern is a suffix match on the path. Empty patterns are not allowed.
 */
final class BlacklistedPathPatternMatcher {
    private final Pattern pattern;

    /**
     * Constructs a new <code>BlacklistedPathPatternMatcher</code> instance from the provided suffix pattern.
     *
     * @param p The suffix pattern. Must be a non-empty string.
     */
    BlacklistedPathPatternMatcher(String p) {
        // guard against accidentally matching everything as an empty string lead to the pattern ".*" which matches everything
        if (p == null || p.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty blacklist patterns are not supported");
        }

        // we don't support [], {}, (), ? and . in their special meaning within a path regex, so escape them here
        String sanitizedPattern = p.replaceAll("([\\[\\]\\{\\}\\(\\)\\?\\.])", "\\\\$1");

        // very simple transformation from wildcard to a proper regex
        String finalPattern = sanitizedPattern.replaceAll("\\*", "[^/]*") // support wildcard matches (within a single path segment)
            .replaceAll("\\\\,", ",");  // restore previously escaped ',' in paths.

        // suffix match
        pattern = Pattern.compile(".*" + finalPattern);
    }

    /**
     * Checks whether the provided path matches the suffix pattern, i.e. "/foo/bar" will match the pattern "bar".
     *
     * @param path The path to match. Must not be null.
     * @return true iff this path is a suffix match.
     */
    public boolean isSuffixMatch(String path) {
        return pattern.matcher(path).matches();
    }
}

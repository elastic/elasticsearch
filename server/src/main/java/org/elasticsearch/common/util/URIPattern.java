/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.regex.Regex;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * URI Pattern matcher
 *
 * The pattern is URI in which authority, path, query and fragment can be replace with simple pattern.
 *
 * For example: foobar://*.local/some_path/*?*#* will match all uris with schema foobar in local domain
 * with any port, with path that starts some_path and with any query and fragment.
 */
public class URIPattern {
    private final URI uriPattern;

    /**
     * Constructs uri pattern
     */
    public URIPattern(String pattern) {
        try {
            uriPattern = new URI(pattern);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("cannot parse URI pattern [" + pattern + "]");
        }
    }

    /**
     * Returns true if the given uri matches the pattern
     */
    public boolean match(URI uri) {
        return matchNormalized(uri.normalize());
    }

    public static boolean match(URIPattern[] patterns, URI uri) {
        URI normalized = uri.normalize();
        for (URIPattern pattern : patterns) {
            if (pattern.matchNormalized(normalized)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchNormalized(URI uri) {
        if (uriPattern.isOpaque()) {
            // This url only has scheme, scheme-specific part and fragment
            return uri.isOpaque()
                && match(uriPattern.getScheme(), uri.getScheme())
                && match(uriPattern.getSchemeSpecificPart(), uri.getSchemeSpecificPart())
                && match(uriPattern.getFragment(), uri.getFragment());

        } else {
            return match(uriPattern.getScheme(), uri.getScheme())
                && match(uriPattern.getAuthority(), uri.getAuthority())
                && match(uriPattern.getQuery(), uri.getQuery())
                && match(uriPattern.getPath(), uri.getPath())
                && match(uriPattern.getFragment(), uri.getFragment());
        }
    }

    private static boolean match(String pattern, String value) {
        if (value == null) {
            // If the pattern is empty or matches anything - it's a match
            if (pattern == null || Regex.isMatchAllPattern(pattern)) {
                return true;
            }
        }
        return Regex.simpleMatch(pattern, value);
    }

    @Override
    public String toString() {
        return uriPattern.toString();
    }
}

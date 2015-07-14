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
     * @param pattern
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
        if(uriPattern.isOpaque()) {
            // This url only has scheme, scheme-specific part and fragment
            return uri.isOpaque() &&
                    match(uriPattern.getScheme(), uri.getScheme()) &&
                    match(uriPattern.getSchemeSpecificPart(), uri.getSchemeSpecificPart()) &&
                    match(uriPattern.getFragment(), uri.getFragment());

        } else {
            return match(uriPattern.getScheme(), uri.getScheme()) &&
                    match(uriPattern.getAuthority(), uri.getAuthority()) &&
                    match(uriPattern.getQuery(), uri.getQuery()) &&
                    match(uriPattern.getPath(), uri.getPath()) &&
                    match(uriPattern.getFragment(), uri.getFragment());
        }
    }

    private boolean match(String pattern, String value) {
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

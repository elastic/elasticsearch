/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.web;

import org.elasticsearch.core.SuppressForbidden;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class UriParts {

    private static final String DOMAIN = "domain";
    private static final String FRAGMENT = "fragment";
    private static final String PATH = "path";
    private static final String PORT = "port";
    private static final String QUERY = "query";
    private static final String SCHEME = "scheme";
    private static final String USER_INFO = "user_info";
    private static final String EXTENSION = "extension";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private static final Map<String, Class<?>> URI_PARTS_TYPES = Map.ofEntries(
        Map.entry(DOMAIN, String.class),
        Map.entry(FRAGMENT, String.class),
        Map.entry(PATH, String.class),
        Map.entry(PORT, Integer.class),
        Map.entry(QUERY, String.class),
        Map.entry(SCHEME, String.class),
        Map.entry(USER_INFO, String.class),
        Map.entry(EXTENSION, String.class),
        Map.entry(USERNAME, String.class),
        Map.entry(PASSWORD, String.class)
    );

    public static Map<String, Class<?>> getUriPartsTypes() {
        return URI_PARTS_TYPES;
    }

    public static Map<String, Object> parse(String uriString) {
        URI uri = null;
        URL url = null;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            try {
                url = new URL(uriString);
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + uriString + "]");
            }
        }
        return getUriParts(uri, url);
    }

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    private static Map<String, Object> getUriParts(URI uri, URL fallbackUrl) {
        var uriParts = new HashMap<String, Object>();
        String domain;
        String fragment;
        String path;
        int port;
        String query;
        String scheme;
        String userInfo;

        if (uri != null) {
            domain = uri.getHost();
            fragment = uri.getFragment();
            path = uri.getPath();
            port = uri.getPort();
            query = uri.getQuery();
            scheme = uri.getScheme();
            userInfo = uri.getUserInfo();
        } else if (fallbackUrl != null) {
            domain = fallbackUrl.getHost();
            fragment = fallbackUrl.getRef();
            path = fallbackUrl.getPath();
            port = fallbackUrl.getPort();
            query = fallbackUrl.getQuery();
            scheme = fallbackUrl.getProtocol();
            userInfo = fallbackUrl.getUserInfo();
        } else {
            // should never occur during processor execution
            throw new IllegalArgumentException("at least one argument must be non-null");
        }

        uriParts.put(DOMAIN, domain);
        if (fragment != null) {
            uriParts.put(FRAGMENT, fragment);
        }
        if (path != null) {
            uriParts.put(PATH, path);
            // To avoid any issues with extracting the extension from a path that contains a dot, we explicitly extract the extension
            // from the last segment in the path.
            var lastSegmentIndex = path.lastIndexOf('/');
            if (lastSegmentIndex >= 0) {
                var lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = lastSegment.lastIndexOf('.');
                if (periodIndex >= 0) {
                    // Don't include the dot in the extension field.
                    uriParts.put(EXTENSION, lastSegment.substring(periodIndex + 1));
                }
            }
        }
        if (port != -1) {
            uriParts.put(PORT, port);
        }
        if (query != null) {
            uriParts.put(QUERY, query);
        }
        uriParts.put(SCHEME, scheme);
        if (userInfo != null) {
            uriParts.put(USER_INFO, userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(':');
                uriParts.put(USERNAME, userInfo.substring(0, colonIndex));
                uriParts.put(PASSWORD, colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        return uriParts;
    }
}

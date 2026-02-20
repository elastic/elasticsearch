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
import java.util.LinkedHashMap;
import java.util.Map;

public class UriParts {

    public static final String DOMAIN = "domain";
    public static final String FRAGMENT = "fragment";
    public static final String PATH = "path";
    public static final String EXTENSION = "extension";
    public static final String PORT = "port";
    public static final String QUERY = "query";
    public static final String SCHEME = "scheme";
    public static final String USER_INFO = "user_info";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    private static final LinkedHashMap<String, Class<?>> URI_PARTS_TYPES;

    static {
        URI_PARTS_TYPES = new LinkedHashMap<>();
        URI_PARTS_TYPES.putLast(DOMAIN, String.class);
        URI_PARTS_TYPES.putLast(FRAGMENT, String.class);
        URI_PARTS_TYPES.putLast(PATH, String.class);
        URI_PARTS_TYPES.putLast(EXTENSION, String.class);
        URI_PARTS_TYPES.putLast(PORT, Integer.class);
        URI_PARTS_TYPES.putLast(QUERY, String.class);
        URI_PARTS_TYPES.putLast(SCHEME, String.class);
        URI_PARTS_TYPES.putLast(USER_INFO, String.class);
        URI_PARTS_TYPES.putLast(USERNAME, String.class);
        URI_PARTS_TYPES.putLast(PASSWORD, String.class);
    }

    public static LinkedHashMap<String, Class<?>> getUriPartsTypes() {
        return URI_PARTS_TYPES;
    }

    public static Map<String, Object> parse(String uriString) {
        final var uriParts = new UriPartsMapCollector();
        parse(uriString, uriParts);
        return uriParts;
    }

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    public static void parse(final String uriString, final UriPartsCollector uriPartsCollector) {
        URI uri = null;
        URL fallbackUrl = null;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e) {
            try {
                // noinspection deprecation
                fallbackUrl = new URL(uriString);
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + uriString + "]");
            }
        }

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

        uriPartsCollector.domain(domain);
        if (fragment != null) {
            uriPartsCollector.fragment(fragment);
        }
        if (path != null) {
            uriPartsCollector.path(path);
            // To avoid any issues with extracting the extension from a path that contains a dot, we explicitly extract the extension
            // from the last segment in the path.
            var lastSegmentIndex = path.lastIndexOf('/');
            if (lastSegmentIndex >= 0) {
                var lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = lastSegment.lastIndexOf('.');
                if (periodIndex >= 0) {
                    // Don't include the dot in the extension field.
                    uriPartsCollector.extension(lastSegment.substring(periodIndex + 1));
                }
            }
        }
        if (port != -1) {
            uriPartsCollector.port(port);
        }
        if (query != null) {
            uriPartsCollector.query(query);
        }
        uriPartsCollector.scheme(scheme);
        if (userInfo != null) {
            uriPartsCollector.userInfo(userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(':');
                uriPartsCollector.username(userInfo.substring(0, colonIndex));
                uriPartsCollector.password(colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }
    }

    /**
     * A dedicated collector for URI parts. Implementation can be specific to the use case, for example, it can avoid map instance
     * allocation and primitive value boxing by writing directly to the collecting data structure.
     */
    public interface UriPartsCollector {
        void domain(String domain);

        void fragment(String fragment);

        void path(String path);

        void extension(String extension);

        void port(int port);

        void query(String query);

        void scheme(String scheme);

        void userInfo(String userInfo);

        void username(String username);

        void password(String password);
    }

    /**
     * A default implementation of {@link UriPartsCollector} that writes to a {@link Map}.
     */
    public static final class UriPartsMapCollector extends HashMap<String, Object> implements UriPartsCollector {
        @Override
        public void domain(String domain) {
            if (domain != null) {
                put(DOMAIN, domain);
            }
        }

        @Override
        public void fragment(String fragment) {
            if (fragment != null) {
                put(FRAGMENT, fragment);
            }
        }

        @Override
        public void path(String path) {
            if (path != null) {
                put(PATH, path);
            }
        }

        @Override
        public void extension(String extension) {
            if (extension != null) {
                put(EXTENSION, extension);
            }
        }

        @Override
        public void port(int port) {
            if (port >= 0) {
                put(PORT, port);
            }
        }

        @Override
        public void query(String query) {
            if (query != null) {
                put(QUERY, query);
            }
        }

        @Override
        public void scheme(String scheme) {
            if (scheme != null) {
                put(SCHEME, scheme);
            }
        }

        @Override
        public void userInfo(String userInfo) {
            if (userInfo != null) {
                put(USER_INFO, userInfo);
            }
        }

        @Override
        public void username(String username) {
            if (username != null) {
                put(USERNAME, username);
            }
        }

        @Override
        public void password(String password) {
            if (password != null) {
                put(PASSWORD, password);
            }
        }
    }
}

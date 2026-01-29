/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.core.SuppressForbidden;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_INT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.intValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * A bridge for the function that extracts parts from a URI string.
 * The extracted parts are:
 * <ul>
 *     <li>domain</li>
 *     <li>fragment</li>
 *     <li>path</li>
 *     <li>extension</li>
 *     <li>port</li>
 *     <li>query</li>
 *     <li>scheme</li>
 *     <li>user_info</li>
 *     <li>username</li>
 *     <li>password</li>
 * </ul>
 */
public final class UriPartsFunctionBridge {

    public static LinkedHashMap<String, Class<?>> getAllOutputFields() {
        return uriPartsOutputFields();
    }

    public static final class UriPartsCollectorImpl extends CompoundOutputEvaluator.OutputFieldsCollector implements UriPartsCollector {
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> fragment;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> path;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> extension;
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> port;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> query;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> scheme;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> userInfo;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> username;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> password;

        public UriPartsCollectorImpl(SequencedCollection<String> outputFields) {
            super(outputFields.size());

            BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> fragment = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> path = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> extension = NOOP_STRING_COLLECTOR;
            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> port = NOOP_INT_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> query = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> scheme = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> userInfo = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> username = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> password = NOOP_STRING_COLLECTOR;

            int index = 0;
            for (String outputField : outputFields) {
                switch (outputField) {
                    case DOMAIN:
                        domain = stringValueCollector(index);
                        break;
                    case FRAGMENT:
                        fragment = stringValueCollector(index);
                        break;
                    case PATH:
                        path = stringValueCollector(index);
                        break;
                    case EXTENSION:
                        extension = stringValueCollector(index);
                        break;
                    case PORT:
                        port = intValueCollector(index, value -> value >= 0);
                        break;
                    case QUERY:
                        query = stringValueCollector(index);
                        break;
                    case SCHEME:
                        scheme = stringValueCollector(index);
                        break;
                    case USER_INFO:
                        userInfo = stringValueCollector(index);
                        break;
                    case USERNAME:
                        username = stringValueCollector(index);
                        break;
                    case PASSWORD:
                        password = stringValueCollector(index);
                        break;
                    default:
                        // we may be asked to collect an unknow field, which we only need to ignore and the corresponding block will be
                        // filled with nulls
                }
                index++;
            }

            this.domain = domain;
            this.fragment = fragment;
            this.path = path;
            this.extension = extension;
            this.port = port;
            this.query = query;
            this.scheme = scheme;
            this.userInfo = userInfo;
            this.username = username;
            this.password = password;
        }

        @Override
        public void domain(String domain) {
            this.domain.accept(rowOutput, domain);
        }

        @Override
        public void fragment(String fragment) {
            this.fragment.accept(rowOutput, fragment);
        }

        @Override
        public void path(String path) {
            this.path.accept(rowOutput, path);
        }

        @Override
        public void extension(String extension) {
            this.extension.accept(rowOutput, extension);
        }

        @Override
        public void port(int port) {
            this.port.accept(rowOutput, port);
        }

        @Override
        public void query(String query) {
            this.query.accept(rowOutput, query);
        }

        @Override
        public void scheme(String scheme) {
            this.scheme.accept(rowOutput, scheme);
        }

        @Override
        public void userInfo(String userInfo) {
            this.userInfo.accept(rowOutput, userInfo);
        }

        @Override
        public void username(String username) {
            this.username.accept(rowOutput, username);
        }

        @Override
        public void password(String password) {
            this.password.accept(rowOutput, password);
        }

        @Override
        public void evaluate(String input) {
            getUriParts(input, this);
        }
    }

    // ==================================================================================
    // Logic should be moved to a common library
    // ==================================================================================

    static final String DOMAIN = "domain";
    static final String FRAGMENT = "fragment";
    static final String PATH = "path";
    static final String EXTENSION = "extension";
    static final String PORT = "port";
    static final String QUERY = "query";
    static final String SCHEME = "scheme";
    static final String USER_INFO = "user_info";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";

    private static Map<String, Object> getUriParts(String urlString) {
        var uriParts = new HashMap<String, Object>();
        getUriParts(urlString, new UriPartsMapCollector(uriParts));
        return uriParts;
    }

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    private static void getUriParts(String urlString, UriPartsCollector uriPartsCollector) {
        URI uri = null;
        URL fallbackUrl = null;
        try {
            uri = new URI(urlString);
        } catch (URISyntaxException e) {
            try {
                // noinspection deprecation
                fallbackUrl = new URL(urlString);
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + urlString + "]");
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

    private static LinkedHashMap<String, Class<?>> uriPartsOutputFields() {
        LinkedHashMap<String, Class<?>> outputColumns = new LinkedHashMap<>();
        outputColumns.putLast(DOMAIN, String.class);
        outputColumns.putLast(FRAGMENT, String.class);
        outputColumns.putLast(PATH, String.class);
        outputColumns.putLast(EXTENSION, String.class);
        outputColumns.putLast(PORT, Integer.class);
        outputColumns.putLast(QUERY, String.class);
        outputColumns.putLast(SCHEME, String.class);
        outputColumns.putLast(USER_INFO, String.class);
        outputColumns.putLast(USERNAME, String.class);
        outputColumns.putLast(PASSWORD, String.class);
        return outputColumns;
    }

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

    public static class UriPartsMapCollector implements UriPartsCollector {
        private final Map<String, Object> uriParts;

        public UriPartsMapCollector(Map<String, Object> uriParts) {
            this.uriParts = uriParts;
        }

        @Override
        public void domain(String domain) {
            if (domain != null) {
                uriParts.put(DOMAIN, domain);
            }
        }

        @Override
        public void fragment(String fragment) {
            if (fragment != null) {
                uriParts.put(FRAGMENT, fragment);
            }
        }

        @Override
        public void path(String path) {
            if (path != null) {
                uriParts.put(PATH, path);
            }
        }

        @Override
        public void extension(String extension) {
            if (extension != null) {
                uriParts.put(EXTENSION, extension);
            }
        }

        @Override
        public void port(int port) {
            if (port >= 0) {
                uriParts.put(PORT, port);
            }
        }

        @Override
        public void query(String query) {
            if (query != null) {
                uriParts.put(QUERY, query);
            }
        }

        @Override
        public void scheme(String scheme) {
            if (scheme != null) {
                uriParts.put(SCHEME, scheme);
            }
        }

        @Override
        public void userInfo(String userInfo) {
            if (userInfo != null) {
                uriParts.put(USER_INFO, userInfo);
            }
        }

        @Override
        public void username(String username) {
            if (username != null) {
                uriParts.put(USERNAME, username);
            }
        }

        @Override
        public void password(String password) {
            if (password != null) {
                uriParts.put(PASSWORD, password);
            }
        }
    }
}

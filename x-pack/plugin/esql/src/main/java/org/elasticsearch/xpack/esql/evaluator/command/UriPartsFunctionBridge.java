/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.core.type.DataType;

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
public class UriPartsFunctionBridge extends CompoundOutputEvaluator<UriPartsFunctionBridge.UriPartsCollectorImpl> {

    public UriPartsFunctionBridge(DataType inputType, Warnings warnings, UriPartsCollectorImpl uriPartsCollector) {
        super(inputType, warnings, uriPartsCollector);
    }

    public static LinkedHashMap<String, Class<?>> getAllOutputFields() {
        return uriPartsOutputFields();
    }

    public static final class UriPartsCollectorImpl extends OutputFieldsCollector implements UriPartsCollector {
        private final BiConsumer<Block.Builder[], String> domain;
        private final BiConsumer<Block.Builder[], String> fragment;
        private final BiConsumer<Block.Builder[], String> path;
        private final BiConsumer<Block.Builder[], String> extension;
        private final ObjIntConsumer<Block.Builder[]> port;
        private final BiConsumer<Block.Builder[], String> query;
        private final BiConsumer<Block.Builder[], String> scheme;
        private final BiConsumer<Block.Builder[], String> userInfo;
        private final BiConsumer<Block.Builder[], String> username;
        private final BiConsumer<Block.Builder[], String> password;

        public UriPartsCollectorImpl(SequencedCollection<String> outputFields, BlocksBearer blocksBearer) {
            super(blocksBearer);

            BiConsumer<Block.Builder[], String> domain = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> fragment = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> path = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> extension = NOOP_STRING_COLLECTOR;
            ObjIntConsumer<Block.Builder[]> port = NOOP_INT_COLLECTOR;
            BiConsumer<Block.Builder[], String> query = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> scheme = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> userInfo = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> username = NOOP_STRING_COLLECTOR;
            BiConsumer<Block.Builder[], String> password = NOOP_STRING_COLLECTOR;

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
                        unknownFieldCollectors.add(nullValueCollector(index));
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
            this.domain.accept(blocksBearer.get(), domain);
        }

        @Override
        public void fragment(String fragment) {
            this.fragment.accept(blocksBearer.get(), fragment);
        }

        @Override
        public void path(String path) {
            this.path.accept(blocksBearer.get(), path);
        }

        @Override
        public void extension(String extension) {
            this.extension.accept(blocksBearer.get(), extension);
        }

        @Override
        public void port(int port) {
            this.port.accept(blocksBearer.get(), port);
        }

        @Override
        public void query(String query) {
            this.query.accept(blocksBearer.get(), query);
        }

        @Override
        public void scheme(String scheme) {
            this.scheme.accept(blocksBearer.get(), scheme);
        }

        @Override
        public void userInfo(String userInfo) {
            this.userInfo.accept(blocksBearer.get(), userInfo);
        }

        @Override
        public void username(String username) {
            this.username.accept(blocksBearer.get(), username);
        }

        @Override
        public void password(String password) {
            this.password.accept(blocksBearer.get(), password);
        }

        @Override
        public boolean evaluate(String input) throws Exception {
            return getUriParts(input, this);
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

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    private static Map<String, Object> getUriParts(String urlString) {
        var uriParts = new HashMap<String, Object>();
        getUriParts(urlString, new UriPartsMapCollector(uriParts));
        return uriParts;
    }

    private static boolean getUriParts(String urlString, UriPartsCollector uriPartsMapCollector) {
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
        String extension = null;
        int port;
        String query;
        String scheme;
        String userInfo;
        String username = null;
        String password = null;

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

        if (path != null) {
            // To avoid any issues with extracting the extension from a path that contains a dot, we explicitly extract the extension
            // from the last segment in the path.
            var lastSegmentIndex = path.lastIndexOf('/');
            if (lastSegmentIndex >= 0) {
                var lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = lastSegment.lastIndexOf('.');
                if (periodIndex >= 0) {
                    // Don't include the dot in the extension field.
                    extension = lastSegment.substring(periodIndex + 1);
                }
            }
        }

        if (userInfo != null) {
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(':');
                username = userInfo.substring(0, colonIndex);
                password = colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "";
            }
        }

        uriPartsMapCollector.domain(domain);
        uriPartsMapCollector.fragment(fragment);
        uriPartsMapCollector.path(path);
        uriPartsMapCollector.extension(extension);
        uriPartsMapCollector.port(port);
        uriPartsMapCollector.query(query);
        uriPartsMapCollector.scheme(scheme);
        uriPartsMapCollector.userInfo(userInfo);
        uriPartsMapCollector.username(username);
        uriPartsMapCollector.password(password);
        return true;
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

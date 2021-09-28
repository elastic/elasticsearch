/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap.plugins;

import org.elasticsearch.cli.SuppressForbidden;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Utilities for working with HTTP proxies.
 */
class ProxyUtils {
    /**
     * Constructs a proxy from the given string. If {@code null} is passed, then {@code null} will
     * be returned, since that is not the same as {@link Proxy#NO_PROXY}.
     *
     * @param proxy the string to use, in the form "host:port"
     * @return a proxy or null
     */
    @SuppressForbidden(reason = "Proxy constructor requires a SocketAddress")
    static Proxy buildProxy(String proxy) throws PluginSyncException {
        if (proxy == null) {
            return null;
        }

        final String[] parts = proxy.split(":");
        if (parts.length != 2) {
            throw new PluginSyncException("Malformed [proxy], expected [host:port]");
        }

        if (validateProxy(parts[0], parts[1]) == false) {
            throw new PluginSyncException("Malformed [proxy], expected [host:port]");
        }

        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(parts[0], Integer.parseUnsignedInt(parts[1])));
    }

    private static final Predicate<String> HOST_PATTERN = Pattern.compile(
        "^ (?!-)[a-z0-9-]+ (?: \\. (?!-)[a-z0-9-]+ )* $",
        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS
    ).asMatchPredicate();

    static boolean validateProxy(String hostname, String port) {
        return hostname != null && port != null && HOST_PATTERN.test(hostname) && port.matches("^\\d+$") != false;
    }
}

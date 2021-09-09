/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cli.UserException;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Utilities for working with HTTP proxies.
 */
public class ProxyUtils {
    /**
     * Constructs a proxy from the given string. If {@code null} is passed, then either a proxy will
     * be returned using the system proxy settings, or {@link Proxy#NO_PROXY} will be returned.
     *
     * @param proxy the string to use, in the form "host:port"
     * @return a proxy
     */
    @SuppressForbidden(reason = "Proxy constructor requires a SocketAddress")
    static Proxy buildProxy(String proxy) throws UserException {
        if (proxy == null) {
            return getSystemProxy();
        }

        final String[] parts = proxy.split(":");
        if (parts.length != 2) {
            throw new UserException(ExitCodes.CONFIG, "Malformed [proxy], expected [host:port]");
        }

        if (validateProxy(parts[0], parts[1]) == false) {
            throw new UserException(ExitCodes.CONFIG, "Malformed [proxy], expected [host:port]");
        }

        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(parts[0], Integer.parseUnsignedInt(parts[1])));
    }

    @SuppressForbidden(reason = "Proxy constructor requires a SocketAddress")
    private static Proxy getSystemProxy() {
        String proxyHost = System.getProperty("https.proxyHost");
        String proxyPort = Objects.requireNonNullElse(System.getProperty("https.proxyPort"), "443");
        if (validateProxy(proxyHost, proxyPort)) {
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        proxyHost = System.getProperty("http.proxyHost");
        proxyPort = Objects.requireNonNullElse(System.getProperty("http.proxyPort"), "80");
        if (validateProxy(proxyHost, proxyPort)) {
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        proxyHost = System.getProperty("socks.proxyHost");
        proxyPort = Objects.requireNonNullElse(System.getProperty("socks.proxyPort"), "1080");
        if (validateProxy(proxyHost, proxyPort)) {
            return new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        return Proxy.NO_PROXY;
    }

    private static final Predicate<String> HOST_PATTERN = Pattern.compile(
        "^ (?!-)[a-z0-9-]+ (?: \\. (?!-)[a-z0-9-]+ )* $",
        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS
    ).asMatchPredicate();

    static boolean validateProxy(String hostname, String port) {
        return hostname != null && port != null && HOST_PATTERN.test(hostname) && port.matches("^\\d+$") != false;
    }
}

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
import org.elasticsearch.common.Strings;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.file.Path;

/**
 * Utilities for working with HTTP proxies.
 */
public class ProxyUtils {

    /**
     * Checks that the supplied string can be used to configure a proxy.
     *
     * @param proxy the URI string to use
     * @param pluginId the ID of the plugin, or null for a global proxy, for constructing error messages
     * @param manifestPath the path to the config, for constructing error messages
     * @throws UserException when passed an invalid URI
     */
    static void validateProxy(String proxy, String pluginId, Path manifestPath) throws UserException {
        String pluginDescription = pluginId == null ? "" : " for plugin [" + pluginId + "]";
        String message = "Malformed [proxy]" + pluginDescription + ", expected [host:port] in " + manifestPath;

        try {
            String proxyUrl;
            if (proxy.matches("^(?:https?|socks[45]?)://.*")) {
                proxyUrl = proxy;
            } else {
                String[] parts = proxy.split(":");
                if (parts.length != 2) {
                    throw new UserException(ExitCodes.CONFIG, message);
                }
                proxyUrl = "http://" + proxy;
            }
            URL url = new URL(proxyUrl);
            if (url.getHost().isBlank()) {
                throw new UserException(ExitCodes.CONFIG, message);
            }
            if (url.getPort() == -1) {
                throw new UserException(ExitCodes.CONFIG, message);
            }
        } catch (MalformedURLException e) {
            throw new UserException(ExitCodes.CONFIG, message);
        }
    }

    /**
     * Constructs a proxy from the given string. Assumes that the string has already been validated using
     * {@link #validateProxy(String, String, Path)}. If {@code null} is passed, then either a proxy will
     * be returned using the system proxy settings, or {@link Proxy#NO_PROXY} will be returned.
     *
     * @param proxy the string to use, which must either be a well-formed HTTP or SOCKS URL, or have the form "host:port"
     * @return a proxy
     */
    @SuppressForbidden(reason = "Proxy constructor uses InetSocketAddress")
    static Proxy buildProxy(String proxy) throws UserException {
        if (proxy == null) {
            return getSystemProxy();
        }

        final String proxyUrl = proxy.matches("^(?:https?|socks[45]?)://.*") ? proxy : "http://" + proxy;

        try {
            URL url = new URL(proxyUrl);
            return new Proxy(
                url.getProtocol().startsWith("socks") ? Proxy.Type.SOCKS : Proxy.Type.HTTP,
                new InetSocketAddress(url.getHost(), url.getPort())
            );
        } catch (MalformedURLException e) {
            throw new UserException(ExitCodes.CONFIG, "Malformed proxy value : [" + proxy + "]");
        }
    }

    @SuppressForbidden(reason = "Proxy constructor uses InetSocketAddress")
    private static Proxy getSystemProxy() {
        String proxyHost = System.getProperty("http.proxyHost");
        String proxyPort = System.getProperty("http.proxyPort");
        if (Strings.isNullOrEmpty(proxyHost) == false && Strings.isNullOrEmpty(proxyPort) == false) {
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        proxyHost = System.getProperty("socks.proxyHost");
        proxyPort = System.getProperty("socks.proxyPort");
        if (Strings.isNullOrEmpty(proxyHost) == false && Strings.isNullOrEmpty(proxyPort) == false) {
            return new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }

        return Proxy.NO_PROXY;
    }
}

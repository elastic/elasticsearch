/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
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
        String pluginDescription = pluginId == null ? "" : "for plugin [" + pluginId + "] ";
        try {
            URI uri = new URI(proxy);
            if (uri.getHost().isBlank()) {
                throw new UserException(ExitCodes.CONFIG, "Malformed host " + pluginDescription + "in [proxy] value in: " + manifestPath);
            }
            if (uri.getPort() == -1) {
                throw new UserException(
                    ExitCodes.CONFIG,
                    "Malformed or missing port " + pluginDescription + "in [proxy] value in: " + manifestPath
                );
            }
        } catch (URISyntaxException e) {
            throw new UserException(ExitCodes.CONFIG, "Malformed [proxy] value " + pluginDescription + "in: " + manifestPath);
        }
    }

    /**
     * Constructs an HTTP proxy from the given URI string. Assumes that the string has already been validated using
     * {@link #validateProxy(String, String, Path)}.
     *
     * @param proxy the string to use
     * @return a proxy
     */
    static Proxy buildProxy(String proxy) throws UserException {
        if (proxy == null) {
            return Proxy.NO_PROXY;
        }

        try {
            URI uri = new URI(proxy);
            return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(uri.getHost(), uri.getPort()));
        } catch (URISyntaxException e) {
            throw new UserException(ExitCodes.CONFIG, "Malformed proxy value : [" + proxy + "]");
        }
    }
}

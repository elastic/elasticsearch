/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.net.InetSocketAddress;
import java.net.Proxy;

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
    static Proxy buildProxy(String proxy) throws UserException {
        if (proxy == null) {
            return null;
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

    /**
     * Check that the hostname is not empty, and that the port is numeric.
     *
     * @param hostname the hostname to check. Besides ensuring it is not null or empty, no further validation is
     *                 performed.
     * @param port the port to check. Must be composed solely of digits.
     * @return whether the arguments describe a potentially valid proxy.
     */
    static boolean validateProxy(String hostname, String port) {
        return Strings.isNullOrEmpty(hostname) == false && port != null && port.matches("^\\d+$");
    }
}

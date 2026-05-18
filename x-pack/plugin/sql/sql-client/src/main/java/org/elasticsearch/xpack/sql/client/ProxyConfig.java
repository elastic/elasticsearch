/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.client;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

class ProxyConfig {

    private static final String HTTP_PROXY = "proxy.http";
    private static final String HTTP_PROXY_DEFAULT = StringUtils.EMPTY;
    private static final String SOCKS_PROXY = "proxy.socks";
    private static final String SOCKS_PROXY_DEFAULT = StringUtils.EMPTY;

    static final Set<String> OPTION_NAMES = new LinkedHashSet<>(Arrays.asList(HTTP_PROXY, SOCKS_PROXY));

    private final Proxy proxy;

    ProxyConfig(Properties settings) {
        Proxy.Type type = null;
        // try http first
        Object[] address = host(settings.getProperty(HTTP_PROXY, HTTP_PROXY_DEFAULT), 80);
        type = Proxy.Type.HTTP;
        // nope, check socks
        if (address == null) {
            address = host(settings.getProperty(SOCKS_PROXY, SOCKS_PROXY_DEFAULT), 1080);
            type = Proxy.Type.SOCKS;
        }
        proxy = address != null ? createProxy(type, address) : null;
    }

    @SuppressForbidden(reason = "create the actual proxy")
    private static Proxy createProxy(Proxy.Type type, Object[] address) {
        return new Proxy(type, new InetSocketAddress((String) address[0], (int) address[1]));
    }

    boolean enabled() {
        return proxy != null;
    }

    Proxy proxy() {
        return proxy;
    }

    // returns hostname (string), port (int)
    private static Object[] host(String address, int defaultPort) {
        if (StringUtils.hasText(address) == false) {
            return null;
        }
        try {
            URI uri = new URI(address);
            Object[] results = { uri.getHost(), uri.getPort() > 0 ? uri.getPort() : defaultPort };
            return results;
        } catch (URISyntaxException ex) {
            throw new ClientException("Unrecognized address format " + address, ex);
        }
    }
}

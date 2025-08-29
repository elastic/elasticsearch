/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import javax.net.ssl.SSLEngine;

public record TLSConfig(EngineProvider engineProvider) {

    public boolean isTLSEnabled() {
        return engineProvider != null;
    }

    public SSLEngine createServerSSLEngine() {
        assert isTLSEnabled();
        SSLEngine sslEngine = engineProvider.create(null, -1);
        sslEngine.setUseClientMode(false);
        return sslEngine;
    }

    public static TLSConfig noTLS() {
        return new TLSConfig(null);
    }

    @FunctionalInterface
    public interface EngineProvider {

        SSLEngine create(String host, int port);
    }
}

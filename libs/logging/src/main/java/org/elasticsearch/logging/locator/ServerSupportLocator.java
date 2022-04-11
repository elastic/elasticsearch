/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.locator;

import org.elasticsearch.logging.spi.ServerSupport;

import java.util.ServiceLoader;

public final class ServerSupportLocator {

    private ServerSupportLocator() {}

    public static final ServerSupport INSTANCE = loadProvider();

    static ServerSupport loadProvider() {
        ServiceLoader<ServerSupport> sl = ServiceLoader.load(ServerSupport.class, ClassLoader.getSystemClassLoader());
        return sl.findFirst().orElseThrow();
    }
}

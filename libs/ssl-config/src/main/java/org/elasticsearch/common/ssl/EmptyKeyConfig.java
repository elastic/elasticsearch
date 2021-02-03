/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

/**
 * A {@link SslKeyConfig} that does nothing (provides a null key manager)
 */
final class EmptyKeyConfig implements SslKeyConfig {

    static final EmptyKeyConfig INSTANCE = new EmptyKeyConfig();

    private EmptyKeyConfig() {
        // Enforce a single instance
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.emptyList();
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        return null;
    }

    @Override
    public String toString() {
        return "empty-key-config";
    }
}

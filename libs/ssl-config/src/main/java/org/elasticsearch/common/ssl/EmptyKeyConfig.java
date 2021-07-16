/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Tuple;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.X509ExtendedKeyManager;

/**
 * A {@link SslKeyConfig} that does nothing (provides a null key manager)
 */
public final class EmptyKeyConfig implements SslKeyConfig {

    public static final EmptyKeyConfig INSTANCE = new EmptyKeyConfig();

    private EmptyKeyConfig() {
        // Enforce a single instance
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of();
    }

    @Override
    public List<Tuple<PrivateKey, X509Certificate>> getKeys() {
        return List.of();
    }

    @Override
    public Collection<StoredCertificate> getConfiguredCertificates() {
        return List.of();
    }

    @Override
    public boolean hasKeyMaterial() {
        return false;
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

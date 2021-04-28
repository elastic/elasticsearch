/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * A {@link SslKeyConfig} that builds a Key Manager from a keystore file.
 */
public class Pkcs11KeyConfig extends SslKeystoreConfig {

    public Pkcs11KeyConfig(char[] storePassword, char[] keyPassword, String algorithm, Path configBasePath) {
        super(storePassword, keyPassword, algorithm, configBasePath);
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        return null;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of();
    }

    @Override
    public String getKeystorePath() {
        return null;
    }

    @Override
    public String getKeystoreType() {
        return "PKCS11";
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pkcs11KeyConfig{");
        sb.append(", storePassword=").append(getKeystorePassword().length == 0 ? "<empty>" : "<non-empty>");
        sb.append(", keyPassword=").append(hasKeyPassword() ? "<set>" : "<not-set>");
        sb.append(", algorithm=").append(getKeystoreAlgorithm());
        sb.append('}');
        return sb.toString();
    }
}

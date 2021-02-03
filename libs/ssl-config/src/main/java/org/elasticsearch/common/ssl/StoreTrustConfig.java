/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;

/**
 * A {@link SslTrustConfig} that builds a Trust Manager from a keystore file.
 */
final class StoreTrustConfig implements SslTrustConfig {
    private final Path path;
    private final char[] password;
    private final String type;
    private final String algorithm;

    /**
     * @param path      The path to the keystore file
     * @param password  The password for the keystore
     * @param type      The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
     *                  See {@link KeyStoreUtil#inferKeyStoreType(Path)}.
     * @param algorithm The algorithm to use for the Trust Manager (see {@link javax.net.ssl.TrustManagerFactory#getAlgorithm()}).
     */
    StoreTrustConfig(Path path, char[] password, String type, String algorithm) {
        this.path = path;
        this.type = type;
        this.algorithm = algorithm;
        this.password = password;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.singleton(path);
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        try {
            final KeyStore store = KeyStoreUtil.readKeyStore(path, type, password);
            checkTrustStore(store);
            return KeyStoreUtil.createTrustManager(store, algorithm);
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot create trust manager for path=[" + (path == null ? null : path.toAbsolutePath())
                + "] type=[" + type + "] password=[" + (password.length == 0 ? "<empty>" : "<non-empty>") + "]", e);
        }
    }

    /**
     * Verifies that the keystore contains at least 1 trusted certificate entry.
     */
    private void checkTrustStore(KeyStore store) throws GeneralSecurityException {
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (store.isCertificateEntry(alias)) {
                return;
            }
        }
        final String message;
        if (path != null) {
            message = "the truststore [" + path + "] does not contain any trusted certificate entries";
        } else {
            message = "the configured PKCS#11 token does not contain any trusted certificate entries";
        }
        throw new SslConfigException(message);
    }

}

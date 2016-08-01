/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

abstract class KeyConfig extends TrustConfig {

    private X509ExtendedKeyManager[] keyManagers = null;

    KeyConfig(boolean includeSystem) {
        super(includeSystem);
    }

    static final KeyConfig NONE = new KeyConfig(false) {
        @Override
        X509ExtendedKeyManager loadKeyManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        X509ExtendedTrustManager nonSystemTrustManager(@Nullable Environment environment) {
            return null;
        }

        @Override
        void validate() {
        }

        @Override
        List<Path> filesToMonitor(@Nullable Environment environment) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "NONE";
        }
    };

    final synchronized X509ExtendedKeyManager[] keyManagers(@Nullable Environment environment) {
        if (keyManagers == null) {
            X509ExtendedKeyManager keyManager = loadKeyManager(environment);
            setKeyManagers(keyManager);
        }
        return keyManagers;
    }

    @Override
    synchronized void reload(@Nullable Environment environment) {
        if (trustManagers == null) {
            // trust managers were never initialized... do it lazily!
            X509ExtendedKeyManager loadedKeyManager = loadKeyManager(environment);
            setKeyManagers(loadedKeyManager);
            return;
        }

        X509ExtendedTrustManager loadedTrustManager = loadAndMergeIfNecessary(environment);
        X509ExtendedKeyManager loadedKeyManager = loadKeyManager(environment);
        setTrustManagers(loadedTrustManager);
        setKeyManagers(loadedKeyManager);
    }

    final synchronized void setKeyManagers(X509ExtendedKeyManager loadedKeyManager) {
        if (loadedKeyManager == null) {
            this.keyManagers = new X509ExtendedKeyManager[0];
        } else if (this.keyManagers == null || this.keyManagers.length == 0) {
            this.keyManagers = new X509ExtendedKeyManager[] { new ReloadableX509KeyManager(loadedKeyManager) };
        } else {
            assert this.keyManagers[0] instanceof ReloadableX509KeyManager;
            ((ReloadableX509KeyManager)this.keyManagers[0]).setKeyManager(loadedKeyManager);
        }
    }

    abstract X509ExtendedKeyManager loadKeyManager(@Nullable Environment environment);

    final class ReloadableX509KeyManager extends X509ExtendedKeyManager {

        private volatile X509ExtendedKeyManager keyManager;

        ReloadableX509KeyManager(X509ExtendedKeyManager keyManager) {
            this.keyManager = keyManager;
        }

        @Override
        public String[] getClientAliases(String s, Principal[] principals) {
            return keyManager.getClientAliases(s, principals);
        }

        @Override
        public String chooseClientAlias(String[] strings, Principal[] principals, Socket socket) {
            return keyManager.chooseClientAlias(strings, principals, socket);
        }

        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return keyManager.getServerAliases(s, principals);
        }

        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return keyManager.chooseServerAlias(s, principals, socket);
        }

        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return keyManager.getCertificateChain(s);
        }

        @Override
        public PrivateKey getPrivateKey(String s) {
            return keyManager.getPrivateKey(s);
        }

        @Override
        public String chooseEngineClientAlias(String[] strings, Principal[] principals, SSLEngine engine) {
            return keyManager.chooseEngineClientAlias(strings, principals, engine);
        }

        @Override
        public String chooseEngineServerAlias(String s, Principal[] principals, SSLEngine engine) {
            return keyManager.chooseEngineServerAlias(s, principals, engine);
        }

        synchronized void setKeyManager(X509ExtendedKeyManager x509ExtendedKeyManager) {
            this.keyManager = x509ExtendedKeyManager;
        }

        // pkg-private accessor for testing
        X509ExtendedKeyManager getKeyManager() {
            return keyManager;
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ssl.TrustConfig.Reloadable.Listener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

abstract class KeyConfig extends TrustConfig {

    KeyConfig(boolean includeSystem, boolean reloadEnabled) {
        super(includeSystem, reloadEnabled);
    }

    static final KeyConfig NONE = new KeyConfig(false, false) {
        @Override
        X509ExtendedKeyManager[] loadKeyManagers(@Nullable Environment environment) {
            return null;
        }

        @Override
        X509ExtendedTrustManager[] nonSystemTrustManagers(@Nullable Environment environment) {
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

    final KeyManager[] keyManagers(@Nullable Environment environment, @Nullable ResourceWatcherService resourceWatcherService,
                                   @Nullable Listener listener) {
        X509ExtendedKeyManager[] keyManagers = loadKeyManagers(environment);
        if (reloadEnabled && resourceWatcherService != null && listener != null) {
            ReloadableX509KeyManager reloadableX509KeyManager = new ReloadableX509KeyManager(keyManagers[0], environment);
            List<Path> filesToMonitor = filesToMonitor(environment);
            ChangeListener changeListener = new ChangeListener(filesToMonitor, reloadableX509KeyManager, listener);
            try {
                for (Path dir : directoriesToMonitor(filesToMonitor)) {
                    FileWatcher fileWatcher = new FileWatcher(dir);
                    fileWatcher.addListener(changeListener);
                    resourceWatcherService.add(fileWatcher, Frequency.HIGH);
                }
                return new X509ExtendedKeyManager[] { reloadableX509KeyManager };
            } catch (IOException e) {
                throw new ElasticsearchException("failed to add file watcher", e);
            }
        }
        return keyManagers;
    }

    abstract X509ExtendedKeyManager[] loadKeyManagers(@Nullable Environment environment);

    final class ReloadableX509KeyManager extends X509ExtendedKeyManager implements Reloadable {

        private final Environment environment;
        private volatile X509ExtendedKeyManager keyManager;

        ReloadableX509KeyManager(X509ExtendedKeyManager keyManager, @Nullable Environment environment) {
            this.keyManager = keyManager;
            this.environment = environment;
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

        public synchronized void reload() {
            X509ExtendedKeyManager[] keyManagers = loadKeyManagers(environment);
            this.keyManager = keyManagers[0];
        }

        synchronized void setKeyManager(X509ExtendedKeyManager x509ExtendedKeyManager) {
            this.keyManager = x509ExtendedKeyManager;
        }
    }
}

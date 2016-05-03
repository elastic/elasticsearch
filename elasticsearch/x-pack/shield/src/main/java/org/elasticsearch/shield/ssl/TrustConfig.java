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
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.ResourceWatcherService.Frequency;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract class TrustConfig {

    protected final boolean includeSystem;
    protected final boolean reloadEnabled;

    TrustConfig(boolean includeSystem, boolean reloadEnabled) {
        this.includeSystem = includeSystem;
        this.reloadEnabled = reloadEnabled;
    }

    final TrustManager[] trustManagers(@Nullable Environment environment, @Nullable ResourceWatcherService resourceWatcherService,
                                       @Nullable Listener listener) {
        X509ExtendedTrustManager[] trustManagers = loadAndMergeIfNecessary(environment);
        if (reloadEnabled && resourceWatcherService != null && listener != null) {
            ReloadableTrustManager reloadableTrustManager = new ReloadableTrustManager(trustManagers[0], environment);
            try {
                List<Path> filesToMonitor = filesToMonitor(environment);
                ChangeListener changeListener = new ChangeListener(filesToMonitor, reloadableTrustManager, listener);
                for (Path path : directoriesToMonitor(filesToMonitor)) {
                    FileWatcher fileWatcher = new FileWatcher(path);
                    fileWatcher.addListener(changeListener);
                    resourceWatcherService.add(fileWatcher, Frequency.HIGH);
                }
                return new X509ExtendedTrustManager[] { reloadableTrustManager };
            } catch (IOException e) {
                throw new ElasticsearchException("failed to add file watcher", e);
            }
        }
        return trustManagers;
    }

    abstract X509ExtendedTrustManager[] nonSystemTrustManagers(@Nullable Environment environment);

    abstract void validate();

    abstract List<Path> filesToMonitor(@Nullable Environment environment);

    public abstract String toString();

    private X509ExtendedTrustManager[] loadAndMergeIfNecessary(@Nullable Environment environment) {
        X509ExtendedTrustManager[] nonSystemTrustManagers = nonSystemTrustManagers(environment);
        if (includeSystem) {
            return mergeWithSystem(nonSystemTrustManagers);
        } else if (nonSystemTrustManagers == null || nonSystemTrustManagers.length == 0) {
            return new X509ExtendedTrustManager[0];
        }
        return nonSystemTrustManagers;
    }

    private X509ExtendedTrustManager[] mergeWithSystem(X509ExtendedTrustManager[] nonSystemTrustManagers) {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            TrustManager[] systemTrustManagers = tmf.getTrustManagers();
            X509ExtendedTrustManager system = findFirstX509TrustManager(systemTrustManagers);
            if (nonSystemTrustManagers == null || nonSystemTrustManagers.length == 0) {
                return new X509ExtendedTrustManager[] { system };
            }

            return new X509ExtendedTrustManager[] { new CombiningX509TrustManager(nonSystemTrustManagers[0], system) };
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a trust managers", e);
        }
    }

    private static X509ExtendedTrustManager findFirstX509TrustManager(TrustManager[] trustManagers) {
        X509ExtendedTrustManager x509TrustManager = null;
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509TrustManager) {
                // first one wins like in the JDK
                x509TrustManager = (X509ExtendedTrustManager) trustManager;
                break;
            }
        }
        if (x509TrustManager == null) {
            throw new IllegalArgumentException("did not find a X509TrustManager");
        }
        return x509TrustManager;
    }

    static Set<Path> directoriesToMonitor(List<Path> filePaths) {
        Set<Path> paths = new HashSet<>();
        for (Path path : filePaths) {
            assert Files.isDirectory(path) == false;
            paths.add(path.getParent());
        }
        return paths;
    }

    private static class CombiningX509TrustManager extends X509ExtendedTrustManager {

        private final X509ExtendedTrustManager first;
        private final X509ExtendedTrustManager second;

        private final X509Certificate[] acceptedIssuers;

        CombiningX509TrustManager(X509ExtendedTrustManager first, X509ExtendedTrustManager second) {
            this.first = first;
            this.second = second;
            X509Certificate[] firstIssuers = first.getAcceptedIssuers();
            X509Certificate[] secondIssuers = second.getAcceptedIssuers();
            this.acceptedIssuers = new X509Certificate[firstIssuers.length + secondIssuers.length];
            System.arraycopy(firstIssuers, 0, acceptedIssuers, 0, firstIssuers.length);
            System.arraycopy(secondIssuers, 0, acceptedIssuers, firstIssuers.length, secondIssuers.length);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s, socket);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s, socket);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s, socket);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s, socket);
            }
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s, sslEngine);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s, sslEngine);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s, sslEngine);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s, sslEngine);
            }
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            try {
                first.checkClientTrusted(x509Certificates, s);
            } catch (CertificateException e) {
                second.checkClientTrusted(x509Certificates, s);
            }
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            try {
                first.checkServerTrusted(x509Certificates, s);
            } catch (CertificateException e) {
                second.checkServerTrusted(x509Certificates, s);
            }
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return acceptedIssuers;
        }
    }

    final class ReloadableTrustManager extends X509ExtendedTrustManager implements Reloadable {

        private final Environment environment;
        private volatile X509ExtendedTrustManager trustManager;

        ReloadableTrustManager(X509ExtendedTrustManager trustManager, @Nullable Environment environment) {
            this.trustManager = trustManager;
            this.environment = environment;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s, socket);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s, socket);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s, sslEngine);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s, sslEngine);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            trustManager.checkClientTrusted(x509Certificates, s);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            trustManager.checkServerTrusted(x509Certificates, s);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return trustManager.getAcceptedIssuers();
        }

        public synchronized void reload() {
            X509ExtendedTrustManager[] array = loadAndMergeIfNecessary(environment);
            this.trustManager = array[0];
        }

        synchronized void setTrustManager(X509ExtendedTrustManager trustManager) {
            this.trustManager = trustManager;
        }
    }

    interface Reloadable {

        void reload();

        interface Listener {

            void onReload();

            void onFailure(Exception e);

        }
    }

    protected static class ChangeListener extends FileChangesListener {

        private final List<Path> paths;
        private final Reloadable reloadable;
        private final Listener listener;

        protected ChangeListener(List<Path> paths, Reloadable reloadable, Listener listener) {
            this.paths = paths;
            this.reloadable = reloadable;
            this.listener = listener;
        }

        @Override
        public void onFileDeleted(Path file) {
            onFileChanged(file);
        }

        @Override
        public void onFileChanged(Path file) {
            for (Path path : paths) {
                if (file.equals(path)) {
                    try {
                        reloadable.reload();
                        listener.onReload();
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                    break;
                }
            }
        }
    }
}

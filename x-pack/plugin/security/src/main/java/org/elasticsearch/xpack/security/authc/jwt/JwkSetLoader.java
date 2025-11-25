/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.PrivilegedFileWatcher;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * This class is responsible for loading the JWK set for PKC signature from either a file or URL.
 * The JWK set is loaded once when the class is instantiated. Subsequent reloading is triggered
 * by invoking the {@link #reload(ActionListener)} method. The updated JWK set can be retrieved with
 * the {@link #getContentAndJwksAlgs()} method once loading or reloading is completed. Additionally,
 * {@link JwtRealm} settings can specify reloading parameters to enable periodic background reloading
 * of the JWK set.
 */
class JwkSetLoader implements Releasable {

    private static final Logger logger = LogManager.getLogger(JwkSetLoader.class);

    private static final double URL_RELOAD_JITTER_PCT = 0.01; // 1% jitter

    private final AtomicReference<ListenableFuture<Void>> reloadFutureRef = new AtomicReference<>();
    private final RealmConfig realmConfig;
    private final List<String> allowedJwksAlgsPkc;
    private final PkcJwkSetLoader loader;
    private final PkcJwkSetReloadNotifier reloadNotifier;
    private volatile ContentAndJwksAlgs contentAndJwksAlgs = new ContentAndJwksAlgs(
        new byte[32],
        new JwksAlgs(Collections.emptyList(), Collections.emptyList())
    );

    JwkSetLoader(
        final RealmConfig realmConfig,
        final List<String> allowedJwksAlgsPkc,
        final SSLService sslService,
        final ThreadPool threadPool, // null only for tests
        final PkcJwkSetReloadNotifier reloadNotifier
    ) {
        this.realmConfig = realmConfig;
        this.allowedJwksAlgsPkc = allowedJwksAlgsPkc;
        // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
        String jwkSetPath = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
        assert Strings.hasText(jwkSetPath);
        URI jwkSetPathUri = JwtUtil.parseHttpsUri(jwkSetPath);
        Consumer<byte[]> listener = content -> handleReloadedContentAndJwksAlgs(content);
        this.loader = jwkSetPathUri == null
            ? new FilePkcJwkSetLoader(realmConfig, threadPool, jwkSetPath, listener)
            : new UrlPkcJwkSetLoader(realmConfig, threadPool, jwkSetPathUri, JwtUtil.createHttpClient(realmConfig, sslService), listener);
        this.reloadNotifier = reloadNotifier;

        // Any exception during loading requires closing JwkSetLoader's HTTP client to avoid a thread pool leak
        try {
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            reload(future);
            // ASSUME: Blocking read operations are OK during startup
            future.actionGet();
        } catch (Throwable t) {
            close();
            throw t;
        }

        if (realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_ENABLED)) {
            loader.start();
        }
    }

    /**
     * Reload the JWK sets, compare to existing JWK sets and update it to the reloaded value if
     * they are different.
     */
    void reload(final ActionListener<Void> listener) {
        final ListenableFuture<Void> future = getFuture();
        future.addListener(listener);
    }

    ContentAndJwksAlgs getContentAndJwksAlgs() {
        return contentAndJwksAlgs;
    }

    // Package private for testing
    ListenableFuture<Void> getFuture() {
        for (;;) {
            final ListenableFuture<Void> existingFuture = reloadFutureRef.get();
            if (existingFuture != null) {
                return existingFuture;
            }

            final ListenableFuture<Void> newFuture = new ListenableFuture<>();
            if (reloadFutureRef.compareAndSet(null, newFuture)) {
                loadInternal(ActionListener.runBefore(newFuture, () -> {
                    final ListenableFuture<Void> oldValue = reloadFutureRef.getAndSet(null);
                    assert oldValue == newFuture : "future reference changed unexpectedly";
                }));
                return newFuture;
            }
            // else, Another thread set the future-ref before us, just try it all again
        }
    }

    // Package private for testing
    void loadInternal(final ActionListener<Void> listener) {
        loader.load(ActionListener.wrap(content -> {
            handleReloadedContentAndJwksAlgs(content); // reloadNotifier callback invoked inside
            listener.onResponse(null);
        }, listener::onFailure));
    }

    private void handleReloadedContentAndJwksAlgs(byte[] bytes) {
        final ContentAndJwksAlgs newContentAndJwksAlgs = parseContent(bytes);
        assert contentAndJwksAlgs != null;
        if ((Arrays.equals(contentAndJwksAlgs.sha256, newContentAndJwksAlgs.sha256)) == false) {
            logger.info(
                "Reloaded JWK set from sha256=[{}] to sha256=[{}]",
                MessageDigests.toHexString(contentAndJwksAlgs.sha256),
                MessageDigests.toHexString(newContentAndJwksAlgs.sha256)
            );
            contentAndJwksAlgs = newContentAndJwksAlgs;
            reloadNotifier.reloaded();
        }
    }

    private ContentAndJwksAlgs parseContent(final byte[] jwkSetContentBytesPkc) {
        final String jwkSetContentsPkc = new String(jwkSetContentBytesPkc, StandardCharsets.UTF_8);
        final byte[] jwkSetContentsPkcSha256 = JwtUtil.sha256(jwkSetContentsPkc);

        // PKC JWKSet parse contents
        final List<JWK> jwksPkc = JwkValidateUtil.loadJwksFromJwkSetString(
            RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
            jwkSetContentsPkc
        );
        // Filter JWK(s) vs signature algorithms. Only keep JWKs with a matching alg. Only keep algs with a matching JWK.
        final JwksAlgs jwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, allowedJwksAlgsPkc);
        logger.debug(
            "Usable PKC: JWKs=[{}] algorithms=[{}] sha256=[{}]",
            jwksAlgsPkc.jwks().size(),
            String.join(",", jwksAlgsPkc.algs()),
            MessageDigests.toHexString(jwkSetContentsPkcSha256)
        );
        return new ContentAndJwksAlgs(jwkSetContentsPkcSha256, jwksAlgsPkc);
    }

    @Override
    public void close() {
        loader.stop();
    }

    // Filtered JWKs and Algs
    record JwksAlgs(List<JWK> jwks, List<String> algs) {
        JwksAlgs {
            Objects.requireNonNull(jwks, "JWKs must not be null");
            Objects.requireNonNull(algs, "Algs must not be null");
        }

        boolean isEmpty() {
            return jwks.isEmpty() && algs.isEmpty();
        }
    }

    // Original PKC JWKSet(for comparison during refresh), and filtered JWKs and Algs
    record ContentAndJwksAlgs(byte[] sha256, JwksAlgs jwksAlgs) {
        ContentAndJwksAlgs {
            Objects.requireNonNull(jwksAlgs, "Filters JWKs and Algs must not be null");
        }
    }

    interface PkcJwkSetLoader {
        void load(ActionListener<byte[]> listener);

        void start();

        void stop();
    }

    static class FilePkcJwkSetLoader implements PkcJwkSetLoader {
        private final RealmConfig realmConfig;
        private final String jwkSetPath;
        private final ThreadPool threadPool;
        private final Consumer<byte[]> listener;

        private volatile boolean closed = false;
        private Scheduler.Cancellable task;

        FilePkcJwkSetLoader(RealmConfig realmConfig, ThreadPool threadPool, String jwkSetPath, Consumer<byte[]> listener) {
            this.realmConfig = realmConfig;
            this.jwkSetPath = jwkSetPath;
            this.threadPool = threadPool;
            this.listener = listener;
        }

        public void start() {
            var fileWatcher = new FileChangeWatcher(JwtUtil.resolvePath(realmConfig.env(), jwkSetPath));
            TimeValue reloadInterval = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_FILE_INTERVAL);
            task = threadPool.scheduleWithFixedDelay(() -> reload(fileWatcher), reloadInterval, threadPool.generic());
        }

        void reload(FileChangeWatcher fileWatcher) {
            if (closed) {
                logger.debug("Skipping file reload because loader is closed");
                return;
            }
            try {
                if (fileWatcher.changedSinceLastCall() == false) {
                    logger.debug("No changes detected in PKC JWK set file [{}], aborting", jwkSetPath);
                    return;
                }
                logger.debug("Detected change in PKC JWK set file [{}], reloading", jwkSetPath);
                load(ActionListener.wrap(listener::accept, e -> logger.warn("Failed to reload PKC JWK set file [" + jwkSetPath + "]", e)));
            } catch (Exception e) {
                logger.warn("Failed to check for changes in PKC JWK set file [" + jwkSetPath + "]", e);
            }
        }

        @Override
        public void load(ActionListener<byte[]> listener) {
            try {
                logger.trace("Loading PKC JWKs from path [{}]", jwkSetPath);
                final byte[] reloadedBytes = JwtUtil.readFileContents(
                    RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                    jwkSetPath,
                    realmConfig.env()
                );
                listener.onResponse(reloadedBytes);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        public void stop() {
            closed = true;
            if (task != null) {
                task.cancel();
            }
        }
    }

    static class UrlPkcJwkSetLoader implements PkcJwkSetLoader {
        private final RealmConfig realmConfig;
        private final ThreadPool threadPool;
        private final URI jwkSetPathUri;
        private final Consumer<byte[]> listener;
        private final CloseableHttpAsyncClient httpClient;
        private final TimeValue reloadIntervalMin;
        private final TimeValue reloadIntervalMax;

        private volatile boolean closed = false;
        private Scheduler.Cancellable task;

        UrlPkcJwkSetLoader(
            RealmConfig realmConfig,
            ThreadPool threadPool,
            URI jwkSetPathUri,
            CloseableHttpAsyncClient httpClient,
            Consumer<byte[]> listener
        ) {
            this.realmConfig = realmConfig;
            this.threadPool = threadPool;
            this.jwkSetPathUri = jwkSetPathUri;
            this.listener = listener;
            this.httpClient = httpClient;
            this.reloadIntervalMin = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MIN);
            this.reloadIntervalMax = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_RELOAD_URL_INTERVAL_MAX);
            if (reloadIntervalMax.compareTo(reloadIntervalMin) < 0) {
                throw new SettingsException(
                    "Invalid PKC JWK set URL reload intervals: max [" + reloadIntervalMax + "] is less than min [" + reloadIntervalMin + "]"
                );
            }
        }

        public void start() {
            scheduleReload(reloadIntervalMin);
        }

        void scheduleReload(TimeValue period) {
            if (closed) {
                logger.debug("Skipping reload schedule because loader is closed");
                return;
            }
            task = threadPool.schedule(this::reload, period, threadPool.generic());
        }

        void reload() {
            doLoad(ActionListener.wrap(res -> {
                Instant targetTime = res.expires() != null
                    ? res.expires()
                    : (res.maxAgeSeconds() != null ? Instant.now().plusSeconds(res.maxAgeSeconds()) : null);
                TimeValue period = calculateNextUrlReload(reloadIntervalMin, reloadIntervalMax, targetTime, URL_RELOAD_JITTER_PCT);
                logger.debug("Successfully reloaded PKC JWK set from HTTPS URI [{}], reload delay is [{}]", jwkSetPathUri, period);
                listener.accept(res.content()); // exception here will be caught by ActionListener.wrap and handled below
                scheduleReload(period);
            }, e -> {
                logger.warn("Failed to reload PKC JWK set from HTTPS URI [" + jwkSetPathUri + "]", e);
                scheduleReload(reloadIntervalMin);
            }));
        }

        @Override
        public void load(ActionListener<byte[]> listener) {
            doLoad(listener.map(JwtUtil.JwksResponse::content));
        }

        private void doLoad(ActionListener<JwtUtil.JwksResponse> listener) {
            logger.trace("Loading PKC JWKs from https URI [{}]", jwkSetPathUri);
            JwtUtil.readUriContents(
                RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetPathUri,
                httpClient,
                listener
            );
        }

        @Override
        public void stop() {
            closed = true;
            if (task != null) {
                task.cancel();
            }
            try {
                httpClient.close();
            } catch (IOException e) {
                logger.warn(() -> "Exception closing HTTPS client for realm [" + realmConfig.name() + "]", e);
            }
        }
    }

    static TimeValue calculateNextUrlReload(TimeValue minVal, TimeValue maxVal, @Nullable Instant targetTime, double maxJitterPct) {
        if (targetTime == null) {
            return minVal;
        }
        var min = Duration.ofSeconds(minVal.seconds());
        var max = Duration.ofSeconds(maxVal.seconds());
        var target = Duration.between(Instant.now(), targetTime);
        if (target.compareTo(min) < 0) {
            return minVal;
        } else if (target.compareTo(max) > 0) {
            return maxVal;
        } else {
            Duration jitter = jitterSeconds(target, maxJitterPct);
            return TimeValue.timeValueSeconds(target.toSeconds() + jitter.toSeconds()); // target +/- max-jitter
        }
    }

    static Duration jitterSeconds(Duration duration, double maxJitterPct) {
        long maxJitter = (long) (duration.toSeconds() * maxJitterPct);
        long jitter = Randomness.get().nextLong(-maxJitter, maxJitter + 1);
        return Duration.ofSeconds(jitter);
    }

    static class FileChangeWatcher implements FileChangesListener {
        final FileWatcher fileWatcher;
        boolean changed = false;

        FileChangeWatcher(Path path) {
            this.fileWatcher = new PrivilegedFileWatcher(path);
            this.fileWatcher.addListener(this);
        }

        boolean changedSinceLastCall() throws IOException {
            fileWatcher.checkAndNotify(); // may call onFileInit, onFileChanged
            boolean c = changed;
            changed = false;
            return c;
        }

        @Override
        public void onFileChanged(Path file) {
            changed = true;
        }

        @Override
        public void onFileInit(Path file) {
            changed = true;
        }
    }

}

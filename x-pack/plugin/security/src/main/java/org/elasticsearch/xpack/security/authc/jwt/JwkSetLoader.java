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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is responsible for loading the JWK set for PKC signature from either a file or URL.
 * The JWK set is loaded once when the class is instantiated. Subsequent reloading is triggered
 * by invoking the {@link #reload(ActionListener)} method. The updated JWK set can be retrieved with
 * the {@link #getContentAndJwksAlgs()} method once loading or reloading is completed.
 */
public class JwkSetLoader implements Releasable {

    private static final Logger logger = LogManager.getLogger(JwkSetLoader.class);

    private final AtomicReference<ListenableFuture<Tuple<Boolean, JwksAlgs>>> reloadFutureRef = new AtomicReference<>();
    private final RealmConfig realmConfig;
    private final List<String> allowedJwksAlgsPkc;
    private final String jwkSetPath;
    @Nullable
    private final URI jwkSetPathUri;
    @Nullable
    private final CloseableHttpAsyncClient httpClient;
    private volatile ContentAndJwksAlgs contentAndJwksAlgs = null;

    public JwkSetLoader(final RealmConfig realmConfig, List<String> allowedJwksAlgsPkc, final SSLService sslService) {
        this.realmConfig = realmConfig;
        this.allowedJwksAlgsPkc = allowedJwksAlgsPkc;
        // PKC JWKSet can be URL, file, or not set; only initialize HTTP client if PKC JWKSet is a URL.
        this.jwkSetPath = realmConfig.getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
        assert Strings.hasText(this.jwkSetPath);
        this.jwkSetPathUri = JwtUtil.parseHttpsUri(jwkSetPath);
        if (this.jwkSetPathUri == null) {
            this.httpClient = null;
        } else {
            this.httpClient = JwtUtil.createHttpClient(realmConfig, sslService);
        }

        // Any exception during loading requires closing JwkSetLoader's HTTP client to avoid a thread pool leak
        try {
            final PlainActionFuture<Tuple<Boolean, JwksAlgs>> future = new PlainActionFuture<>();
            reload(future);
            // ASSUME: Blocking read operations are OK during startup
            final Boolean isUpdated = future.actionGet().v1();
            assert isUpdated : "initial reload should have updated the JWK set";
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * Reload the JWK sets, compare to existing JWK sets and update it to the reloaded value if
     * they are different. The listener is called with false if the reloaded content is the same
     * as the existing one or true if they are different.
     */
    void reload(final ActionListener<Tuple<Boolean, JwksAlgs>> listener) {
        final ListenableFuture<Tuple<Boolean, JwksAlgs>> future = getFuture();
        future.addListener(listener);
    }

    ContentAndJwksAlgs getContentAndJwksAlgs() {
        return contentAndJwksAlgs;
    }

    // Package private for testing
    ListenableFuture<Tuple<Boolean, JwksAlgs>> getFuture() {
        for (;;) {
            final ListenableFuture<Tuple<Boolean, JwksAlgs>> existingFuture = reloadFutureRef.get();
            if (existingFuture != null) {
                return existingFuture;
            }

            final ListenableFuture<Tuple<Boolean, JwksAlgs>> newFuture = new ListenableFuture<>();
            if (reloadFutureRef.compareAndSet(null, newFuture)) {
                loadInternal(ActionListener.runBefore(newFuture, () -> {
                    final ListenableFuture<Tuple<Boolean, JwksAlgs>> oldValue = reloadFutureRef.getAndSet(null);
                    assert oldValue == newFuture : "future reference changed unexpectedly";
                }));
                return newFuture;
            }
            // else, Another thread set the future-ref before us, just try it all again
        }
    }

    // Package private for testing
    void loadInternal(final ActionListener<Tuple<Boolean, JwksAlgs>> listener) {
        // PKC JWKSet get contents from local file or remote HTTPS URL
        if (httpClient == null) {
            logger.trace("Loading PKC JWKs from path [{}]", jwkSetPath);
            final byte[] reloadedBytes = JwtUtil.readFileContents(
                RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetPath,
                realmConfig.env()
            );
            listener.onResponse(handleReloadedContentAndJwksAlgs(reloadedBytes));
        } else {
            logger.trace("Loading PKC JWKs from https URI [{}]", jwkSetPathUri);
            JwtUtil.readUriContents(
                RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetPathUri,
                httpClient,
                listener.map(reloadedBytes -> {
                    logger.trace("Loaded bytes [{}] from [{}]", reloadedBytes.length, jwkSetPathUri);
                    return handleReloadedContentAndJwksAlgs(reloadedBytes);
                })
            );
        }
    }

    private Tuple<Boolean, JwksAlgs> handleReloadedContentAndJwksAlgs(byte[] bytes) {
        final ContentAndJwksAlgs newContentAndJwksAlgs = parseContent(bytes);
        final boolean isUpdated;
        if (contentAndJwksAlgs != null && Arrays.equals(contentAndJwksAlgs.sha256, newContentAndJwksAlgs.sha256)) {
            logger.debug("No change in reloaded JWK set");
            isUpdated = false;
        } else {
            logger.debug("Reloaded JWK set is different from the existing set");
            contentAndJwksAlgs = newContentAndJwksAlgs;
            isUpdated = true;
        }
        return new Tuple<>(isUpdated, contentAndJwksAlgs.jwksAlgs);
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
        logger.info(
            "Usable PKC: JWKs=[{}] algorithms=[{}] sha256=[{}]",
            jwksAlgsPkc.jwks().size(),
            String.join(",", jwksAlgsPkc.algs()),
            MessageDigests.toHexString(jwkSetContentsPkcSha256)
        );
        return new ContentAndJwksAlgs(jwkSetContentsPkcSha256, jwksAlgsPkc);
    }

    @Override
    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                logger.warn(() -> "Exception closing HTTPS client for realm [" + realmConfig.name() + "]", e);
            }
        }
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

}

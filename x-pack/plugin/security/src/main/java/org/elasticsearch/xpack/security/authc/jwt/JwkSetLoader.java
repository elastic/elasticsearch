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

public class JwkSetLoader implements Releasable {

    private static final Logger logger = LogManager.getLogger(JwkSetLoader.class);

    private final AtomicReference<ListenableFuture<ContentAndJwksAlgs>> reloadFutureRef = new AtomicReference<>();
    private final RealmConfig realmConfig;
    private final List<String> allowedJwksAlgs;
    private final String jwkSetPath;
    @Nullable
    private final URI jwkSetPathUri;
    @Nullable
    private final CloseableHttpAsyncClient httpClient;
    private volatile ContentAndJwksAlgs contentAndJwksAlgs;

    public JwkSetLoader(final RealmConfig realmConfig, final SSLService sslService) {
        this.realmConfig = realmConfig;
        this.allowedJwksAlgs = realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS)
            .stream()
            .filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains)
            .toList();
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
            final PlainActionFuture<ContentAndJwksAlgs> future = new PlainActionFuture<>();
            load(future);
            // ASSUME: Blocking read operations are OK during startup
            contentAndJwksAlgs = future.actionGet();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    /**
     * Load the JWK sets and pass its content to the specified listener.
     */
    void load(final ActionListener<ContentAndJwksAlgs> listener) {
        final ListenableFuture<ContentAndJwksAlgs> future = this.getFuture();
        future.addListener(listener);
    }

    /**
     * Reload the JWK sets, compare to existing JWK sets and update it to the reloaded value if
     * they are different. The listener is called with false if the reloaded content is the same
     * as the existing one or true if they are different.
     */
    void reload(final ActionListener<Boolean> listener) {
        load(ActionListener.wrap(newContentAndJwksAlgs -> {
            if (Arrays.equals(contentAndJwksAlgs.sha256, newContentAndJwksAlgs.sha256)) {
                // No change in JWKSet
                listener.onResponse(false);
            } else {
                contentAndJwksAlgs = newContentAndJwksAlgs;
                listener.onResponse(true);
            }
        }, listener::onFailure));
    }

    ContentAndJwksAlgs getContentAndJwksAlgs() {
        return contentAndJwksAlgs;
    }

    private ListenableFuture<ContentAndJwksAlgs> getFuture() {
        for (;;) {
            final ListenableFuture<ContentAndJwksAlgs> existingFuture = this.reloadFutureRef.get();
            if (existingFuture != null) {
                return existingFuture;
            }

            final ListenableFuture<ContentAndJwksAlgs> newFuture = new ListenableFuture<>();
            if (this.reloadFutureRef.compareAndSet(null, newFuture)) {
                loadInternal(ActionListener.runAfter(newFuture, () -> {
                    final ListenableFuture<ContentAndJwksAlgs> oldValue = this.reloadFutureRef.getAndSet(null);
                    assert oldValue == newFuture : "future reference changed unexpectedly";
                }));
                return newFuture;
            }
            // else, Another thread set the future-ref before us, just try it all again
        }
    }

    private void loadInternal(final ActionListener<ContentAndJwksAlgs> listener) {
        // PKC JWKSet get contents from local file or remote HTTPS URL
        if (httpClient == null) {
            logger.trace("Loading PKC JWKs from path [{}]", jwkSetPath);
            listener.onResponse(
                this.parseContent(
                    JwtUtil.readFileContents(
                        RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                        jwkSetPath,
                        realmConfig.env()
                    )
                )
            );
        } else {
            logger.trace("Loading PKC JWKs from https URI [{}]", jwkSetPathUri);
            JwtUtil.readUriContents(
                RealmSettings.getFullSettingKey(realmConfig, JwtRealmSettings.PKC_JWKSET_PATH),
                jwkSetPathUri,
                httpClient,
                listener.map(bytes -> {
                    logger.trace("Loaded bytes [{}] from [{}]", bytes.length, jwkSetPathUri);
                    return this.parseContent(bytes);
                })
            );
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
        final JwksAlgs jwksAlgsPkc = JwkValidateUtil.filterJwksAndAlgorithms(jwksPkc, allowedJwksAlgs);
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
            return this.jwks.isEmpty() && this.algs.isEmpty();
        }
    }

    private static final ContentAndJwksAlgs EMPTY_CONTENT_AND_JWKS_ALGS = new ContentAndJwksAlgs(null, new JwksAlgs(List.of(), List.of()));

    // Original PKC JWKSet(for comparison during refresh), and filtered JWKs and Algs
    record ContentAndJwksAlgs(byte[] sha256, JwksAlgs jwksAlgs) {
        ContentAndJwksAlgs {
            Objects.requireNonNull(jwksAlgs, "Filters JWKs and Algs must not be null");
        }
    }

}

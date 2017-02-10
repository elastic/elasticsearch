/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.XPackSettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Bootstrap check to ensure that we only use the generated key config in non-production situations. This class is currently public because
 * {@link org.elasticsearch.xpack.security.Security} is in a different package and we use package private accessors of the
 * {@link SSLService} to get the configuration for the node to node transport
 */
public final class SSLBootstrapCheck implements BootstrapCheck {

    private final SSLService sslService;
    private final Settings settings;
    private final Environment environment;

    public SSLBootstrapCheck(SSLService sslService, Settings settings, @Nullable Environment environment) {
        this.sslService = sslService;
        this.settings = settings;
        this.environment = environment;
    }

    @Override
    public boolean check() {
        final Settings transportSSLSettings = settings.getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX);
        return sslService.sslConfiguration(transportSSLSettings).keyConfig() == KeyConfig.NONE
                || isDefaultCACertificateTrusted() || isDefaultPrivateKeyUsed();
    }

    /**
     * Looks at all of the trusted certificates to ensure the default CA is not being trusted. We cannot let this happen in production mode
     */
    private boolean isDefaultCACertificateTrusted() {
        final PublicKey publicKey;
        try {
            publicKey = GeneratedKeyConfig.readCACert().getPublicKey();
        } catch (IOException | CertificateException e) {
            throw new ElasticsearchException("failed to check default CA", e);
        }
        return sslService.getLoadedSSLConfigurations().stream()
                .flatMap(config -> Stream.of(config.keyConfig().createTrustManager(environment),
                        config.trustConfig().createTrustManager(environment)))
                .filter(Objects::nonNull)
                .flatMap((tm) -> Arrays.stream(tm.getAcceptedIssuers()))
                .anyMatch((cert) -> {
                    try {
                        cert.verify(publicKey);
                        return true;
                    } catch (CertificateException | NoSuchAlgorithmException | InvalidKeyException | NoSuchProviderException
                            | SignatureException e) {
                        // just ignore these
                        return false;
                    }
                });
    }

    /**
     * Looks at all of the private keys and if there is a key that is equal to the default CA key then we should bail out
     */
    private boolean isDefaultPrivateKeyUsed() {
        final PrivateKey defaultPrivateKey;
        try {
            defaultPrivateKey = GeneratedKeyConfig.readPrivateKey();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read key", e);
        }

        return sslService.getLoadedSSLConfigurations().stream()
                .flatMap(sslConfiguration -> sslConfiguration.keyConfig().privateKeys(environment).stream())
                .anyMatch(defaultPrivateKey::equals);
    }

    @Override
    public String errorMessage() {
        return "Default SSL key and certificate do not provide security; please generate keys and certificates";
    }
}

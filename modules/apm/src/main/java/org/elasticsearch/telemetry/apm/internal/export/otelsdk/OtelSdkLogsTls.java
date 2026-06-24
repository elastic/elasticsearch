/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.DefaultJdkTrustConfig;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.ssl.SslConfigException;
import org.elasticsearch.common.ssl.SslTrustConfig;

import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * Holds a pre-built {@link SSLContext} and its associated {@link X509ExtendedTrustManager} for the
 * OTel audit-log gRPC exporter, derived from the three {@code telemetry.logs.audit.ssl.*} node settings.
 *
 * <p>The context is built once at startup (static load — no file-watcher); cert rotation requires a
 * node restart. This matches the JWT and OIDC realm HTTP clients, which snapshot an SSLContext into a
 * long-lived client with no cert watcher. If in-place hot-reload without restart becomes necessary,
 * see plan-of-record item 10.
 *
 */
public record OtelSdkLogsTls(SSLContext sslContext, X509ExtendedTrustManager trustManager) {

    /**
     * Build an {@code OtelSdkLogsTls} from the {@code telemetry.logs.audit.ssl.*} settings.
     *
     * @param settings  node settings
     * @param configDir base directory for resolving relative cert/key paths (the Elasticsearch
     *                  config directory, i.e. {@code Environment.configDir()})
     * @return a configured instance, or {@code null} if none of the three SSL settings are set
     *         (in which case the exporter uses its own default TLS handling)
     * @throws IllegalArgumentException if exactly one of {@code certificate} / {@code key} is set
     */
    public static OtelSdkLogsTls fromSettings(Settings settings, Path configDir) {
        List<String> cas = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE_AUTHORITIES.get(settings);
        String cert = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE.get(settings);
        String key = OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_KEY.get(settings);

        if (cas.isEmpty() && cert.isEmpty() && key.isEmpty()) {
            return null;
        }
        if (cert.isEmpty() != key.isEmpty()) {
            throw new IllegalArgumentException(
                OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_CERTIFICATE.getKey()
                    + " and "
                    + OtelSdkSettings.TELEMETRY_LOGS_AUDIT_SSL_KEY.getKey()
                    + " must be set together"
            );
        }

        SslTrustConfig trustConfig = cas.isEmpty() ? DefaultJdkTrustConfig.DEFAULT_INSTANCE : new PemTrustConfig(cas, configDir);
        X509ExtendedTrustManager tm = trustConfig.createTrustManager();

        KeyManager[] kms = cert.isEmpty()
            ? null
            : new KeyManager[] { new PemKeyConfig(cert, key, new char[0], configDir).createKeyManager() };

        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kms, new TrustManager[] { tm }, null);
            return new OtelSdkLogsTls(ctx, tm);
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot create SSL context for telemetry.logs.audit.ssl", e);
        }
    }
}

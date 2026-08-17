/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.SslProfile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_CONNECT_TIMEOUT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_HOST;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_PORT;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_PROXY_SCHEME;
import static org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings.HTTP_SOCKET_TIMEOUT;

/**
 * Resolves an OpenID Connect Provider's {@code .well-known/openid-configuration} discovery document, so that
 * {@code op.*} endpoint settings that are not explicitly configured can be derived from it, per
 * <a href="https://openid.net/specs/openid-connect-discovery-1_0.html">OpenID Connect Discovery 1.0</a>.
 * The fetch is a one-shot, synchronous, blocking call performed at realm construction time; it is only invoked
 * when at least one {@code op.*} endpoint setting has been left unset.
 */
final class OpenIdConnectProviderDiscoveryResolver {

    private OpenIdConnectProviderDiscoveryResolver() {}

    static OIDCProviderMetadata resolve(RealmConfig config, SSLService sslService, String issuer) {
        final URI wellKnownUri = buildWellKnownUri(config, issuer);
        final String body = fetch(config, sslService, wellKnownUri);
        final OIDCProviderMetadata metadata;
        try {
            metadata = OIDCProviderMetadata.parse(body);
        } catch (ParseException e) {
            throw new SettingsException("Failed to parse the OpenID Connect provider metadata received from [" + wellKnownUri + "]", e);
        }
        if (metadata.getIssuer().getValue().equals(issuer) == false) {
            throw new SettingsException(
                "The configured issuer ["
                    + issuer
                    + "] does not match the issuer ["
                    + metadata.getIssuer().getValue()
                    + "] returned by the discovery document at ["
                    + wellKnownUri
                    + "]"
            );
        }
        return metadata;
    }

    private static URI buildWellKnownUri(RealmConfig config, String issuer) {
        final String normalizedIssuer = issuer.endsWith("/") ? issuer.substring(0, issuer.length() - 1) : issuer;
        try {
            return new URI(normalizedIssuer + "/.well-known/openid-configuration");
        } catch (URISyntaxException e) {
            throw new SettingsException(
                "Invalid value ["
                    + issuer
                    + "] for ["
                    + RealmSettings.getFullSettingKey(config, OpenIdConnectRealmSettings.OP_ISSUER)
                    + "]. Not a valid URI.",
                e
            );
        }
    }

    private static String fetch(RealmConfig config, SSLService sslService, URI wellKnownUri) {
        final HttpClientBuilder builder = HttpClientBuilder.create();
        final String sslKey = RealmSettings.realmSslPrefix(config.identifier());
        final SslProfile sslProfile = sslService.profile(sslKey);
        final SSLConnectionSocketFactory socketFactory = sslProfile.connectionSocketFactory();
        builder.setSSLSocketFactory(socketFactory);

        final RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(Math.toIntExact(config.getSetting(HTTP_CONNECT_TIMEOUT).getMillis()))
            .setSocketTimeout(Math.toIntExact(config.getSetting(HTTP_SOCKET_TIMEOUT).getMillis()))
            .build();
        builder.setDefaultRequestConfig(requestConfig);

        if (config.hasSetting(HTTP_PROXY_HOST)) {
            builder.setProxy(
                new HttpHost(config.getSetting(HTTP_PROXY_HOST), config.getSetting(HTTP_PROXY_PORT), config.getSetting(HTTP_PROXY_SCHEME))
            );
        }

        try (CloseableHttpClient httpClient = builder.build()) {
            try (CloseableHttpResponse response = httpClient.execute(new HttpGet(wellKnownUri))) {
                final int statusCode = response.getStatusLine().getStatusCode();
                final String body = EntityUtils.toString(response.getEntity());
                if (statusCode != 200) {
                    throw new SettingsException(
                        "Failed to fetch OpenID Connect provider metadata from ["
                            + wellKnownUri
                            + "] for realm ["
                            + RealmSettings.getFullSettingKey(config, OpenIdConnectRealmSettings.OP_ISSUER)
                            + "]. Unexpected response status ["
                            + statusCode
                            + "]"
                    );
                }
                return body;
            }
        } catch (IOException e) {
            throw new SettingsException(
                "Failed to fetch OpenID Connect provider metadata from ["
                    + wellKnownUri
                    + "] for realm ["
                    + RealmSettings.getFullSettingKey(config, OpenIdConnectRealmSettings.OP_ISSUER)
                    + "]",
                e
            );
        }
    }
}

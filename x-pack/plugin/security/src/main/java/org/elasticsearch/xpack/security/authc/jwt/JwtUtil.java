/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

/**
 * Utilities for JWT realm.
 */
public class JwtUtil {
    private static final Logger LOGGER = LogManager.getLogger(JwtUtil.class);

    /**
     * Get header from threadContext, look for the scheme name, and extract the value after it.
     * @param threadContext Contains the request parameters.
     * @param headerName Header name to look for.
     * @param schemeName Scheme name to look for
     * @param ignoreSchemeNameCase Ignore case of scheme name.
     * @return If found, the trimmed value after the scheme name. Null if parameter not found, or scheme mismatch.
     */
    public static SecureString getHeaderValue(
        final ThreadContext threadContext,
        final String headerName,
        final String schemeName,
        final boolean ignoreSchemeNameCase
    ) {
        final String headerValue = threadContext.getHeader(headerName);
        if (Strings.hasText(headerValue)) {
            final String schemeValuePlusSpace = schemeName + " ";
            if (headerValue.regionMatches(ignoreSchemeNameCase, 0, schemeValuePlusSpace, 0, schemeValuePlusSpace.length())) {
                final String trimmedSchemeParameters = headerValue.substring(schemeValuePlusSpace.length()).trim();
                return new SecureString(trimmedSchemeParameters.toCharArray());
            }
        }
        return null;
    }

    // Static method for unit testing. No need to construct a complete RealmConfig with all settings.
    public static void validateClientAuthenticationSettings(
        final String clientAuthenticationTypeConfigKey,
        final JwtRealmSettings.ClientAuthenticationType clientAuthenticationType,
        final String clientAuthenticationSharedSecretConfigKey,
        final SecureString clientAuthenticationSharedSecret
    ) throws SettingsException {
        switch (clientAuthenticationType) {
            case SHARED_SECRET:
                // If type is "SharedSecret", the shared secret value must be set
                if (Strings.hasText(clientAuthenticationSharedSecret) == false) {
                    throw new SettingsException(
                        "Missing setting for ["
                            + clientAuthenticationSharedSecretConfigKey
                            + "]. It is required when setting ["
                            + clientAuthenticationTypeConfigKey
                            + "] is ["
                            + JwtRealmSettings.ClientAuthenticationType.SHARED_SECRET.value()
                            + "]"
                    );
                }
                break;
            case NONE:
            default:
                // If type is "None", the shared secret value must not be set
                if (Strings.hasText(clientAuthenticationSharedSecret)) {
                    throw new SettingsException(
                        "Setting ["
                            + clientAuthenticationSharedSecretConfigKey
                            + "] is not supported, because setting ["
                            + clientAuthenticationTypeConfigKey
                            + "] is ["
                            + JwtRealmSettings.ClientAuthenticationType.NONE.value()
                            + "]"
                    );
                }
                LOGGER.warn(
                    "Setting [{}] value [{}] may not be secure. Unauthorized clients may be able to submit JWTs from the same issuer.",
                    clientAuthenticationSharedSecretConfigKey,
                    JwtRealmSettings.ClientAuthenticationType.NONE.value()
                );
                break;
        }
    }

    public static void validateClientAuthentication(
        final JwtRealmSettings.ClientAuthenticationType type,
        final SecureString expectedSecret,
        final SecureString actualSecret
    ) throws Exception {
        switch (type) {
            case SHARED_SECRET:
                if (Strings.hasText(actualSecret) == false) {
                    throw new Exception("Rejected client. Authentication type is [" + type + "] and secret is missing.");
                } else if (expectedSecret.equals(actualSecret) == false) {
                    throw new Exception("Rejected client. Authentication type is [" + type + "] and secret did not match.");
                }
                LOGGER.trace("Accepted client. Authentication type is [{}] and secret matched.", type);
                break;
            case NONE:
            default:
                if (Strings.hasText(actualSecret)) {
                    LOGGER.debug("Accepted client. Authentication type [{}]. Secret is present but ignored.", type);
                } else {
                    LOGGER.trace("Accepted client. Authentication type [{}].", type);
                }
                break;
        }
    }

    public static URI parseHttpsUri(final String uriString) {
        if (Strings.hasText(uriString)) {
            if (uriString.startsWith("https")) {
                final URI uri;
                try {
                    uri = new URI(uriString);
                } catch (Exception e) {
                    throw new SettingsException("Failed to parse HTTPS URI [" + uriString + "].", e);
                }
                if (Strings.hasText(uri.getHost()) == false) {
                    // Example URIs w/o host: "https:/", "https://", "https://:443"
                    throw new SettingsException("Host is missing in HTTPS URI [" + uriString + "].");
                }
                return uri;
            } else if (uriString.startsWith("http")) {
                throw new SettingsException("Not allowed to use HTTP URI [" + uriString + "]. Only HTTPS is supported.");
            } else {
                LOGGER.trace("Not a HTTPS URI [{}].", uriString);
            }
        }
        return null;
    }

    public static byte[] readUriContents(
        final String jwkSetConfigKeyPkc,
        final URI jwkSetPathPkcUri,
        final CloseableHttpAsyncClient httpClient
    ) throws SettingsException {
        try {
            return JwtUtil.readBytes(httpClient, jwkSetPathPkcUri);
        } catch (Exception e) {
            throw new SettingsException("Can't get contents for setting [" + jwkSetConfigKeyPkc + "] value [" + jwkSetPathPkcUri + "].", e);
        }
    }

    public static byte[] readFileContents(final String jwkSetConfigKeyPkc, final String jwkSetPathPkc, final Environment environment)
        throws SettingsException {
        try {
            final Path path = JwtUtil.resolvePath(environment, jwkSetPathPkc);
            return Files.readAllBytes(path);
        } catch (Exception e) {
            throw new SettingsException(
                "Failed to read contents for setting [" + jwkSetConfigKeyPkc + "] value [" + jwkSetPathPkc + "].",
                e
            );
        }
    }

    public static String serializeJwkSet(final JWKSet jwkSet, final boolean publicKeysOnly) {
        if ((jwkSet == null) || (jwkSet.getKeys().isEmpty())) {
            return null;
        }
        return JSONObjectUtils.toJSONString(jwkSet.toJSONObject(publicKeysOnly));
    }

    public static String serializeJwkHmacOidc(final JWK key) {
        return new String(key.toOctetSequenceKey().toByteArray(), StandardCharsets.UTF_8);
    }

    /**
     * Creates a {@link CloseableHttpAsyncClient} that uses a {@link PoolingNHttpClientConnectionManager}
     * @param realmConfig Realm config for a JWT realm.
     * @param sslService Realm config for SSL.
     * @return Initialized HTTPS client.
     */
    public static CloseableHttpAsyncClient createHttpClient(final RealmConfig realmConfig, final SSLService sslService) {
        try {
            SpecialPermission.check();
            return AccessController.doPrivileged((PrivilegedExceptionAction<CloseableHttpAsyncClient>) () -> {
                final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
                final String sslKey = RealmSettings.realmSslPrefix(realmConfig.identifier());
                final SslConfiguration sslConfiguration = sslService.getSSLConfiguration(sslKey);
                final SSLContext clientContext = sslService.sslContext(sslConfiguration);
                final HostnameVerifier verifier = SSLService.getHostnameVerifier(sslConfiguration);
                final Registry<SchemeIOSessionStrategy> registry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                    .register("https", new SSLIOSessionStrategy(clientContext, verifier))
                    .build();
                final PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor, registry);
                connectionManager.setDefaultMaxPerRoute(realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_ENDPOINT_CONNECTIONS));
                connectionManager.setMaxTotal(realmConfig.getSetting(JwtRealmSettings.HTTP_MAX_CONNECTIONS));
                final RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(Math.toIntExact(realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECT_TIMEOUT).getMillis()))
                    .setConnectionRequestTimeout(
                        Math.toIntExact(realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT).getSeconds())
                    )
                    .setSocketTimeout(Math.toIntExact(realmConfig.getSetting(JwtRealmSettings.HTTP_SOCKET_TIMEOUT).getMillis()))
                    .build();
                final HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom()
                    .setConnectionManager(connectionManager)
                    .setDefaultRequestConfig(requestConfig);
                final CloseableHttpAsyncClient httpAsyncClient = httpAsyncClientBuilder.build();
                httpAsyncClient.start();
                return httpAsyncClient;
            });
        } catch (PrivilegedActionException e) {
            throw new IllegalStateException("Unable to create a HttpAsyncClient instance", e);
        }
    }

    /**
     * Use the HTTP Client to get URL content bytes up to N max bytes.
     * @param httpClient Configured HTTP/HTTPS client.
     * @param uri URI to download.
     * @return Byte array of the URI contents up to N max bytes.
     */
    public static byte[] readBytes(final CloseableHttpAsyncClient httpClient, final URI uri) {
        final PlainActionFuture<byte[]> plainActionFuture = PlainActionFuture.newFuture();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            httpClient.execute(new HttpGet(uri), new FutureCallback<>() {
                @Override
                public void completed(final HttpResponse result) {
                    final StatusLine statusLine = result.getStatusLine();
                    final int statusCode = statusLine.getStatusCode();
                    if (statusCode == 200) {
                        final HttpEntity entity = result.getEntity();
                        try (InputStream inputStream = entity.getContent()) {
                            plainActionFuture.onResponse(inputStream.readAllBytes());
                        } catch (Exception e) {
                            plainActionFuture.onFailure(e);
                        }
                    } else {
                        plainActionFuture.onFailure(
                            new ElasticsearchSecurityException(
                                "Get [" + uri + "] failed, status [" + statusCode + "], reason [" + statusLine.getReasonPhrase() + "]."
                            )
                        );
                    }
                }

                @Override
                public void failed(Exception e) {
                    plainActionFuture.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] failed.", e));
                }

                @Override
                public void cancelled() {
                    plainActionFuture.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] was cancelled."));
                }
            });
            return null;
        });
        return plainActionFuture.actionGet();
    }

    public static Path resolvePath(final Environment environment, final String jwkSetPath) {
        final Path directoryPath = environment.configFile();
        return directoryPath.resolve(jwkSetPath);
    }

    /**
     * Concatenate values with separator strings.
     * Same method signature as {@link java.lang.String#join(CharSequence, CharSequence...)}.
     *
     * @param delimiter Separator string between the concatenated values.
     * @param secureStrings SecureString values to concatenate.
     * @return SecureString of the concatenated values with separator strings.
     */
    public static SecureString join(final CharSequence delimiter, final CharSequence... secureStrings) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < secureStrings.length; i++) {
            if (i != 0) {
                sb.append(delimiter);
            }
            sb.append(secureStrings[i]); // allow null
        }
        return new SecureString(sb.toString().toCharArray());
    }

    /**
     * Format and filter JWT contents as user metadata.
     *   JWSHeader: Header are not support.
     *   JWTClaimsSet: Claims are supported. Claim keys are prefixed by "jwt_claim_".
     *   Base64URL: Signature is not supported.
     * @param jwt SignedJWT object.
     * @return Map of formatted and filtered values to be used as user metadata.
     * @throws Exception Parse error.
     */
    //
    // Values will be filtered by type using isAllowedTypeForClaim().
    public static Map<String, Object> toUserMetadata(final SignedJWT jwt) throws Exception {
        final JWTClaimsSet claimsSet = jwt.getJWTClaimsSet();
        return claimsSet.getClaims()
            .entrySet()
            .stream()
            .filter(entry -> JwtUtil.isAllowedTypeForClaim(entry.getValue()))
            .collect(Collectors.toUnmodifiableMap(entry -> "jwt_claim_" + entry.getKey(), Map.Entry::getValue));
    }

    /**
     * JWTClaimsSet values are only allowed to be String, Boolean, Number, or Collection.
     * Collections are only allowed to contain String, Boolean, or Number.
     * Collections recursion is not allowed.
     * Maps are not allowed.
     * Nulls are not allowed.
     * @param value Claim value object.
     * @return True if the claim value is allowed, otherwise false.
     */
    static boolean isAllowedTypeForClaim(final Object value) {
        return (value instanceof String
            || value instanceof Boolean
            || value instanceof Number
            || (value instanceof Collection
                && ((Collection<?>) value).stream().allMatch(e -> e instanceof String || e instanceof Boolean || e instanceof Number)));
    }
}

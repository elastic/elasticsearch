/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.nimbusds.jwt.JWT;
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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressLoggerChecks;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.settings.RotatableSecret;
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
import java.security.MessageDigest;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

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
        final RotatableSecret clientAuthenticationSharedSecret
    ) throws SettingsException {
        switch (clientAuthenticationType) {
            case SHARED_SECRET:
                // If type is "SharedSecret", the shared secret value must be set
                if (clientAuthenticationSharedSecret.isSet() == false) {
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
                if (clientAuthenticationSharedSecret.isSet()) {
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
        final RotatableSecret expectedSecret,
        final SecureString actualSecret,
        final String tokenPrincipal
    ) throws Exception {
        switch (type) {
            case SHARED_SECRET:
                if (Strings.hasText(actualSecret) == false) {
                    throw new Exception("Rejected client. Authentication type is [" + type + "] and secret is missing.");
                } else if (expectedSecret.matches(actualSecret) == false) {
                    throw new Exception("Rejected client. Authentication type is [" + type + "] and secret did not match.");
                }
                LOGGER.trace("Accepted client for token [{}]. Authentication type is [{}] and secret matched.", tokenPrincipal, type);
                break;
            case NONE:
            default:
                if (Strings.hasText(actualSecret)) {
                    LOGGER.trace(
                        "Accepted client for token [{}]. Authentication type [{}]. Secret is present but ignored.",
                        tokenPrincipal,
                        type
                    );
                } else {
                    LOGGER.trace("Accepted client for token [{}]. Authentication type [{}].", tokenPrincipal, type);
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

    public static void readUriContents(
        final String jwkSetConfigKeyPkc,
        final URI jwkSetPathPkcUri,
        final CloseableHttpAsyncClient httpClient,
        final ActionListener<byte[]> listener
    ) {
        JwtUtil.readBytes(
            httpClient,
            jwkSetPathPkcUri,
            ActionListener.wrap(
                listener::onResponse,
                ex -> listener.onFailure(
                    new SettingsException(
                        "Can't get contents for setting [" + jwkSetConfigKeyPkc + "] value [" + jwkSetPathPkcUri + "].",
                        ex
                    )
                )
            )
        );
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
        if (jwkSet == null) {
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
                        Math.toIntExact(realmConfig.getSetting(JwtRealmSettings.HTTP_CONNECTION_READ_TIMEOUT).getMillis())
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
     * Use the HTTP Client to get URL content bytes.
     * @param httpClient Configured HTTP/HTTPS client.
     * @param uri URI to download.
     */
    public static void readBytes(final CloseableHttpAsyncClient httpClient, final URI uri, ActionListener<byte[]> listener) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            httpClient.execute(new HttpGet(uri), new FutureCallback<>() {
                @Override
                public void completed(final HttpResponse result) {
                    final StatusLine statusLine = result.getStatusLine();
                    final int statusCode = statusLine.getStatusCode();
                    if (statusCode == 200) {
                        final HttpEntity entity = result.getEntity();
                        try (InputStream inputStream = entity.getContent()) {
                            listener.onResponse(inputStream.readAllBytes());
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    } else {
                        listener.onFailure(
                            new ElasticsearchSecurityException(
                                "Get [" + uri + "] failed, status [" + statusCode + "], reason [" + statusLine.getReasonPhrase() + "]."
                            )
                        );
                    }
                }

                @Override
                public void failed(Exception e) {
                    listener.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] failed.", e));
                }

                @Override
                public void cancelled() {
                    listener.onFailure(new ElasticsearchSecurityException("Get [" + uri + "] was cancelled."));
                }
            });
            return null;
        });
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

    public static byte[] sha256(final CharSequence charSequence) {
        final MessageDigest messageDigest = MessageDigests.sha256();
        messageDigest.update(charSequence.toString().getBytes(StandardCharsets.UTF_8));
        return messageDigest.digest();
    }

    public static SignedJWT parseSignedJWT(SecureString token) {
        if (token == null || token.isEmpty()) {
            return null;
        }
        // a lightweight pre-check for JWTs
        if (containsAtLeastTwoDots(token) == false) {
            return null;
        }
        try {
            SignedJWT signedJWT = SignedJWT.parse(token.toString());
            // trigger claim set parsing (the parsed version will be cached internally)
            signedJWT.getJWTClaimsSet();
            return signedJWT;
        } catch (ParseException e) {
            LOGGER.debug("Failed to parse JWT bearer token", e);
            return null;
        }
    }

    /**
     * Helper class to consolidate multiple trace level statements to a single trace statement with lazy evaluation.
     * If trace level is not enabled, then no work is performed. This class is not threadsafe and is not intended for a long lifecycle.
     */
    public static class TraceBuffer implements AutoCloseable {
        private final Logger logger;
        private final List<Object> params = new ArrayList<>();
        private final StringBuilder builder = new StringBuilder();
        boolean closed = false;

        public TraceBuffer(Logger logger) {
            this.logger = logger;
        }

        public void append(String s, Object... args) {
            assert closed == false;
            if (logger.isTraceEnabled()) {
                builder.append(s).append(" ");
                List<Object> resolved = Arrays.stream(args).map(x -> (x instanceof Supplier) ? ((Supplier<?>) x).get() : x).toList();
                params.addAll(resolved);
            }
        }

        @SuppressLoggerChecks(reason = "builds the tracer dynamically")
        public void flush() {
            assert closed == false;
            if (logger.isTraceEnabled() && builder.isEmpty() == false) {
                logger.trace(builder.toString(), params.toArray());
                params.clear();
                builder.setLength(0); // does not guarantee contents available for GC, don't overly reuse a single instance of this class
            }
        }

        @Override
        public void close() {
            flush();
            closed = true;
        }
    }

    /**
     * @param jwt The signed JWT
     * @return A print safe supplier to describe a JWT that redacts the signature. While the signature is not generally sensitive,
     * we don't want to leak the entire JWT to the log to avoid a possible replay.
     */
    public static Supplier<String> toStringRedactSignature(JWT jwt) {
        if (jwt instanceof JWSObject) {
            Base64URL[] parts = jwt.getParsedParts();
            assert parts.length == 3;
            assert parts[0] != null;
            assert parts[1] != null;
            assert parts[2] != null;
            assert Objects.equals(parts[2], ((JWSObject) jwt).getSignature());
            return () -> parts[0] + "." + parts[1] + ".<redacted-signature>";
        } else {
            return jwt::getParsedString;
        }
    }

    /**
     * This is a lightweight pre-check for the JWT token format.
     * If this returns {@code true}, the token MIGHT be a JWT. Otherwise, the token is definitely not a JWT.
     */
    private static boolean containsAtLeastTwoDots(SecureString secureString) {
        if (secureString == null || secureString.length() < 2) {
            return false;
        }
        int ndots = 0;
        for (int i = 0; i < secureString.length(); i++) {
            if (secureString.charAt(i) == '.' && ++ndots >= 2) {
                return true;
            }
        }
        return false;
    }
}

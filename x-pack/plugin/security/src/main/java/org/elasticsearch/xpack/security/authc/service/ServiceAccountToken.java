/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * A decoded credential that may be used to authenticate a {@link ServiceAccount}.
 * It consists of:
 * <ol>
 *   <li>A {@link #getTokenId() service account token ID}</li>
 *   <li>The {@link #getSecret() secret credential} for that token</li>
 * </ol>
 */
public class ServiceAccountToken implements AuthenticationToken, Closeable {

    public static final byte MAGIC_BYTE = '\0';
    public static final byte TOKEN_TYPE = '\1';
    public static final byte RESERVED_BYTE = '\0';
    public static final byte FORMAT_VERSION = '\1';
    public static final byte[] PREFIX = new byte[] { MAGIC_BYTE, TOKEN_TYPE, RESERVED_BYTE, FORMAT_VERSION };

    private static final Logger logger = LogManager.getLogger(ServiceAccountToken.class);

    private final ServiceAccountTokenId tokenId;
    private final SecureString secret;

    // pkg private for testing
    ServiceAccountToken(ServiceAccountId accountId, String tokenName, SecureString secret) {
        tokenId = new ServiceAccountTokenId(accountId, tokenName);
        this.secret = Objects.requireNonNull(secret, "service account token secret cannot be null");
    }

    public ServiceAccountTokenId getTokenId() {
        return tokenId;
    }

    public SecureString getSecret() {
        return secret;
    }

    public ServiceAccountId getAccountId() {
        return tokenId.getAccountId();
    }

    public String getTokenName() {
        return tokenId.getTokenName();
    }

    public String getQualifiedName() {
        return tokenId.getQualifiedName();
    }

    public SecureString asBearerString() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            out.writeBytes(PREFIX);
            out.write(getQualifiedName().getBytes(StandardCharsets.UTF_8));
            out.write(':');
            out.write(secret.toString().getBytes(StandardCharsets.UTF_8));
            final String base64 = Base64.getEncoder().withoutPadding().encodeToString(out.toByteArray());
            return new SecureString(base64.toCharArray());
        }
    }

    public static ServiceAccountToken fromBearerString(SecureString bearerString) throws IOException {
        final byte[] bytes = CharArrays.toUtf8Bytes(bearerString.getChars());
        try (InputStream in = Base64.getDecoder().wrap(new ByteArrayInputStream(bytes))) {
            final byte[] prefixBytes = in.readNBytes(4);
            if (prefixBytes.length != 4 || false == Arrays.equals(prefixBytes, PREFIX)) {
                logger.trace(
                    () -> format(
                        "service account token expects the 4 leading bytes to be %s, got %s.",
                        Arrays.toString(PREFIX),
                        Arrays.toString(prefixBytes)
                    )
                );
                return null;
            }
            final char[] content = CharArrays.utf8BytesToChars(in.readAllBytes());
            final int i = UsernamePasswordToken.indexOfColon(content);
            if (i < 0) {
                logger.trace("failed to extract qualified service token name and secret, missing ':'");
                return null;
            }
            final String qualifiedName = new String(Arrays.copyOfRange(content, 0, i));
            final String[] split = Strings.delimitedListToStringArray(qualifiedName, "/");
            if (split == null || split.length != 3) {
                logger.trace(
                    "The qualified name of a service token should take format of " + "'namespace/service_name/token_name', got [{}]",
                    qualifiedName
                );
                return null;
            }
            return new ServiceAccountToken(
                new ServiceAccountId(split[0], split[1]),
                split[2],
                new SecureString(Arrays.copyOfRange(content, i + 1, content.length))
            );
        }
    }

    @Override
    public void close() {
        secret.close();
    }

    @Override
    public String toString() {
        return getQualifiedName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceAccountToken that = (ServiceAccountToken) o;
        return tokenId.equals(that.tokenId) && secret.equals(that.secret);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenId, secret);
    }

    public static ServiceAccountToken newToken(ServiceAccountId accountId, String tokenName) {
        return new ServiceAccountToken(accountId, tokenName, UUIDs.randomBase64UUIDSecureString());
    }

    @Override
    public String principal() {
        return tokenId.getAccountId().asPrincipal();
    }

    @Override
    public Object credentials() {
        return secret;
    }

    @Override
    public void clearCredentials() {
        close();
    }

    public static class ServiceAccountTokenId {
        private final ServiceAccountId accountId;
        private final String tokenName;

        public ServiceAccountTokenId(ServiceAccountId accountId, String tokenName) {
            this.accountId = Objects.requireNonNull(accountId, "service account ID cannot be null");
            if (false == Validation.isValidServiceAccountTokenName(tokenName)) {
                throw new IllegalArgumentException(Validation.formatInvalidServiceTokenNameErrorMessage(tokenName));
            }
            this.tokenName = Objects.requireNonNull(tokenName, "service account token name cannot be null");
        }

        public ServiceAccountId getAccountId() {
            return accountId;
        }

        public String getTokenName() {
            return tokenName;
        }

        public String getQualifiedName() {
            return accountId.asPrincipal() + "/" + tokenName;
        }

        @Override
        public String toString() {
            return getQualifiedName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServiceAccountTokenId that = (ServiceAccountTokenId) o;
            return accountId.equals(that.accountId) && tokenName.equals(that.tokenName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, tokenName);
        }
    }
}

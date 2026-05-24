/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Base64;

/**
 * Credentials for API key authentication. Holds the decoded id and secret from the
 * encoded API key string (Base64(id + ":" + secret)).
 */
public final class ApiKeyCredentials implements AuthenticationToken, Closeable {

    /**
     * Length of the secret portion of a cross-cluster API key (after Base64 decoding).
     */
    private static final int CROSS_CLUSTER_API_KEY_SECRET_LENGTH = 22;

    private final String id;
    private final SecureString key;
    private final ApiKey.Type expectedType;
    private final String certificateIdentity;

    public ApiKeyCredentials(String id, SecureString key, ApiKey.Type expectedType) {
        this(id, key, expectedType, null);
    }

    public ApiKeyCredentials(String id, SecureString key, ApiKey.Type expectedType, @Nullable String certificateIdentity) {
        this.id = id;
        this.key = key;
        this.expectedType = expectedType;
        this.certificateIdentity = certificateIdentity;
    }

    /**
     * Parses the encoded API key credential (Base64(id + ":" + secret)) into an {@link ApiKeyCredentials} instance.
     *
     * @param apiKeyString the encoded API key string, or null
     * @param certificateIdentity optional certificate identity for cross-cluster API keys
     * @param expectedType the expected API key type (validates secret length for cross-cluster keys)
     * @return the parsed credentials, or null if apiKeyString is null
     * @throws IllegalArgumentException if the value is invalid
     */
    public static ApiKeyCredentials parse(SecureString apiKeyString, @Nullable String certificateIdentity, ApiKey.Type expectedType) {
        if (apiKeyString != null) {
            if (apiKeyString.length() == 0) {
                throw new IllegalArgumentException("api key is empty");
            }
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(CharArrays.toUtf8Bytes(apiKeyString.getChars()));
            char[] apiKeyCredChars = null;
            try {
                apiKeyCredChars = CharArrays.utf8BytesToChars(decodedApiKeyCredBytes);
                int colonIndex = -1;
                for (int i = 0; i < apiKeyCredChars.length; i++) {
                    if (apiKeyCredChars[i] == ':') {
                        colonIndex = i;
                        break;
                    }
                }

                if (colonIndex < 1) {
                    throw new IllegalArgumentException("invalid ApiKey value");
                }
                final int secretStartPos = colonIndex + 1;
                if (ApiKey.Type.CROSS_CLUSTER == expectedType
                    && CROSS_CLUSTER_API_KEY_SECRET_LENGTH != apiKeyCredChars.length - secretStartPos) {
                    throw new IllegalArgumentException("invalid cross-cluster API key value");
                }
                return new ApiKeyCredentials(
                    new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, secretStartPos, apiKeyCredChars.length)),
                    expectedType,
                    certificateIdentity
                );
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }

    public String getId() {
        return id;
    }

    public SecureString getKey() {
        return key;
    }

    @Override
    public void close() {
        key.close();
    }

    @Override
    public String principal() {
        return id;
    }

    @Override
    public Object credentials() {
        return key;
    }

    @Override
    public void clearCredentials() {
        close();
    }

    public ApiKey.Type getExpectedType() {
        return expectedType;
    }

    /**
     * The identity (Subject DistinguishedName) of the X.509 certificate that was provided by the client
     * alongside the API during authenticate.
     * <em>At the time of writing, the only place where this is used is for cross cluster request signing</em>
     */
    public String getCertificateIdentity() {
        return certificateIdentity;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.io.IOException;
import java.util.Base64;

/**
 * A class from reading/writing {@link org.elasticsearch.xpack.core.security.authc.Authentication} objects to/from a
 * {@link org.elasticsearch.common.util.concurrent.ThreadContext} under a specified key
 */
public class AuthenticationContextSerializer {

    private static final Logger logger = LogManager.getLogger(AuthenticationContextSerializer.class);

    private final String contextKey;

    public AuthenticationContextSerializer() {
        this(AuthenticationField.AUTHENTICATION_KEY);
    }

    public AuthenticationContextSerializer(String contextKey) {
        this.contextKey = contextKey;
    }

    @Nullable
    public Authentication readFromContext(ThreadContext ctx) throws IOException {
        Authentication authentication = ctx.getTransient(contextKey);
        if (authentication != null) {
            assert ctx.getHeader(contextKey) != null;
            return authentication;
        }

        String authenticationHeader = ctx.getHeader(contextKey);
        if (authenticationHeader == null) {
            return null;
        }
        return deserializeHeaderAndPutInContext(authenticationHeader, ctx);
    }

    Authentication deserializeHeaderAndPutInContext(String headerValue, ThreadContext ctx) throws IOException, IllegalArgumentException {
        assert ctx.getTransient(contextKey) == null;

        Authentication authentication = decode(headerValue);
        ctx.putTransient(contextKey, authentication);
        return authentication;
    }

    public static Authentication decode(String header) throws IOException {
        try {
            byte[] bytes = Base64.getDecoder().decode(header);
            StreamInput input = StreamInput.wrap(bytes);
            TransportVersion version = TransportVersion.readVersion(input);
            input.setTransportVersion(version);
            return new Authentication(input);
        } catch (IOException | RuntimeException e) {
            logger.warn("Failed to decode authentication [" + header + "]", e);
            throw e;
        }
    }

    public Authentication getAuthentication(ThreadContext context) {
        return context.getTransient(contextKey);
    }

    /**
     * Writes the authentication to the context. There must not be an existing authentication in the context and if there is an
     * {@link IllegalStateException} will be thrown
     */
    public void writeToContext(Authentication authentication, ThreadContext ctx) throws IOException {
        ensureContextDoesNotContainAuthentication(ctx);
        String header = authentication.encode();
        assert header != null : "Authentication object encoded to null"; // this usually happens with mock objects in tests
        ctx.putTransient(contextKey, authentication);
        ctx.putHeader(contextKey, header);
    }

    void ensureContextDoesNotContainAuthentication(ThreadContext ctx) {
        if (ctx.getTransient(contextKey) != null) {
            if (ctx.getHeader(contextKey) == null) {
                throw new IllegalStateException("authentication present as a transient ([" + contextKey + "]) but not a header");
            }
            throw new IllegalStateException("authentication ([" + contextKey + "]) is already present in the context");
        }
    }
}

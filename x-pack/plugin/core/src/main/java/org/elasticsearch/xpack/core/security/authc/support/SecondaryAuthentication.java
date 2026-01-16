/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Some Elasticsearch APIs need to be provided with 2 sets of credentials.
 * Typically this happens when a system user needs to perform an action while accessing data on behalf of, or user information regarding
 * a logged in user.
 * This class is a representation of that secondary user that can be activated in the security context while processing specific blocks
 * of code or within a listener.
 */
public class SecondaryAuthentication {

    private static final Logger logger = LogManager.getLogger(SecondaryAuthentication.class);

    public static final String THREAD_CTX_KEY = "_xpack_security_secondary_authc";
    /**
     * Prefix for transient keys that should be preserved from the authentication phase into secondary authentication execution.
     */
    public static final String PRESERVABLE_AUTHC_TRANSIENT_PREFIX = "_security_authc_";

    /**
     * Prefix for transient keys stored in the original context that should be restored only during secondary authentication execution.
     */
    public static final String SECONDARY_AUTHC_PRESERVED_TRANSIENT_PREFIX = "_security_secondary_authc_preserved_";

    private final SecurityContext securityContext;
    private final Authentication authentication;

    public SecondaryAuthentication(SecurityContext securityContext, Authentication authentication) {
        this.securityContext = Objects.requireNonNull(securityContext);
        this.authentication = Objects.requireNonNull(authentication);
    }

    @Nullable
    public static SecondaryAuthentication readFromContext(SecurityContext securityContext) throws IOException {
        final Authentication authentication = serializer().readFromContext(securityContext.getThreadContext());
        if (authentication == null) {
            return null;
        }
        return new SecondaryAuthentication(securityContext, authentication);
    }

    public void writeToContext(ThreadContext threadContext) throws IOException {
        serializer().writeToContext(this.authentication, threadContext);
    }

    private static AuthenticationContextSerializer serializer() {
        return new AuthenticationContextSerializer(THREAD_CTX_KEY);
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public User getUser() {
        return authentication.getEffectiveSubject().getUser();
    }

    public <T> T execute(Function<ThreadContext.StoredContext, T> body) {
        final var secondaryAuthTransientHeaders = extractPreservedSecondaryAuthTransients();
        return securityContext.executeWithAuthentication(authentication, secondaryAuthTransientHeaders, body);
    }

    public Runnable wrap(Runnable runnable) {
        return () -> execute(ignore -> {
            runnable.run();
            return null;
        });
    }

    private Map<String, Object> extractPreservedSecondaryAuthTransients() {
        final var transientHeaders = securityContext.getThreadContext().getTransientHeaders();
        if (transientHeaders.isEmpty()) {
            logger.trace("no secondary authc preserved transient headers found");
            return Map.of();
        }
        final Map<String, Object> extracted = new HashMap<>();
        for (var transientHeader : transientHeaders.entrySet()) {
            if (transientHeader.getKey().startsWith(SECONDARY_AUTHC_PRESERVED_TRANSIENT_PREFIX)) {
                final String originalKey = transientHeader.getKey().substring(SECONDARY_AUTHC_PRESERVED_TRANSIENT_PREFIX.length());
                assert originalKey.startsWith(PRESERVABLE_AUTHC_TRANSIENT_PREFIX)
                    : "preserved transient header must start with " + PRESERVABLE_AUTHC_TRANSIENT_PREFIX;
                extracted.put(originalKey, transientHeader.getValue());
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("found secondary authc preserved transient headers: {}", extracted.keySet());
        }
        return Collections.unmodifiableMap(extracted);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + authentication + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SecondaryAuthentication that = (SecondaryAuthentication) o;
        return authentication.equals(that.authentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authentication);
    }
}

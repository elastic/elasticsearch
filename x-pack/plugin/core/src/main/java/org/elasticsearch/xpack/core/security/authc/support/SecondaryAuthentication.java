/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
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

    public static final String THREAD_CTX_KEY = "_xpack_security_secondary_authc";

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
        return authentication.getUser();
    }

    public <T> T execute(Function<ThreadContext.StoredContext, T> body) {
        return this.securityContext.executeWithAuthentication(this.authentication, body);
    }

    public Runnable wrap(Runnable runnable) {
        return () -> execute(ignore -> {
            runnable.run();
            return null;
        });
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContextTransient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
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

    public static final String THREAD_CTX_KEY = "_xpack_security_secondary_authc";
    private static final AuthenticationContextSerializer serializer = new AuthenticationContextSerializer(THREAD_CTX_KEY);

    static final ThreadContextTransient<Map<String, Object>> CAPTURED_TRANSIENT_HEADERS = mapTransientValue(
        "_security_secondary_authc_captured_transient_headers"
    );

    @SuppressWarnings("unchecked")
    private static <K, V> ThreadContextTransient<Map<K, V>> mapTransientValue(String key) {
        return (ThreadContextTransient<Map<K, V>>) (ThreadContextTransient<?>) ThreadContextTransient.transientValue(key, Map.class);
    }

    private final SecurityContext securityContext;
    private final Authentication authentication;
    private final Map<String, Object> transientHeaders;

    public SecondaryAuthentication(SecurityContext securityContext, Authentication authentication, Map<String, Object> transientHeaders) {
        this.securityContext = Objects.requireNonNull(securityContext);
        this.authentication = Objects.requireNonNull(authentication);
        this.transientHeaders = Map.copyOf(Objects.requireNonNull(transientHeaders));
    }

    @Nullable
    public static SecondaryAuthentication readFromContext(SecurityContext securityContext) throws IOException {
        final Authentication authentication = serializer.readFromContext(securityContext.getThreadContext());
        if (authentication == null) {
            assert CAPTURED_TRANSIENT_HEADERS.get(securityContext.getThreadContext()) == null
                : "captured transient headers present without secondary authentication";
            return null;
        }
        final Map<String, Object> captured = CAPTURED_TRANSIENT_HEADERS.get(securityContext.getThreadContext());
        return new SecondaryAuthentication(securityContext, authentication, captured != null ? captured : Map.of());
    }

    public void writeToContext(ThreadContext threadContext) throws IOException {
        serializer.writeToContext(this.authentication, threadContext);
        if (!transientHeaders.isEmpty()) {
            CAPTURED_TRANSIENT_HEADERS.set(threadContext, transientHeaders);
        }
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    public Map<String, Object> getTransientHeaders() {
        return transientHeaders;
    }

    public User getUser() {
        return authentication.getEffectiveSubject().getUser();
    }

    public <T> T execute(Function<ThreadContext.StoredContext, T> body) {
        return this.securityContext.executeWithSecondaryAuthentication(this, body);
    }

    public Runnable wrap(Runnable runnable) {
        return () -> execute(ignore -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + authentication + ", transientHeaders=" + transientHeaders.keySet() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SecondaryAuthentication that = (SecondaryAuthentication) o;
        return authentication.equals(that.authentication) && transientHeaders.equals(that.transientHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authentication, transientHeaders);
    }

}

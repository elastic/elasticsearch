/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A lightweight utility that can find the current user and authentication information for the local thread.
 */
public class SecurityContext {
    private final Logger logger = LogManager.getLogger(SecurityContext.class);

    private final ThreadContext threadContext;
    private final UserSettings userSettings;
    private final String nodeName;

    /**
     * Creates a new security context.
     * If cryptoService is null, security is disabled and {@link UserSettings#getUser()}
     * and {@link UserSettings#getAuthentication()} will always return null.
     */
    public SecurityContext(Settings settings, ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.userSettings = new UserSettings(threadContext);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    /** Returns the current user information, or null if the current request has no authentication info. */
    public User getUser() {
        Authentication authentication = getAuthentication();
        return authentication == null ? null : authentication.getUser();
    }

    /** Returns the authentication information, or null if the current request has no authentication info. */
    public Authentication getAuthentication() {
        try {
            return Authentication.readFromContext(threadContext);
        } catch (IOException e) {
            logger.error("failed to read authentication", e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Sets the user forcefully to the provided user. There must not be an existing user in the ThreadContext otherwise an exception
     * will be thrown. This method is package private for testing.
     */
    public void setUser(User user, Version version) {
        Objects.requireNonNull(user);
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("__attach", "__attach", nodeName);
        final Authentication.RealmRef lookedUpBy;
        if (user.isRunAs()) {
            lookedUpBy = authenticatedBy;
        } else {
            lookedUpBy = null;
        }
        setAuthentication(
            new Authentication(user, authenticatedBy, lookedUpBy, version, AuthenticationType.INTERNAL, Collections.emptyMap()));
    }

    /** Writes the authentication to the thread context */
    private void setAuthentication(Authentication authentication) {
        try {
            authentication.writeToContext(threadContext);
        } catch (IOException e) {
            throw new AssertionError("how can we have a IOException with a user we set", e);
        }
    }

    /**
     * Runs the consumer in a new context as the provided user. The original context is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public void executeAsUser(User user, Consumer<StoredContext> consumer, Version version) {
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setUser(user, version);
            consumer.accept(original);
        }
    }

    /**
     * Runs the consumer in a new context after setting a new version of the authentication that is compatible with the version provided.
     * The original context is provided to the consumer. When this method returns, the original context is restored.
     */
    public void executeAfterRewritingAuthentication(Consumer<StoredContext> consumer, Version version) {
        final StoredContext original = threadContext.newStoredContext(true);
        final Authentication authentication = Objects.requireNonNull(userSettings.getAuthentication());
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(new Authentication(authentication.getUser(), authentication.getAuthenticatedBy(),
                authentication.getLookedUpBy(), version, authentication.getAuthenticationType(), authentication.getMetadata()));
            consumer.accept(original);
        }
    }
}

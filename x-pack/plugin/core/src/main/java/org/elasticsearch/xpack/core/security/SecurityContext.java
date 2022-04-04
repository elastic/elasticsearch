/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.SecondaryAuthentication;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;

/**
 * A lightweight utility that can find the current user and authentication information for the local thread.
 */
public class SecurityContext {

    private static final Logger logger = LogManager.getLogger(SecurityContext.class);

    private final ThreadContext threadContext;
    private final AuthenticationContextSerializer authenticationSerializer;
    private final String nodeName;

    public SecurityContext(Settings settings, ThreadContext threadContext) {
        this.threadContext = threadContext;
        this.authenticationSerializer = new AuthenticationContextSerializer();
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    /**
     * Returns the current user information, or throws {@link org.elasticsearch.ElasticsearchSecurityException}
     * if the current request has no authentication information.
     */
    public User requireUser() {
        User user = getUser();
        if (user == null) {
            throw new ElasticsearchSecurityException("there is no user available in the current context");
        }
        return user;
    }

    /** Returns the current user information, or null if the current request has no authentication info. */
    @Nullable
    public User getUser() {
        Authentication authentication = getAuthentication();
        return authentication == null ? null : authentication.getUser();
    }

    /** Returns the authentication information, or null if the current request has no authentication info. */
    @Nullable
    public Authentication getAuthentication() {
        try {
            return authenticationSerializer.readFromContext(threadContext);
        } catch (IOException e) {
            logger.error("failed to read authentication", e);
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the "secondary authentication" (see {@link SecondaryAuthentication}) information,
     * or {@code null} if the current request does not have a secondary authentication context
     */
    public SecondaryAuthentication getSecondaryAuthentication() {
        try {
            return SecondaryAuthentication.readFromContext(this);
        } catch (IOException e) {
            logger.error("failed to read secondary authentication", e);
            throw new UncheckedIOException(e);
        }
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    /**
     * Sets the user forcefully to the provided user. There must not be an existing user in the ThreadContext otherwise an exception
     * will be thrown. This method is package private for testing.
     */
    public void setInternalUser(User internalUser, Version version) {
        assert User.isInternal(internalUser);
        setAuthentication(Authentication.newInternalAuthentication(internalUser, version, nodeName));
    }

    /**
     * Runs the consumer in a new context as the provided user. The original context is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public void executeAsInternalUser(User internalUser, Version version, Consumer<StoredContext> consumer) {
        assert User.isInternal(internalUser);
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setInternalUser(internalUser, version);
            consumer.accept(original);
        }
    }

    public void executeAsSystemUser(Consumer<StoredContext> consumer) {
        executeAsSystemUser(Version.CURRENT, consumer);
    }

    public void executeAsSystemUser(Version version, Consumer<StoredContext> consumer) {
        executeAsInternalUser(SystemUser.INSTANCE, version, consumer);
    }

    /**
     * Runs the consumer in a new context as the provided user. The original context is provided to the consumer. When this method
     * returns, the original context is restored.
     */
    public <T> T executeWithAuthentication(Authentication authentication, Function<StoredContext, T> consumer) {
        final StoredContext original = threadContext.newStoredContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(authentication);
            return consumer.apply(original);
        }
    }

    /**
     * Runs the consumer in a new context after setting a new version of the authentication that is compatible with the version provided.
     * The original context is provided to the consumer. When this method returns, the original context is restored.
     */
    public void executeAfterRewritingAuthentication(Consumer<StoredContext> consumer, Version version) {
        // Preserve request headers other than authentication
        final Map<String, String> existingRequestHeaders = threadContext.getRequestHeadersOnly();
        final StoredContext original = threadContext.newStoredContext(true);
        final Authentication authentication = getAuthentication();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            setAuthentication(authentication.maybeRewriteForOlderVersion(version));
            existingRequestHeaders.forEach((k, v) -> {
                if (threadContext.getHeader(k) == null) {
                    threadContext.putHeader(k, v);
                }
            });
            consumer.accept(original);
        }
    }

    /**
     * Checks whether the user or API key of the passed in authentication can access the resources owned by the user
     * or API key of this authentication. The rules are as follows:
     *   * True if the authentications are for the same API key (same API key ID)
     *   * True if they are the same username from the same realm
     *      - For file and native realm, same realm means the same realm type
     *      - For all other realms, same realm means same realm type plus same realm name
     *   * An user and its API key cannot access each other's resources
     *   * An user and its token can access each other's resources
     *   * Two API keys are never able to access each other's resources regardless of their ownership.
     *
     *  This check is a best effort and it does not account for certain static and external changes.
     *  See also <a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/security-limitations.html">
     *      security limitations</a>
     */
    public boolean canIAccessResourcesCreatedBy(@Nullable Authentication resourceCreatorAuthentication) {
        if (resourceCreatorAuthentication == null) {
            // resource creation was not authenticated (security was disabled); anyone can access such resources
            return true;
        }
        final Authentication myAuthentication = getAuthentication();
        if (myAuthentication == null) {
            // unauthenticated users cannot access any resources created by authenticated users, even anonymously authenticated ones
            return false;
        }
        return myAuthentication.canAccessResourcesOf(resourceCreatorAuthentication);
    }

    public boolean canIAccessResourcesCreatedWithHeaders(Map<String, String> resourceCreateRequestHeaders) throws IOException {
        Authentication resourceCreatorAuthentication = null;
        if (resourceCreateRequestHeaders != null && resourceCreateRequestHeaders.containsKey(AUTHENTICATION_KEY)) {
            resourceCreatorAuthentication = AuthenticationContextSerializer.decode(resourceCreateRequestHeaders.get(AUTHENTICATION_KEY));
        }
        return canIAccessResourcesCreatedBy(resourceCreatorAuthentication);
    }

    /** Writes the authentication to the thread context */
    private void setAuthentication(Authentication authentication) {
        try {
            authentication.writeToContext(threadContext);
        } catch (IOException e) {
            throw new AssertionError("how can we have a IOException with a user we set", e);
        }
    }
}

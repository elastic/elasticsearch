/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The Authenticator interface represents an authentication mechanism or a group of similar authentication mechanisms.
 */
public interface Authenticator {

    /**
     * A descriptive name of the authenticator.
     */
    String name();

    /**
     * Attempt to Extract an {@link AuthenticationToken} from the given {@link Context}.
     * @param context The context object encapsulating current request and other information relevant for authentication.
     *
     * @return An {@link AuthenticationToken} if one can be extracted or null if this Authenticator cannot
     *         extract one.
     */
    @Nullable
    AuthenticationToken extractCredentials(Context context);

    /**
     * Whether authentication with anonymous or fallback user is allowed after this authenticator.
     */
    default boolean canBeFollowedByNullTokenHandler() {
        return true;
    }

    /**
     * Attempt to authenticate current request encapsulated by the {@link Context} object.
     * @param context The context object encapsulating current request and other information relevant for authentication.
     * @param listener The listener accepts a {@link AuthenticationResult} object indicating the outcome of authentication.
     */
    void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener);

    static SecureString extractCredentialFromAuthorizationHeader(ThreadContext threadContext, String prefix) {
        return extractCredentialFromHeaderValue(threadContext.getHeader("Authorization"), prefix);
    }

    static SecureString extractCredentialFromHeaderValue(String header, String prefix) {
        final String prefixWithSpace = prefix + " ";
        if (Strings.hasText(header)
            && header.regionMatches(true, 0, prefixWithSpace, 0, prefixWithSpace.length())
            && header.length() > prefixWithSpace.length()) {
            char[] chars = new char[header.length() - prefixWithSpace.length()];
            header.getChars(prefixWithSpace.length(), header.length(), chars, 0);
            return new SecureString(chars);
        }
        return null;
    }

    /**
     * Gets the token from the <code>Authorization</code> header if the header begins with
     * <code>Bearer </code>
     */
    static SecureString extractBearerTokenFromHeader(ThreadContext threadContext) {
        return extractCredentialFromAuthorizationHeader(threadContext, "Bearer");
    }

    /**
     * This class is a container to encapsulate the current request and other necessary information (mostly configuration related)
     * required for authentication.
     * It is instantiated for every incoming request and passed around to {@link AuthenticatorChain} and subsequently all
     * {@link Authenticator}.
     */
    class Context implements Closeable {
        private final ThreadContext threadContext;
        private final AuthenticationService.AuditableRequest request;
        private final User fallbackUser;
        private final boolean allowAnonymous;
        private final Realms realms;
        private final List<AuthenticationToken> authenticationTokens = new ArrayList<>();
        private final List<String> unsuccessfulMessages = new ArrayList<>();
        private boolean handleNullToken = true;
        private SecureString bearerString = null;
        private List<Realm> defaultOrderedRealmList = null;
        private List<Realm> unlicensedRealms = null;

        public Context(
            ThreadContext threadContext,
            AuthenticationService.AuditableRequest request,
            User fallbackUser,
            boolean allowAnonymous,
            Realms realms
        ) {
            this.threadContext = threadContext;
            this.request = request;
            this.fallbackUser = fallbackUser;
            this.allowAnonymous = allowAnonymous;
            this.realms = realms;
        }

        public ThreadContext getThreadContext() {
            return threadContext;
        }

        public AuthenticationService.AuditableRequest getRequest() {
            return request;
        }

        public User getFallbackUser() {
            return fallbackUser;
        }

        public boolean isAllowAnonymous() {
            return allowAnonymous;
        }

        public void setHandleNullToken(boolean value) {
            handleNullToken = value;
        }

        public boolean shouldHandleNullToken() {
            return handleNullToken;
        }

        public List<String> getUnsuccessfulMessages() {
            return unsuccessfulMessages;
        }

        public void addAuthenticationToken(AuthenticationToken authenticationToken) {
            authenticationTokens.add(authenticationToken);
        }

        @Nullable
        public AuthenticationToken getMostRecentAuthenticationToken() {
            return authenticationTokens.isEmpty() ? null : authenticationTokens.get(authenticationTokens.size() - 1);
        }

        public SecureString getBearerString() {
            if (bearerString == null) {
                bearerString = extractBearerTokenFromHeader(threadContext);
            }
            return bearerString;
        }

        public List<Realm> getDefaultOrderedRealmList() {
            if (defaultOrderedRealmList == null) {
                defaultOrderedRealmList = realms.getActiveRealms();
            }
            return defaultOrderedRealmList;
        }

        public List<Realm> getUnlicensedRealms() {
            if (unlicensedRealms == null) {
                unlicensedRealms = realms.getUnlicensedRealms();
            }
            return unlicensedRealms;
        }

        public void addUnsuccessfulMessage(String message) {
            unsuccessfulMessages.add(message);
        }

        @Override
        public void close() throws IOException {
            authenticationTokens.forEach(AuthenticationToken::clearCredentials);
        }

        public void addUnsuccessfulMessageToMetadata(final ElasticsearchSecurityException ese) {
            if (false == getUnsuccessfulMessages().isEmpty()) {
                ese.addMetadata("es.additional_unsuccessful_credentials", getUnsuccessfulMessages());
            }
        }
    }
}

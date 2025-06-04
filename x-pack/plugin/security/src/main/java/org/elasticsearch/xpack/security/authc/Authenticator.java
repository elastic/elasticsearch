/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchSecurityException;
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

    static SecureString extractApiKeyFromHeader(ThreadContext threadContext) {
        return extractCredentialFromAuthorizationHeader(threadContext, "ApiKey");
    }

    /**
     * This class is a container to encapsulate the current request and other necessary information (mostly configuration related)
     * required for authentication.
     * It is instantiated for every incoming request and passed around to {@link AuthenticatorChain} and subsequently all
     * {@link Authenticator}.
     * {@link Authenticator}s are consulted in order (see {@link AuthenticatorChain}),
     * where each is given the chance to first extract some token, and then to verify it.
     * If token verification fails in some particular way (i.e. {@code AuthenticationResult.Status.CONTINUE}),
     * the next {@link Authenticator} is tried.
     * The extracted tokens are all appended with {@link #addAuthenticationToken(AuthenticationToken)}.
     */
    class Context implements Closeable {
        private final ThreadContext threadContext;
        private final AuthenticationService.AuditableRequest request;
        private final User fallbackUser;
        private final boolean allowAnonymous;
        private final boolean extractCredentials;
        private final Realms realms;
        private final List<AuthenticationToken> authenticationTokens;
        private final List<String> unsuccessfulMessages = new ArrayList<>();
        private boolean handleNullToken = true;
        private SecureString bearerString = null;
        private SecureString apiKeyString = null;
        private List<Realm> defaultOrderedRealmList = null;
        private List<Realm> unlicensedRealms = null;

        /**
         * Context constructor that provides the authentication token directly as an argument.
         * This avoids extracting any tokens from the thread context, which is the regular way that authn works.
         * In this case, the authentication process will simply verify the provided token, and will never fall back to the null-token case
         * (i.e. in case the token CAN NOT be verified, the user IS NOT authenticated as the anonymous or the fallback user, and
         * instead the authentication process fails, see {@link AuthenticatorChain#doAuthenticate}). If a {@code null} token is provided
         * the authentication will invariably fail.
         */
        Context(
            ThreadContext threadContext,
            AuthenticationService.AuditableRequest request,
            Realms realms,
            @Nullable AuthenticationToken token
        ) {
            this.threadContext = threadContext;
            this.request = request;
            this.realms = realms;
            // when a token is directly supplied for authn, don't extract other tokens, and don't handle the null-token case
            this.authenticationTokens = token != null ? List.of(token) : List.of(); // no other tokens should be added
            this.extractCredentials = false;
            this.handleNullToken = false;
            // if handleNullToken is false, fallbackUser and allowAnonymous are irrelevant
            this.fallbackUser = null;
            this.allowAnonymous = false;
        }

        /**
         * Context constructor where authentication looks for credentials in the thread context.
         */
        public Context(
            ThreadContext threadContext,
            AuthenticationService.AuditableRequest request,
            User fallbackUser,
            boolean allowAnonymous,
            Realms realms
        ) {
            this.threadContext = threadContext;
            this.request = request;
            this.extractCredentials = true;
            // the extracted tokens, in order, for each {@code Authenticator}
            this.authenticationTokens = new ArrayList<>();
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

        /**
         * Returns {@code true}, if {@code Authenticator}s should first be tried in order to extract the credentials token
         * from the thread context. The extracted tokens are appended to this authenticator context with
         * {@link #addAuthenticationToken(AuthenticationToken)}.
         * If {@code false}, the credentials token is directly passed in to this authenticator context, and the authenticators
         * themselves are only consulted to authenticate the token, and never to extract any tokens from the thread context.
         */
        public boolean shouldExtractCredentials() {
            return extractCredentials;
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

        public SecureString getApiKeyString() {
            if (apiKeyString == null) {
                apiKeyString = extractApiKeyFromHeader(threadContext);
            }
            return apiKeyString;
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

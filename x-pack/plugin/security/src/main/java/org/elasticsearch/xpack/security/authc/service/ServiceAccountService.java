/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.support.SecurityTokenType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts.ACCOUNTS;

public class ServiceAccountService {

    public static final String REALM_TYPE = "service_account";
    public static final String REALM_NAME = "service_account";
    public static final Version VERSION_MINIMUM = Version.V_8_0_0;

    private static final Logger logger = LogManager.getLogger(ServiceAccountService.class);

    private final ServiceAccountsTokenStore serviceAccountsTokenStore;

    public ServiceAccountService(ServiceAccountsTokenStore serviceAccountsTokenStore) {
        this.serviceAccountsTokenStore = serviceAccountsTokenStore;
    }

    public static boolean isServiceAccount(Authentication authentication) {
        return REALM_TYPE.equals(authentication.getAuthenticatedBy().getType()) && null == authentication.getLookedUpBy();
    }

    public static boolean isServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static Collection<String> getServiceAccountPrincipals() {
        return ACCOUNTS.keySet();
    }

    /**
     * Parses a token object from the content of a {@link ServiceAccountToken#asBearerString()} bearer string}.
     * This bearer string would typically be
     * {@link org.elasticsearch.xpack.security.authc.TokenService#extractBearerTokenFromHeader extracted} from an HTTP authorization header.
     *
     * <p>
     * <strong>This method does not validate the credential, it simply parses it.</strong>
     * There is no guarantee that the {@link ServiceAccountToken#getSecret() secret} is valid,
     * or even that the {@link ServiceAccountToken#getAccountId() account} exists.
     * </p>
     * @param token A raw token string (if this is from an HTTP header, then the <code>"Bearer "</code> prefix must be removed before
     *              calling this method.
     * @return An unvalidated token object.
     */
    public static ServiceAccountToken tryParseToken(SecureString token) {
        try {
            if (token == null) {
                return null;
            }
            return doParseToken(token);
        } catch (IOException e) {
            logger.debug("Cannot parse possible service account token", e);
            return null;
        }
    }

    public void tryAuthenticateBearerToken(SecureString bearerToken, String nodeName, ActionListener<Authentication> listener) {
        final ServiceAccountToken serviceAccountToken = ServiceAccountService.tryParseToken(bearerToken);
        if (serviceAccountToken == null) {
            // This should be the only situation where a null is returned to mean the authentication should continue.
            // For all other situations, it should be either onResponse(authentication) for success or onFailure for any error.
            listener.onResponse(null);
        } else {
            authenticateToken(serviceAccountToken, nodeName, listener);
        }
    }

    public void authenticateToken(ServiceAccountToken serviceAccountToken, String nodeName, ActionListener<Authentication> listener) {

        if (ElasticServiceAccounts.NAMESPACE.equals(serviceAccountToken.getAccountId().namespace()) == false) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "only [{}] service accounts are supported, but received [{}]",
                ElasticServiceAccounts.NAMESPACE, serviceAccountToken.getAccountId().asPrincipal());
            logger.debug(message);
            listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage(), RestStatus.UNAUTHORIZED));
            return;
        }

        final ServiceAccount account = ACCOUNTS.get(serviceAccountToken.getAccountId().asPrincipal());
        if (account == null) {
            final ParameterizedMessage message = new ParameterizedMessage(
                "the [{}] service account does not exist", serviceAccountToken.getAccountId().asPrincipal());
            logger.debug(message);
            listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage(), RestStatus.UNAUTHORIZED));
            return;
        }

        serviceAccountsTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(success -> {
            if (success) {
                listener.onResponse(createAuthentication(account, serviceAccountToken, nodeName));
            } else {
                final ParameterizedMessage message = new ParameterizedMessage(
                    "failed to authenticate service account [{}] with token name [{}]",
                    serviceAccountToken.getAccountId().asPrincipal(),
                    serviceAccountToken.getTokenName());
                logger.debug(message);
                listener.onFailure(new ElasticsearchSecurityException(message.getFormattedMessage(), RestStatus.UNAUTHORIZED));
            }
        }, listener::onFailure));
    }

    public void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert isServiceAccount(authentication) : "authentication is not for service account: " + authentication;

        final String principal = authentication.getUser().principal();
        final ServiceAccount account = ACCOUNTS.get(principal);
        if (account == null) {
            listener.onFailure(new ElasticsearchSecurityException(
                "cannot load role for service account [" + principal + "] - no such service account"
            ));
            return;
        }
        listener.onResponse(account.roleDescriptor());
    }

    private Authentication createAuthentication(ServiceAccount account, ServiceAccountToken token, String nodeName) {
        final User user = account.asUser();
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef(REALM_NAME, REALM_TYPE, nodeName);
        return new Authentication(user, authenticatedBy, null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
            Map.of("_token_name", token.getTokenName()));
    }

    private static ServiceAccountToken doParseToken(SecureString token) throws IOException {
        final byte[] bytes = CharArrays.toUtf8Bytes(token.getChars());
        logger.trace("parsing token bytes {}", MessageDigests.toHexString(bytes));
        try (StreamInput in = new InputStreamStreamInput(Base64.getDecoder().wrap(new ByteArrayInputStream(bytes)), bytes.length)) {
            final Version version = Version.readVersion(in);
            in.setVersion(version);
            if (version.before(VERSION_MINIMUM)) {
                logger.trace("token has version {}, but we need at least {}", version, VERSION_MINIMUM);
                return null;
            }
            final SecurityTokenType tokenType = SecurityTokenType.read(in);
            if (tokenType != SecurityTokenType.SERVICE_ACCOUNT) {
                logger.trace("token is of type {}, but we only handle {}", tokenType, SecurityTokenType.SERVICE_ACCOUNT);
                return null;
            }

            final ServiceAccountId account = new ServiceAccountId(in);
            final String tokenName = in.readString();
            final SecureString secret = in.readSecureString();

            return new ServiceAccountToken(account, tokenName, secret);
        }
    }

}

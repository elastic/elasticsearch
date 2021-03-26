/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts.ACCOUNTS;

public class ServiceAccountService {

    public static final String REALM_TYPE = "service_account";
    public static final String REALM_NAME = "service_account";

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
     * @param bearerString A raw token string (if this is from an HTTP header, then the <code>"Bearer "</code> prefix must be removed before
     *              calling this method.
     * @return An unvalidated token object.
     */
    public static ServiceAccountToken tryParseToken(SecureString bearerString) {
        try {
            if (bearerString == null) {
                return null;
            }
            return ServiceAccountToken.fromBearerString(bearerString);
        } catch (Exception e) {
            logger.trace("Cannot parse possible service account token", e);
            return null;
        }
    }

    public void authenticateToken(ServiceAccountToken serviceAccountToken, String nodeName, ActionListener<Authentication> listener) {
        logger.trace("attempt to authenticate service account token [{}]", serviceAccountToken.getQualifiedName());
        if (ElasticServiceAccounts.NAMESPACE.equals(serviceAccountToken.getAccountId().namespace()) == false) {
            logger.debug("only [{}] service accounts are supported, but received [{}]",
                ElasticServiceAccounts.NAMESPACE, serviceAccountToken.getAccountId().asPrincipal());
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        final ServiceAccount account = ACCOUNTS.get(serviceAccountToken.getAccountId().asPrincipal());
        if (account == null) {
            logger.debug("the [{}] service account does not exist", serviceAccountToken.getAccountId().asPrincipal());
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        serviceAccountsTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(success -> {
            if (success) {
                listener.onResponse(createAuthentication(account, serviceAccountToken, nodeName));
            } else {
                final ElasticsearchSecurityException e = createAuthenticationException(serviceAccountToken);
                logger.debug(e.getMessage());
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    public void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert isServiceAccount(authentication) : "authentication is not for service account: " + authentication;

        final String principal = authentication.getUser().principal();
        final ServiceAccount account = ACCOUNTS.get(principal);
        if (account == null) {
            listener.onFailure(new ElasticsearchSecurityException(
                "cannot load role for service account [" + principal + "] - no such service account"));
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

    private ElasticsearchSecurityException createAuthenticationException(ServiceAccountToken serviceAccountToken) {
        return new ElasticsearchSecurityException("failed to authenticate service account [{}] with token name [{}]",
            RestStatus.UNAUTHORIZED,
            serviceAccountToken.getAccountId().asPrincipal(),
            serviceAccountToken.getTokenName());
    }
}

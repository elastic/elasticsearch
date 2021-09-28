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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsNodesRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountCredentialsResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountNodesCredentialsAction;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo;
import org.elasticsearch.xpack.core.security.action.service.TokenInfo.TokenSource;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_NAME_FIELD;
import static org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings.TOKEN_SOURCE_FIELD;
import static org.elasticsearch.xpack.security.authc.service.ElasticServiceAccounts.ACCOUNTS;

public class ServiceAccountService {

    private static final Logger logger = LogManager.getLogger(ServiceAccountService.class);
    private static final int MIN_TOKEN_SECRET_LENGTH = 10;

    private final Client client;
    private final IndexServiceAccountTokenStore indexServiceAccountTokenStore;
    private final CompositeServiceAccountTokenStore compositeServiceAccountTokenStore;

    public ServiceAccountService(Client client,
                                 FileServiceAccountTokenStore fileServiceAccountTokenStore,
                                 IndexServiceAccountTokenStore indexServiceAccountTokenStore) {
        this.client = client;
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.compositeServiceAccountTokenStore = new CompositeServiceAccountTokenStore(
            List.of(fileServiceAccountTokenStore, indexServiceAccountTokenStore), client.threadPool().getThreadContext());
    }

    public static boolean isServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static Collection<String> getServiceAccountPrincipals() {
        return ACCOUNTS.keySet();
    }

    public static Map<String, ServiceAccount> getServiceAccounts() {
        return Map.copyOf(ACCOUNTS);
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
            logger.debug(
                "only [{}] service accounts are supported, but received [{}]",
                ElasticServiceAccounts.NAMESPACE,
                serviceAccountToken.getAccountId().asPrincipal()
            );
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        final ServiceAccount account = ACCOUNTS.get(serviceAccountToken.getAccountId().asPrincipal());
        if (account == null) {
            logger.debug("the [{}] service account does not exist", serviceAccountToken.getAccountId().asPrincipal());
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        if (serviceAccountToken.getSecret().length() < MIN_TOKEN_SECRET_LENGTH) {
            logger.debug(
                "failing authentication for service account token [{}],"
                    + " the provided credential has length [{}]"
                    + " but a token's secret value must be at least [{}] characters",
                serviceAccountToken.getQualifiedName(),
                serviceAccountToken.getSecret().length(),
                MIN_TOKEN_SECRET_LENGTH
            );
            listener.onFailure(createAuthenticationException(serviceAccountToken));
            return;
        }

        compositeServiceAccountTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(storeAuthenticationResult -> {
            if (storeAuthenticationResult.isSuccess()) {
                listener.onResponse(
                    createAuthentication(account, serviceAccountToken, storeAuthenticationResult.getTokenSource(), nodeName)
                );
            } else {
                final ElasticsearchSecurityException e = createAuthenticationException(serviceAccountToken);
                logger.debug(e.getMessage());
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    public void createIndexToken(Authentication authentication, CreateServiceAccountTokenRequest request,
                                 ActionListener<CreateServiceAccountTokenResponse> listener) {
        indexServiceAccountTokenStore.createToken(authentication, request, listener);
    }

    public void deleteIndexToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
       indexServiceAccountTokenStore.deleteToken(request, listener);
    }

    public void findTokensFor(GetServiceAccountCredentialsRequest request,
                              ActionListener<GetServiceAccountCredentialsResponse> listener) {
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        findIndexTokens(accountId, listener);
    }

    public void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert authentication.isServiceAccount() : "authentication is not for service account: " + authentication;
        final String principal = authentication.getUser().principal();
        final ServiceAccount account = ACCOUNTS.get(principal);
        if (account == null) {
            listener.onFailure(
                new ElasticsearchSecurityException("cannot load role for service account [" + principal + "] - no such service account")
            );
            return;
        }
        listener.onResponse(account.roleDescriptor());
    }

    private Authentication createAuthentication(ServiceAccount account, ServiceAccountToken token, TokenSource tokenSource,
                                                String nodeName) {
        final User user = account.asUser();
        final Authentication.RealmRef authenticatedBy =
            new Authentication.RealmRef(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE, nodeName);
        return new Authentication(user, authenticatedBy, null, Version.CURRENT, Authentication.AuthenticationType.TOKEN,
            Map.of(TOKEN_NAME_FIELD, token.getTokenName(), TOKEN_SOURCE_FIELD, tokenSource.name().toLowerCase(Locale.ROOT)));
    }

    private ElasticsearchSecurityException createAuthenticationException(ServiceAccountToken serviceAccountToken) {
        return new ElasticsearchSecurityException("failed to authenticate service account [{}] with token name [{}]",
            RestStatus.UNAUTHORIZED,
            serviceAccountToken.getAccountId().asPrincipal(),
            serviceAccountToken.getTokenName());
    }

    private void findIndexTokens(ServiceAccountId accountId, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        indexServiceAccountTokenStore.findTokensFor(accountId, ActionListener.wrap(indexTokenInfos -> {
            findFileTokens(indexTokenInfos, accountId, listener);
        }, listener::onFailure));
    }

    private void findFileTokens( Collection<TokenInfo> indexTokenInfos,
                                 ServiceAccountId accountId,
                                 ActionListener<GetServiceAccountCredentialsResponse> listener) {
        executeAsyncWithOrigin(client, SECURITY_ORIGIN,
            GetServiceAccountNodesCredentialsAction.INSTANCE,
            new GetServiceAccountCredentialsNodesRequest(accountId.namespace(), accountId.serviceName()),
            ActionListener.wrap(fileTokensResponse -> listener.onResponse(
                new GetServiceAccountCredentialsResponse(accountId.asPrincipal(), indexTokenInfos, fileTokensResponse)),
                listener::onFailure));
    }
}

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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountTokenStore;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
    private final ServiceAccountTokenStore readOnlyServiceAccountTokenStore;
    @Nullable
    private final IndexUserServiceAccountStore indexUserServiceAccountStore;

    public ServiceAccountService(Client client, ServiceAccountTokenStore readOnlyServiceAccountTokenStore) {
        this(client, readOnlyServiceAccountTokenStore, null, null);
    }

    public ServiceAccountService(
        Client client,
        ServiceAccountTokenStore readOnlyServiceAccountTokenStore,
        @Nullable IndexServiceAccountTokenStore indexServiceAccountTokenStore
    ) {
        this(client, readOnlyServiceAccountTokenStore, indexServiceAccountTokenStore, null);
    }

    public ServiceAccountService(
        Client client,
        ServiceAccountTokenStore readOnlyServiceAccountTokenStore,
        @Nullable IndexServiceAccountTokenStore indexServiceAccountTokenStore,
        @Nullable IndexUserServiceAccountStore indexUserServiceAccountStore
    ) {
        this.client = client;
        this.readOnlyServiceAccountTokenStore = readOnlyServiceAccountTokenStore;
        this.indexServiceAccountTokenStore = indexServiceAccountTokenStore;
        this.indexUserServiceAccountStore = indexUserServiceAccountStore;
    }

    public static boolean isBuiltInServiceAccountPrincipal(String principal) {
        return ACCOUNTS.containsKey(principal);
    }

    public static Collection<String> getServiceAccountPrincipals() {
        return ACCOUNTS.keySet();
    }

    public static Map<String, ServiceAccount> getServiceAccounts() {
        return Map.copyOf(ACCOUNTS);
    }

    @Nullable
    public IndexUserServiceAccountStore getIndexUserServiceAccountStore() {
        return indexUserServiceAccountStore;
    }

    /**
     * Parses a token object from the content of a {@link ServiceAccountToken#asBearerString()} bearer string}.
     * This bearer string would typically be extracted from an HTTP authorization header.
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

        resolveAccount(serviceAccountToken.getAccountId().asPrincipal(), ActionListener.wrap(account -> {
            if (account == null) {
                logger.debug("the [{}] service account does not exist", serviceAccountToken.getAccountId().asPrincipal());
                listener.onFailure(createAuthenticationException(serviceAccountToken));
                return;
            }
            readOnlyServiceAccountTokenStore.authenticate(serviceAccountToken, ActionListener.wrap(storeAuthenticationResult -> {
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
        }, listener::onFailure));
    }

    /**
     * Resolve the named service account (built-in or user-defined) to a {@link ServiceAccount} instance,
     * or {@code null} if no such account exists. Built-in accounts take precedence over user-defined ones
     * with the same principal (which can only happen if a user-defined account was somehow indexed under
     * the {@code elastic} namespace; the put API rejects that).
     */
    public void resolveAccount(String principal, ActionListener<ServiceAccount> listener) {
        final ServiceAccount builtIn = ACCOUNTS.get(principal);
        if (builtIn != null) {
            listener.onResponse(builtIn);
            return;
        }
        if (indexUserServiceAccountStore == null) {
            listener.onResponse(null);
            return;
        }
        indexUserServiceAccountStore.getAccount(principal, ActionListener.wrap(roleDescriptor -> {
            if (roleDescriptor == null) {
                listener.onResponse(null);
            } else {
                listener.onResponse(new UserDefinedServiceAccount(ServiceAccountId.fromPrincipal(principal), roleDescriptor));
            }
        }, listener::onFailure));
    }

    public void createIndexToken(
        Authentication authentication,
        CreateServiceAccountTokenRequest request,
        ActionListener<CreateServiceAccountTokenResponse> listener
    ) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't create token because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        resolveAccount(accountId.asPrincipal(), ActionListener.wrap(account -> {
            if (account == null) {
                listener.onFailure(new IllegalArgumentException("service account [" + accountId + "] does not exist"));
                return;
            }
            indexServiceAccountTokenStore.createToken(authentication, request, listener);
        }, listener::onFailure));
    }

    public void deleteIndexToken(DeleteServiceAccountTokenRequest request, ActionListener<Boolean> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't delete token because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        resolveAccount(accountId.asPrincipal(), ActionListener.wrap(account -> {
            if (account == null) {
                listener.onResponse(false);
                return;
            }
            indexServiceAccountTokenStore.deleteToken(request, listener);
        }, listener::onFailure));
    }

    public void findTokensFor(GetServiceAccountCredentialsRequest request, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        if (indexServiceAccountTokenStore == null) {
            throw new IllegalStateException("Can't find tokens because index service account token store not configured");
        }
        final ServiceAccountId accountId = new ServiceAccountId(request.getNamespace(), request.getServiceName());
        findIndexTokens(accountId, listener);
    }

    // TODO: No production code usage
    public void getRoleDescriptor(Authentication authentication, ActionListener<RoleDescriptor> listener) {
        assert authentication.isServiceAccount() : "authentication is not for service account: " + authentication;
        final String principal = authentication.getEffectiveSubject().getUser().principal();
        getRoleDescriptorForPrincipal(principal, listener);
    }

    public void getRoleDescriptorForPrincipal(String principal, ActionListener<RoleDescriptor> listener) {
        final ServiceAccount builtIn = ACCOUNTS.get(principal);
        if (builtIn != null) {
            listener.onResponse(builtIn.roleDescriptor());
            return;
        }
        if (indexUserServiceAccountStore == null) {
            listener.onFailure(
                new ElasticsearchSecurityException("cannot load role for service account [" + principal + "] - no such service account")
            );
            return;
        }
        indexUserServiceAccountStore.getAccount(principal, ActionListener.wrap(roleDescriptor -> {
            if (roleDescriptor == null) {
                listener.onFailure(
                    new ElasticsearchSecurityException("cannot load role for service account [" + principal + "] - no such service account")
                );
            } else {
                listener.onResponse(roleDescriptor);
            }
        }, listener::onFailure));
    }

    private static Authentication createAuthentication(
        ServiceAccount account,
        ServiceAccountToken token,
        TokenSource tokenSource,
        String nodeName
    ) {
        final User user = account.asUser();
        return Authentication.newServiceAccountAuthentication(
            user,
            nodeName,
            Map.of(TOKEN_NAME_FIELD, token.getTokenName(), TOKEN_SOURCE_FIELD, tokenSource.name().toLowerCase(Locale.ROOT))
        );
    }

    private static ElasticsearchSecurityException createAuthenticationException(ServiceAccountToken serviceAccountToken) {
        return new ElasticsearchSecurityException(
            "failed to authenticate service account [{}] with token name [{}]",
            RestStatus.UNAUTHORIZED,
            serviceAccountToken.getAccountId().asPrincipal(),
            serviceAccountToken.getTokenName()
        );
    }

    private void findIndexTokens(ServiceAccountId accountId, ActionListener<GetServiceAccountCredentialsResponse> listener) {
        indexServiceAccountTokenStore.findTokensFor(accountId, ActionListener.wrap(indexTokenInfos -> {
            findFileTokens(indexTokenInfos, accountId, listener);
        }, listener::onFailure));
    }

    private void findFileTokens(
        Collection<TokenInfo> indexTokenInfos,
        ServiceAccountId accountId,
        ActionListener<GetServiceAccountCredentialsResponse> listener
    ) {
        executeAsyncWithOrigin(
            client,
            SECURITY_ORIGIN,
            GetServiceAccountNodesCredentialsAction.INSTANCE,
            new GetServiceAccountCredentialsNodesRequest(accountId.namespace(), accountId.serviceName()),
            ActionListener.wrap(
                fileTokensResponse -> listener.onResponse(
                    new GetServiceAccountCredentialsResponse(accountId.asPrincipal(), indexTokenInfos, fileTokensResponse)
                ),
                listener::onFailure
            )
        );
    }

    /**
     * Runtime {@link ServiceAccount} representation for a user-defined account loaded from the
     * {@code .security} index. Distinct from {@link ElasticServiceAccounts.ElasticServiceAccount}
     * so that callers can distinguish via {@code instanceof} where needed.
     */
    public static final class UserDefinedServiceAccount implements ServiceAccount {

        private final ServiceAccountId id;
        private final RoleDescriptor roleDescriptor;
        private final User user;

        public UserDefinedServiceAccount(ServiceAccountId id, RoleDescriptor roleDescriptor) {
            this.id = Objects.requireNonNull(id, "service account ID cannot be null");
            this.roleDescriptor = Objects.requireNonNull(roleDescriptor, "role descriptor cannot be null");
            this.user = new User(
                id.asPrincipal(),
                org.elasticsearch.common.Strings.EMPTY_ARRAY,
                "Service account - " + id,
                null,
                Map.of("_user_defined_service_account", true),
                true
            );
        }

        @Override
        public ServiceAccountId id() {
            return id;
        }

        @Override
        public RoleDescriptor roleDescriptor() {
            return roleDescriptor;
        }

        @Override
        public User asUser() {
            return user;
        }
    }
}
